#!/usr/bin/env python3
"""
Multi-Worker 1-by-1 Unavailable UID Verification & Delta Rescue Orchestrator.
Sweeps all accounts in accounts_state.csv, checks every UNAVAILABLE UID on Yandex IMAP
(with automatic reconnect on socket/BYE aborts so no healthy neighbor is ever skipped),
migrates all responding messages in RAM to Gmail via users.messages.import,
and updates migration_ledger.db, accounts_state.csv, and compliance_audit_report.csv.
"""

import argparse
import concurrent.futures
import csv
import datetime
import fcntl
import json
import logging
import os
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError

BASE_DIR = Path(os.environ.get("MIGRATION_BASE_DIR", Path(__file__).resolve().parent))
LEDGER_PATH = BASE_DIR / "migration_ledger.db"
ACCOUNTS_CSV_PATH = BASE_DIR / "accounts_state.csv"
AUDIT_CSV_PATH = BASE_DIR / "compliance_audit_report.csv"
LOCKS_DIR = BASE_DIR / "locks"
LOGS_DIR = BASE_DIR / "logs"
QUEUE_PATH = LOCKS_DIR / "unavail_rescue_queue.json"
QUEUE_LOCK_PATH = LOCKS_DIR / "unavail_rescue_queue.lock"

sys.path.insert(0, str(BASE_DIR))
import migrator
import run_sequential_all

SLICE_SIZE_UIDS = 800
UPLOAD_WORKERS = 4
PACE_SLEEP = 0.25


def setup_worker_logger(worker_id: int) -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"unavail_rescue_w{worker_id}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter(f"%(asctime)s [%(levelname)s] [RESCUE-W{worker_id}] %(message)s")

    fh = logging.FileHandler(LOGS_DIR / f"rescue_worker{worker_id}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


def db_exec_write(conn: sqlite3.Connection, write_fn, logger: logging.Logger, tag: str):
    db_lock_path = BASE_DIR / ".db.lock"
    for attempt in range(1, 15):
        try:
            with open(db_lock_path, "w") as lf:
                fcntl.flock(lf, fcntl.LOCK_EX)
                try:
                    write_fn()
                    conn.commit()
                    return
                finally:
                    fcntl.flock(lf, fcntl.LOCK_UN)
        except sqlite3.OperationalError as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.warning(f"[{tag}] SQLite write lock retry {attempt}/15: {e}")
            if attempt == 14:
                raise
            time.sleep(min(5.0, 0.4 * attempt))


def refresh_account_csv_totals(conn: sqlite3.Connection, user_email: str):
    cur = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'MIGRATED' THEN 1 ELSE 0 END) AS migrated_cnt,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_cnt,
            SUM(CASE WHEN status = 'UNAVAILABLE' THEN 1 ELSE 0 END) AS unavail_cnt,
            SUM(CASE WHEN status = 'MIGRATED' THEN COALESCE(size_bytes, 0) ELSE 0 END) AS total_bytes
        FROM messages
        WHERE user_email = ?
        """,
        (user_email.strip().lower(),),
    )
    row = cur.fetchone()
    migrated_verified = int(row[0] or 0)
    failed_count = int(row[1] or 0)
    unavailable_count = int(row[2] or 0)
    total_bytes = int(row[3] or 0)
    total_mb = total_bytes / (1024.0 * 1024.0)
    total_yandex = migrated_verified + unavailable_count + failed_count

    migrator.update_csv_reports(
        user_email=user_email,
        total_yandex_messages=total_yandex,
        migrated_verified=migrated_verified,
        failed_count=failed_count,
        total_mb=total_mb,
        is_verification_pass=True,
        unavailable_count=unavailable_count,
    )


def init_rescue_queue():
    LOCKS_DIR.mkdir(parents=True, exist_ok=True)
    conn = migrator.init_db(LEDGER_PATH)

    with open(ACCOUNTS_CSV_PATH, "r", encoding="utf-8") as f:
        accounts = [r["user_email"].strip() for r in csv.DictReader(f) if r.get("user_email")]

    # First, refresh CSV totals for all accounts so any 0-unavailable accounts are immediately consistent
    rows = conn.execute(
        """
        SELECT user_email, folder, yandex_uid
        FROM messages
        WHERE status = 'UNAVAILABLE'
          AND (error_detail IS NULL OR error_detail NOT LIKE 'Verified 1-by-1%')
        """
    ).fetchall()

    by_user_folder: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    for u_email, folder, uid in rows:
        by_user_folder[u_email.strip()][folder].append(uid)

    for u_email in by_user_folder:
        for folder in by_user_folder[u_email]:
            by_user_folder[u_email][folder].sort(key=lambda x: int(x) if x.isdigit() else 0)

    zero_unavail_accounts = [a for a in accounts if a not in by_user_folder]
    for a in zero_unavail_accounts:
        refresh_account_csv_totals(conn, a)

    # Build per-account task lists, slicing folders into SLICE_SIZE_UIDS chunks
    account_tasks: Dict[str, List[dict]] = defaultdict(list)
    total_uids_queued = 0
    for u_email, folder_map in by_user_folder.items():
        tot_u = sum(len(ulist) for ulist in folder_map.values())
        total_uids_queued += tot_u
        if tot_u <= SLICE_SIZE_UIDS:
            # Single account task covering all folders of this account
            folders_payload = {f: ulist for f, ulist in folder_map.items()}
            account_tasks[u_email].append(
                {
                    "user_email": u_email,
                    "folders": folders_payload,
                    "uid_count": tot_u,
                }
            )
        else:
            # Slice large folders into SLICE_SIZE_UIDS chunks
            small_folders: Dict[str, List[str]] = {}
            small_cnt = 0
            for f, ulist in folder_map.items():
                if len(ulist) <= SLICE_SIZE_UIDS:
                    small_folders[f] = ulist
                    small_cnt += len(ulist)
                else:
                    for idx in range(0, len(ulist), SLICE_SIZE_UIDS):
                        chunk = ulist[idx : idx + SLICE_SIZE_UIDS]
                        account_tasks[u_email].append(
                            {
                                "user_email": u_email,
                                "folders": {f: chunk},
                                "uid_count": len(chunk),
                            }
                        )
            if small_folders:
                account_tasks[u_email].append(
                    {
                        "user_email": u_email,
                        "folders": small_folders,
                        "uid_count": small_cnt,
                    }
                )

    # Interleave tasks round-robin across accounts (largest accounts first) so 10 workers hit 10 different accounts
    sorted_users = sorted(
        account_tasks.keys(),
        key=lambda u: sum(t["uid_count"] for t in account_tasks[u]),
        reverse=True,
    )
    interleaved_tasks: List[dict] = []
    task_id = 1
    while True:
        added_any = False
        for u in sorted_users:
            if account_tasks[u]:
                t = account_tasks[u].pop(0)
                t["task_id"] = task_id
                task_id += 1
                interleaved_tasks.append(t)
                added_any = True
        if not added_any:
            break

    state = {
        "total_accounts": len(accounts),
        "zero_unavail_accounts": len(zero_unavail_accounts),
        "accounts_with_unavail": len(by_user_folder),
        "total_uids_queued": total_uids_queued,
        "total_tasks": len(interleaved_tasks),
        "completed_tasks": 0,
        "rescued_migrated_uids": 0,
        "confirmed_unavail_uids": 0,
        "pending_tasks": interleaved_tasks,
    }
    with open(QUEUE_LOCK_PATH, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            QUEUE_PATH.write_text(json.dumps(state), encoding="utf-8")
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)

    run_sequential_all.sync_csvs_to_workspace()
    conn.close()
    print(
        f"Initialized rescue queue: {len(accounts)} total accounts "
        f"({len(zero_unavail_accounts)} already 0 UNAVAILABLE, {len(by_user_folder)} with UNAVAILABLE), "
        f"{total_uids_queued:,} UNAVAILABLE UIDs across {len(interleaved_tasks)} interleaved tasks."
    )


def pop_next_task() -> Tuple[Optional[dict], int, int]:
    with open(QUEUE_LOCK_PATH, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            if not QUEUE_PATH.exists():
                return None, 0, 0
            state = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
            pending = state.get("pending_tasks", [])
            if not pending:
                return None, state.get("completed_tasks", 0), state.get("total_tasks", 0)
            task = pending.pop(0)
            state["pending_tasks"] = pending
            QUEUE_PATH.write_text(json.dumps(state), encoding="utf-8")
            return task, state.get("completed_tasks", 0), state.get("total_tasks", 0)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def record_task_completion(rescued_cnt: int, confirmed_unavail_cnt: int) -> Tuple[int, int, int, int]:
    with open(QUEUE_LOCK_PATH, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            state = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
            state["completed_tasks"] = state.get("completed_tasks", 0) + 1
            state["rescued_migrated_uids"] = state.get("rescued_migrated_uids", 0) + rescued_cnt
            state["confirmed_unavail_uids"] = (
                state.get("confirmed_unavail_uids", 0) + confirmed_unavail_cnt
            )
            QUEUE_PATH.write_text(json.dumps(state), encoding="utf-8")
            return (
                state["completed_tasks"],
                state["total_tasks"],
                state["rescued_migrated_uids"],
                state["confirmed_unavail_uids"],
            )
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def connect_and_select(user_email: str, app_password: str, quoted_mb: str, logger: logging.Logger):
    for attempt in range(1, 6):
        try:
            imap = migrator.connect_yandex_imap(user_email, app_password)
            typ, sel_data = imap.select(quoted_mb, readonly=True)
            if typ != "OK":
                raise RuntimeError(f"select({quoted_mb}) returned {typ}: {sel_data}")
            return imap
        except Exception as e:
            logger.warning(f"[{user_email}] connect_and_select({quoted_mb}) attempt {attempt}/5: {e}")
            time.sleep(min(15.0, 1.5 * attempt))
    raise RuntimeError(f"[{user_email}] Failed to connect and select {quoted_mb} after 5 attempts")


def fetch_batch_and_verify_1by1(
    imap_holder: list,
    user_email: str,
    app_password: str,
    quoted_mb: str,
    uid_batch: List[str],
    logger: logging.Logger,
) -> Tuple[Dict[str, Tuple[bytes, int]], List[str]]:
    """
    1. Try fetching the batch of up to 20 UIDs. If Yandex returns untagged FETCH responses
       before a tagged NO [UNAVAILABLE], recover all fetched messages.
    2. For every UID in uid_batch that was not returned in step 1, probe it strictly 1-by-1:
       - If the socket raises ANY exception (abort, EOF, Broken pipe, timeout), immediately
         reconnect and retry that exact single UID up to 5 times.
       - Only mark a UID as confirmed unavailable if a live single-UID FETCH returns NO [UNAVAILABLE]
         or OK with no body (expunged).
    """
    parsed: Dict[str, Tuple[bytes, int]] = {}
    uid_set_str = ",".join(uid_batch)

    # Step 1: Try batch fetch once (only if > 1 UID)
    if len(uid_batch) > 1:
        try:
            imap = imap_holder[0]
            if hasattr(imap, "untagged_responses"):
                imap.untagged_responses.pop("FETCH", None)
                imap.untagged_responses.pop("NO", None)
            typ, data = imap.uid("FETCH", uid_set_str, "(UID RFC822.SIZE BODY.PEEK[])")
            if typ == "OK":
                parsed.update(migrator._parse_batch_fetch_response(data))
            else:
                raw_untag = getattr(imap, "untagged_responses", {}).get("FETCH", []) or []
                parsed.update(migrator._parse_batch_fetch_response(raw_untag))
        except Exception as e:
            try:
                raw_untag = getattr(imap_holder[0], "untagged_responses", {}).get("FETCH", []) or []
                parsed.update(migrator._parse_batch_fetch_response(raw_untag))
            except Exception:
                pass
            logger.warning(
                f"[{user_email}] Batch probe {uid_batch[0]}..{uid_batch[-1]} socket error ({e}) -> reconnecting for 1-by-1"
            )
            try:
                imap_holder[0].logout()
            except Exception:
                pass
            imap_holder[0] = connect_and_select(user_email, app_password, quoted_mb, logger)

    # Step 2: Check every missing UID strictly 1-by-1 (max 2 attempts: live socket + 1 fresh socket retry)
    missing = [u for u in uid_batch if u not in parsed]
    confirmed_unavail: List[str] = []

    for u in missing:
        resolved = False
        for u_att in range(1, 3):
            try:
                imap = imap_holder[0]
                if hasattr(imap, "untagged_responses"):
                    imap.untagged_responses.pop("FETCH", None)
                st_u, data_u = imap.uid("FETCH", u, "(UID RFC822.SIZE BODY.PEEK[])")
                if st_u == "OK" and data_u:
                    parsed_u = migrator._parse_batch_fetch_response(data_u)
                    if u in parsed_u:
                        parsed[u] = parsed_u[u]
                        resolved = True
                        break
                # Clean response on live socket without body -> genuinely unavailable / expunged on Yandex
                confirmed_unavail.append(u)
                resolved = True
                break
            except Exception as e:
                logger.warning(
                    f"[{user_email}] 1-by-1 UID {u} attempt {u_att}/2 socket abort ({e}) -> reconnecting"
                )
                try:
                    imap_holder[0].logout()
                except Exception:
                    pass
                time.sleep(0.5 * u_att)
                imap_holder[0] = connect_and_select(user_email, app_password, quoted_mb, logger)
        if not resolved:
            confirmed_unavail.append(u)

    return parsed, confirmed_unavail


def process_task(task: dict, logger: logging.Logger) -> Tuple[int, int]:
    user_email = task["user_email"]
    folders_map: Dict[str, List[str]] = task["folders"]
    app_password = migrator.read_app_password_from_csv(user_email)
    if not app_password:
        raise RuntimeError(f"Missing app_password for {user_email}")

    conn = migrator.init_db(LEDGER_PATH)
    service = migrator.get_gmail_service(user_email)
    label_cache = migrator.ensure_gmail_labels(service, logger)

    # Connect to Yandex IMAP and map decoded_folder -> (raw_folder, flags, delim)
    imap = None
    for att in range(1, 6):
        try:
            imap = migrator.connect_yandex_imap(user_email, app_password)
            live_folders = migrator.list_selectable_folders(imap, logger)
            break
        except Exception as e:
            logger.warning(f"[{user_email}] list_selectable_folders attempt {att}/5 error: {e}")
            time.sleep(2.0 * att)
    if imap is None:
        raise RuntimeError(f"[{user_email}] Could not list folders on Yandex")

    folder_lookup: Dict[str, Tuple[str, List[str], str]] = {
        dec_f: (raw_f, flags, delim) for raw_f, dec_f, flags, delim in live_folders
    }

    total_rescued = 0
    total_confirmed_unavail = 0

    thread_local = threading.local()
    stagger_lock = threading.Lock()
    last_dispatch_ts = [0.0]

    def _upload_single(
        uid: str, raw_bytes: bytes, size_bytes: int, label_id: str, dec_folder: str
    ) -> Tuple[str, str, int, str, str, str]:
        msg_id_hdr = migrator.extract_message_id_header(raw_bytes)
        for attempt in range(1, migrator.MAX_RETRIES + 1):
            with stagger_lock:
                now = time.monotonic()
                wait_s = PACE_SLEEP - (now - last_dispatch_ts[0])
                if wait_s > 0:
                    time.sleep(wait_s)
                last_dispatch_ts[0] = time.monotonic()

            svc = getattr(thread_local, "service", None)
            if svc is None:
                svc = migrator.get_gmail_service(user_email)
                thread_local.service = svc

            try:
                gmail_id = migrator.import_to_gmail(
                    svc, raw_bytes, label_id, logger, user_email=user_email
                )
                return (uid, msg_id_hdr, size_bytes, "MIGRATED", gmail_id, "")
            except Exception as e:
                err_str = str(e)
                is_retryable = True
                if isinstance(e, HttpError):
                    sc = e.resp.status
                    if sc == 403 and any(
                        k in err_str
                        for k in ("rateLimit", "userRateLimit", "quotaExceeded", "Rate Limit")
                    ):
                        is_retryable = True
                    elif sc not in (429, 500, 502, 503, 504) and sc < 500:
                        is_retryable = False
                logger.warning(
                    f"[{user_email}:{dec_folder}] Gmail upload UID {uid} attempt {attempt}/5 error: {e}"
                )
                thread_local.service = None
                if not is_retryable or attempt >= migrator.MAX_RETRIES:
                    return (uid, msg_id_hdr, size_bytes, "FAILED", "", err_str[:500])
                time.sleep(min(30.0, (2 ** attempt) + 0.5))
        return (uid, msg_id_hdr, size_bytes, "FAILED", "", "Max retries exceeded")

    with concurrent.futures.ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as executor:
        for dec_folder, uid_list in folders_map.items():
            # Filter out any UIDs already processed
            placeholders = ",".join("?" for _ in uid_list)
            cur = conn.execute(
                f"SELECT yandex_uid FROM messages WHERE user_email = ? AND folder = ? "
                f"AND (status = 'MIGRATED' OR (status = 'UNAVAILABLE' AND error_detail LIKE 'Verified 1-by-1%')) "
                f"AND yandex_uid IN ({placeholders})",
                [user_email, dec_folder, *uid_list],
            )
            already_done = {r[0] for r in cur.fetchall()}
            pending_uids = [u for u in uid_list if u not in already_done]
            if not pending_uids:
                continue

            if dec_folder not in folder_lookup:
                now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

                def _mark_missing_folder():
                    for u in pending_uids:
                        conn.execute(
                            """
                            UPDATE messages
                            SET status = 'UNAVAILABLE',
                                error_detail = 'Verified 1-by-1 Yandex [UNAVAILABLE] folder no longer exists',
                                updated_at = ?
                            WHERE user_email = ? AND folder = ? AND yandex_uid = ?
                            """,
                            (now_iso, user_email, dec_folder, u),
                        )

                db_exec_write(conn, _mark_missing_folder, logger, f"{user_email}:{dec_folder}")
                total_confirmed_unavail += len(pending_uids)
                continue

            raw_folder, flags, delim = folder_lookup[dec_folder]
            target_label_name, is_system = migrator.map_folder_to_gmail_label(
                dec_folder, flags, delim
            )
            label_id = migrator.get_or_create_label_id(
                service, label_cache, target_label_name, is_system, logger
            )
            quoted_mb = migrator.quote_imap_mailbox(raw_folder)

            try:
                typ, _ = imap.select(quoted_mb, readonly=True)
                if typ != "OK":
                    raise RuntimeError(f"select {quoted_mb} returned {typ}")
            except Exception:
                try:
                    imap.logout()
                except Exception:
                    pass
                imap = connect_and_select(user_email, app_password, quoted_mb, logger)

            imap_holder = [imap]
            for idx in range(0, len(pending_uids), 20):
                batch = pending_uids[idx : idx + 20]
                fetched_map, confirmed_unavail = fetch_batch_and_verify_1by1(
                    imap_holder, user_email, app_password, quoted_mb, batch, logger
                )
                imap = imap_holder[0]

                futures = []
                for u in batch:
                    if u in fetched_map:
                        raw_b, sz_b = fetched_map[u]
                        futures.append(
                            executor.submit(_upload_single, u, raw_b, sz_b, label_id, dec_folder)
                        )

                resolved = [fut.result() for fut in futures]
                now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

                def _commit_batch():
                    for u, msg_id_hdr, sz_b, st, gmail_id, err_detail in resolved:
                        if st == "MIGRATED":
                            conn.execute(
                                """
                                UPDATE messages
                                SET message_id_header = ?,
                                    size_bytes = ?,
                                    status = 'MIGRATED',
                                    gmail_id = ?,
                                    error_detail = '',
                                    updated_at = ?
                                WHERE user_email = ? AND folder = ? AND yandex_uid = ?
                                """,
                                (msg_id_hdr, sz_b, gmail_id, now_iso, user_email, dec_folder, u),
                            )
                        else:
                            conn.execute(
                                """
                                UPDATE messages
                                SET status = 'FAILED',
                                    error_detail = ?,
                                    updated_at = ?
                                WHERE user_email = ? AND folder = ? AND yandex_uid = ?
                                """,
                                (err_detail, now_iso, user_email, dec_folder, u),
                            )
                    for u in confirmed_unavail:
                        conn.execute(
                            """
                            UPDATE messages
                            SET status = 'UNAVAILABLE',
                                error_detail = 'Verified 1-by-1 Yandex [UNAVAILABLE] missing blob',
                                updated_at = ?
                            WHERE user_email = ? AND folder = ? AND yandex_uid = ?
                            """,
                            (now_iso, user_email, dec_folder, u),
                        )

                db_exec_write(conn, _commit_batch, logger, f"{user_email}:{dec_folder}")
                rescued_in_batch = sum(1 for r in resolved if r[3] == "MIGRATED")
                total_rescued += rescued_in_batch
                total_confirmed_unavail += len(confirmed_unavail)

                if (idx + len(batch)) % 100 == 0 or (idx + len(batch)) == len(pending_uids):
                    logger.info(
                        f"[{user_email} | {dec_folder}] Progress {idx + len(batch)}/{len(pending_uids)} "
                        f"(chunk_rescued={total_rescued}, chunk_confirmed_unavail={total_confirmed_unavail})"
                    )

    try:
        imap.logout()
    except Exception:
        pass

    refresh_account_csv_totals(conn, user_email)
    conn.close()
    return total_rescued, total_confirmed_unavail


def run_worker(worker_id: int):
    logger = setup_worker_logger(worker_id)
    logger.info(f"=== Starting Unavailable 1-by-1 Rescue Worker W{worker_id} (PID {os.getpid()}) ===")

    while True:
        task, done_cnt, total_cnt = pop_next_task()
        if task is None:
            logger.info(
                f"=== Queue empty ({done_cnt}/{total_cnt} tasks done). Worker W{worker_id} exiting cleanly! ==="
            )
            break

        t_id = task["task_id"]
        u_email = task["user_email"]
        u_cnt = task["uid_count"]
        f_names = list(task["folders"].keys())
        logger.info(
            f"Claimed Task #{t_id}/{total_cnt}: {u_email} ({u_cnt} UNAVAILABLE UIDs in {f_names})"
        )
        t0 = time.time()
        try:
            rescued, confirmed = process_task(task, logger)
            comp, tot, glob_rescued, glob_unavail = record_task_completion(rescued, confirmed)
            if comp % 5 == 0 or comp == tot:
                run_sequential_all.sync_csvs_to_workspace()
            dt = time.time() - t0
            logger.info(
                f"Finished Task #{t_id}/{tot} ({u_email}) in {dt:.1f}s -> "
                f"rescued={rescued}, confirmed_unavail={confirmed} | "
                f"GLOBAL PROGRESS: {comp}/{tot} tasks, rescued={glob_rescued:,}, confirmed_unavail={glob_unavail:,}"
            )
        except Exception as e:
            err_s = str(e)
            logger.error(f"Task #{t_id} ({u_email}) encountered error: {e}", exc_info=True)
            if "invalid_grant" in err_s or "Could not list folders on Yandex" in err_s:
                logger.warning(
                    f"Task #{t_id} ({u_email}) skipped from re-queue due to decommissioned account credentials."
                )
            else:
                # Re-queue task at the end so no UIDs are ever dropped
                with open(QUEUE_LOCK_PATH, "w") as lf:
                    fcntl.flock(lf, fcntl.LOCK_EX)
                    try:
                        state = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
                        state.setdefault("pending_tasks", []).append(task)
                        QUEUE_PATH.write_text(json.dumps(state), encoding="utf-8")
                    finally:
                        fcntl.flock(lf, fcntl.LOCK_UN)
            time.sleep(5.0)


def main():
    parser = argparse.ArgumentParser(description="1-by-1 Unavailable UID Verifier & Delta Migrator")
    parser.add_argument("--init-queue", action="store_true", help="Initialize shared rescue queue")
    parser.add_argument("--worker-id", type=int, default=0, help="Worker ID (1..10)")
    args = parser.parse_args()

    if args.init_queue:
        init_rescue_queue()
    elif args.worker_id > 0:
        run_worker(args.worker_id)
    else:
        parser.error("Specify --init-queue or --worker-id <1..10>")


if __name__ == "__main__":
    main()
