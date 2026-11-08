#!/usr/bin/env python3
"""
Master Multi-Worker Orchestrator for Yandex 360 -> Google Workspace Archive Migration.
Iterates through all accounts in accounts_state.csv:
1. Optionally resets password in Yandex 360 Admin to MIGRATION_TEMP_PASSWORD with forced reset unchecked.
2. Provisions 16-char App Password ('gw-migration') in isolated Playwright & saves to app_passwords.csv.
3. Runs Migration Pass (Pass 1) and Delta Verification Pass (Pass 2).
4. Syncs CSV reports after every account.
"""

import argparse
import csv
import datetime
import fcntl
import importlib
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(os.environ.get("MIGRATION_BASE_DIR", Path(__file__).resolve().parent))
WORKSPACE_CSV_DIR = Path(os.environ.get("MIGRATION_EXPORT_CSV_DIR", str(BASE_DIR / "csv")))
ACCOUNTS_CSV_PATH = BASE_DIR / "accounts_state.csv"
AUDIT_CSV_PATH = BASE_DIR / "compliance_audit_report.csv"
APP_PASSWORDS_CSV_PATH = BASE_DIR / "app_passwords.csv"
LOGS_DIR = BASE_DIR / "logs"
DEFAULT_MASTER_PASSWORD = os.environ.get("MIGRATION_TEMP_PASSWORD", "ChangeMe!Mig2026")
YANDEX_ADMIN_ORG_URL = os.environ.get("YANDEX_ADMIN_ORG_URL", "https://admin.yandex.com/users")

sys.path.insert(0, str(BASE_DIR))
import migrator
import provision_user


def sync_csvs_to_workspace():
    lock_path = BASE_DIR / ".csv.lock"
    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            WORKSPACE_CSV_DIR.mkdir(parents=True, exist_ok=True)
            for src in (ACCOUNTS_CSV_PATH, AUDIT_CSV_PATH, APP_PASSWORDS_CSV_PATH):
                if src.exists():
                    shutil.copy2(src, WORKSPACE_CSV_DIR / src.name)
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)


def run_chrome_js(js_code: str) -> str:
    escaped = js_code.replace("\\", "\\\\").replace('"', '\\"')
    apple_script = f'''tell application "Google Chrome"
    repeat with w in windows
        repeat with t in tabs of w
            if URL of t contains "admin.yandex" then
                return execute t javascript "{escaped}"
            end if
        end repeat
    end repeat
    return execute tab 2 of window 1 javascript "{escaped}"
end tell'''
    res = subprocess.run(
        ["osascript", "-e", apple_script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if res.returncode != 0:
        raise RuntimeError(f"osascript error: {res.stderr.strip()}")
    return res.stdout.strip()


def reset_password_in_yandex_admin(user_email: str, new_password: str = DEFAULT_MASTER_PASSWORD):
    local_part = user_email.split("@")[0].strip()
    print(f"[Yandex Admin] Resetting password for {user_email} (search={local_part})...")

    for attempt in range(1, 4):
        try:
            # 1. Close any modal and return to employee search list if on profile drawer
            run_chrome_js(
                f"""(() => {{
                const cancelBtn = Array.from(document.querySelectorAll("button")).find(b => b.innerText && ["Cancel", "Отмена"].includes(b.innerText.trim()));
                if (cancelBtn) cancelBtn.click();
                const closeBtn = Array.from(document.querySelectorAll("button, span")).find(b => b.innerText && ["Close", "Закрыть"].includes(b.innerText.trim()));
                if (closeBtn) closeBtn.click();
                if (!document.body.innerText.toLowerCase().includes("{user_email.lower()}")) {{
                    const backBtn = Array.from(document.querySelectorAll("button, a, span")).find(b => b.innerText && ["Back", "Назад"].includes(b.innerText.trim()));
                    if (backBtn) backBtn.click();
                }}
                return "ready";
            }})()"""
            )
            time.sleep(1.5)

            # Check if we are already on this exact user's profile page
            already_open = run_chrome_js(
                f'(() => document.body.innerText.toLowerCase().includes("{user_email.lower()}") && (document.body.innerText.includes("Change password") || document.body.innerText.includes("Изменить пароль")) ? "yes" : "no")()'
            )
            if already_open != "yes":
                # Wait up to 5s for search input to appear after clicking Back
                has_input = "no"
                for _ in range(10):
                    has_input = run_chrome_js(
                        '(() => document.querySelector(\'input[placeholder="Search"], input[placeholder="Поиск"]\') ? "yes" : "no")()'
                    )
                    if has_input == "yes":
                        break
                    time.sleep(0.5)

                # 2. Search for local_part
                search_res = run_chrome_js(
                    f"""(() => {{
                    const inp = document.querySelector('input[placeholder="Search"], input[placeholder="Поиск"]');
                    if (!inp) return "no_input";
                    inp.focus();
                    const lastVal = inp.value;
                    const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
                    nativeSetter.call(inp, "{local_part}");
                    if (inp._valueTracker) inp._valueTracker.setValue(lastVal);
                    inp.dispatchEvent(new Event("input", {{ bubbles: true }}));
                    inp.dispatchEvent(new Event("change", {{ bubbles: true }}));
                    return "searched";
                }})()"""
                )
                if search_res == "no_input":
                    run_chrome_js(f'(() => {{ window.location.href = "{YANDEX_ADMIN_ORG_URL}"; return "reloading"; }})()')
                    time.sleep(6.0)
                    continue

                # 3. Poll up to 7.5s for matching user row to appear and click it
                click_res = "not_found"
                for _ in range(10):
                    time.sleep(0.75)
                    click_res = run_chrome_js(
                        f"""(() => {{
                        const cell = Array.from(document.querySelectorAll("td, div, span, a")).find(
                            el => el.innerText && el.innerText.trim().toLowerCase() === "{local_part.lower()}"
                        );
                        if (cell) {{ cell.click(); return "clicked"; }}
                        return "not_found";
                    }})()"""
                    )
                    if click_res == "clicked":
                        break
                if click_res != "clicked":
                    run_chrome_js(f'(() => {{ window.location.href = "{YANDEX_ADMIN_ORG_URL}"; return "reloading"; }})()')
                    time.sleep(5.0)
                    raise RuntimeError(f"User '{local_part}' not found in Yandex Admin search results")
                time.sleep(2.2)

            # 4. Click "Change password"
            open_modal = run_chrome_js(
                """(() => {
                const btn = Array.from(document.querySelectorAll("button, a, span")).find(
                    b => b.innerText && ["Change password", "Изменить пароль"].includes(b.innerText.trim())
                );
                if (btn) { btn.click(); return "opened"; }
                return "no_btn";
            })()"""
            )
            if open_modal != "opened":
                raise RuntimeError("Could not find 'Change password' button on user profile")
            time.sleep(1.5)

            # 5. Fill password1, password2, uncheck isPasswordChange, click Save
            save_res = run_chrome_js(
                f"""(() => {{
                function setReactInput(el, val) {{
                    const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
                    nativeSetter.call(el, val);
                    el.dispatchEvent(new Event("input", {{ bubbles: true }}));
                    el.dispatchEvent(new Event("change", {{ bubbles: true }}));
                }}
                const p1 = document.querySelector('input[name="password1"]');
                const p2 = document.querySelector('input[name="password2"]');
                if (!p1 || !p2) return "no_pwd_inputs";
                setReactInput(p1, "{new_password}");
                setReactInput(p2, "{new_password}");

                const lbl = document.querySelector('label[data-testid="password-change-checkbox"]');
                const cb = document.querySelector('input[name="isPasswordChange"]');
                if (cb && cb.checked) cb.click();
                else if (lbl && cb && cb.getAttribute("aria-checked") === "true") lbl.click();

                setTimeout(() => {{
                    const saveBtn = Array.from(document.querySelectorAll("button")).find(
                        b => b.innerText && ["Save", "Сохранить"].includes(b.innerText.trim())
                    );
                    if (saveBtn) saveBtn.click();
                }}, 400);
                return "saved_cb=" + (cb ? cb.checked : "none");
            }})()"""
            )
            time.sleep(3.0)
            print(f"[Yandex Admin] Password reset completed for {user_email} ({save_res})")
            return
        except Exception as e:
            print(f"[Yandex Admin] Attempt {attempt}/3 error for {user_email}: {e}")
            time.sleep(3.0)

    raise RuntimeError(f"Failed to reset password in Yandex Admin for {user_email} after 3 attempts")


def reorder_special_accounts_to_end():
    """Optional hook to move known large/whale accounts (from MIGRATION_TAIL_ACCOUNTS) to the end of the queue."""
    tail_env = {
        e.strip().lower()
        for e in os.environ.get("MIGRATION_TAIL_ACCOUNTS", "").split(",")
        if e.strip()
    }
    if not tail_env:
        return
    lock_path = BASE_DIR / ".csv.lock"
    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            with open(ACCOUNTS_CSV_PATH, "r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
                if not rows:
                    return
                fieldnames = rows[0].keys()

            normal = []
            tail = []
            for r in rows:
                em = r["user_email"].strip().lower()
                if em in tail_env:
                    tail.append(r)
                else:
                    normal.append(r)

            with open(ACCOUNTS_CSV_PATH, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(normal + tail)
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)


def read_live_account_row(user_email: str, fallback_row: dict) -> dict:
    lock_path = BASE_DIR / ".csv.lock"
    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            with open(ACCOUNTS_CSV_PATH, "r", newline="", encoding="utf-8") as f:
                live_map = {r["user_email"].strip().lower(): r for r in csv.DictReader(f)}
            return live_map.get(user_email.lower(), fallback_row)
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)


TAIL_WHALE_ACCOUNTS = {
    e.strip().lower()
    for e in os.environ.get("MIGRATION_TAIL_ACCOUNTS", "").split(",")
    if e.strip()
}


def is_tail_account(email_lower: str) -> bool:
    return email_lower in TAIL_WHALE_ACCOUNTS


def build_worker_slice(all_accounts: list, worker_id: int, num_workers: int) -> list:
    n = len(all_accounts)
    chunk_size = (n + num_workers - 1) // num_workers
    start_idx = (worker_id - 1) * chunk_size
    end_idx = min(n, worker_id * chunk_size)
    raw_slice = all_accounts[start_idx:end_idx]

    # Place known whale/special accounts at the tail of this worker's slice
    normal_slice = [r for r in raw_slice if not is_tail_account(r["user_email"].strip().lower())]
    tail_slice = [r for r in raw_slice if is_tail_account(r["user_email"].strip().lower())]
    worker_slice = normal_slice + tail_slice

    queue_file = BASE_DIR / f"queue_worker{worker_id}.csv"
    if worker_slice:
        fieldnames = list(worker_slice[0].keys())
        with open(queue_file, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(worker_slice)
    return worker_slice


def main():
    parser = argparse.ArgumentParser(description="Concurrent Multi-Worker Yandex -> Gmail Orchestrator")
    parser.add_argument("--worker-id", type=int, default=1, help="1-indexed worker ID (1..8)")
    parser.add_argument("--num-workers", type=int, default=3, help="Total number of concurrent workers")
    parser.add_argument(
        "--use-existing-queue",
        action="store_true",
        help="Load pre-generated queue_worker{worker_id}.csv instead of slicing accounts_state.csv",
    )
    args = parser.parse_args()

    worker_id = args.worker_id
    num_workers = args.num_workers

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    worker_log_path = LOGS_DIR / f"worker{worker_id}.log"
    os.environ["MIGRATOR_WORKER_LOG"] = str(worker_log_path)

    if worker_id == 1 and not args.use_existing_queue:
        reorder_special_accounts_to_end()
        sync_csvs_to_workspace()

    lock_path = BASE_DIR / ".csv.lock"
    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            with open(ACCOUNTS_CSV_PATH, "r", newline="", encoding="utf-8") as f:
                all_accounts = list(csv.DictReader(f))
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)

    queue_file = BASE_DIR / f"queue_worker{worker_id}.csv"
    if args.use_existing_queue and queue_file.exists():
        with open(queue_file, "r", newline="", encoding="utf-8") as f:
            worker_accounts = list(csv.DictReader(f))
    else:
        worker_accounts = build_worker_slice(all_accounts, worker_id, num_workers)
    total_worker_accounts = len(worker_accounts)

    print(
        f"[Worker {worker_id}/{num_workers}] Assigned {total_worker_accounts} accounts "
        f"({worker_accounts[0]['user_email']} .. {worker_accounts[-1]['user_email']}), "
        f"logging to {worker_log_path}"
    )

    for sweep in range(1, 4):
        os.environ["MIGRATOR_DEFER_OVER_UIDS"] = (
            "0" if args.use_existing_queue else ("50000" if sweep == 1 else "0")
        )
        print(f"\n##################################################################")
        print(
            f"### [Worker {worker_id}/{num_workers}] STARTING SWEEP {sweep}/3 "
            f"({total_worker_accounts} accounts, defer_over={os.environ['MIGRATOR_DEFER_OVER_UIDS']}) ###"
        )
        print(f"##################################################################")

        for idx, init_acc in enumerate(worker_accounts, start=1):
            user_email = init_acc["user_email"].strip()

            acc = read_live_account_row(user_email, init_acc)
            status = acc.get("status", "").strip()
            app_pw = acc.get("app_password", "").strip()

            if status == "COMPLETED":
                print(
                    f"[Worker {worker_id} | Sweep {sweep} | {idx}/{total_worker_accounts}] "
                    f"Skipping already COMPLETED account: {user_email}"
                )
                continue

            if sweep == 1 and is_tail_account(user_email.lower()):
                print(
                    f"[Worker {worker_id} | Sweep {sweep} | {idx}/{total_worker_accounts}] "
                    f"Deferring tail/whale account to Sweep 2: {user_email}"
                )
                continue

            importlib.reload(provision_user)
            importlib.reload(migrator)

            print(f"\n==================================================================")
            print(
                f"[Worker {worker_id} | Sweep {sweep} | {idx}/{total_worker_accounts}] "
                f"Processing {user_email} (current status: {status})"
            )
            print(f"==================================================================")

            try:
                # 1. Provision App Password if not yet a valid 16-char token or if previous run failed IMAP auth (serialized via .provision.lock)
                import re as _re
                is_valid_token = bool(_re.fullmatch(r"[a-z0-9]{16}", app_pw))
                need_provision = not is_valid_token or (
                    status == "ERROR" and "AUTHENTICATIONFAILED" in acc.get("notes", "")
                )
                if need_provision:
                    prov_lock_path = BASE_DIR / ".provision.lock"
                    with open(prov_lock_path, "w") as prov_lock_f:
                        fcntl.flock(prov_lock_f, fcntl.LOCK_EX)
                        try:
                            # Re-check inside lock
                            acc_recheck = read_live_account_row(user_email, init_acc)
                            app_pw = acc_recheck.get("app_password", "").strip()
                            if not _re.fullmatch(r"[a-z0-9]{16}", app_pw) or not provision_user.test_imap_login(user_email, app_pw):
                                master_pw = DEFAULT_MASTER_PASSWORD
                                if os.environ.get("ENABLE_ADMIN_PASSWORD_RESET", "0") == "1":
                                    reset_password_in_yandex_admin(user_email, master_pw)

                                app_pw = provision_user.provision_user(user_email, master_pw)
                                sync_csvs_to_workspace()
                        finally:
                            fcntl.flock(prov_lock_f, fcntl.LOCK_UN)

                # 2. Run Migration Pass (Pass 1)
                rc1 = migrator.migrate_account(
                    user_email=user_email, app_password=app_pw, is_verification_pass=False
                )
                sync_csvs_to_workspace()
                if rc1 == 3:
                    print(
                        f"[Worker {worker_id} | Sweep {sweep} | {idx}/{total_worker_accounts}] "
                        f"DEFERRED: {user_email} has >50,000 pending messages; moved to tail Sweep 2!"
                    )
                    continue

                # 3. Run Delta Verification Pass (Pass 2)
                importlib.reload(migrator)
                rc2 = migrator.migrate_account(
                    user_email=user_email, app_password=app_pw, is_verification_pass=True
                )
                sync_csvs_to_workspace()
                if rc2 == 3:
                    print(
                        f"[Worker {worker_id} | Sweep {sweep} | {idx}/{total_worker_accounts}] "
                        f"DEFERRED: {user_email} has >50,000 pending messages in Pass 2; moved to tail Sweep 2!"
                    )
                    continue

                if rc2 == 0:
                    print(
                        f"[Worker {worker_id} | Sweep {sweep} | {idx}/{total_worker_accounts}] "
                        f"SUCCESS: {user_email} marked COMPLETED!"
                    )
                else:
                    print(
                        f"[Worker {worker_id} | Sweep {sweep} | {idx}/{total_worker_accounts}] "
                        f"WARNING: {user_email} finished delta pass with rc={rc2}"
                    )

            except Exception as e:
                try:
                    migrator.release_account_claim(user_email)
                except Exception:
                    pass
                print(
                    f"[Worker {worker_id} | Sweep {sweep} | {idx}/{total_worker_accounts}] "
                    f"ERROR on {user_email}: {e}"
                )
                provision_user.update_account_csv_password(
                    user_email, app_pw, "ERROR", f"Exception: {str(e)[:200]}"
                )
                sync_csvs_to_workspace()


if __name__ == "__main__":
    main()

