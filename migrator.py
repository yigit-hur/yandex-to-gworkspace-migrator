#!/usr/bin/env python3
"""
Yandex 360 to Google Workspace Sequential Archive Migrator
Strictly Read-Only on Yandex IMAP (readonly=True, BODY.PEEK[]).
Streams RFC822 messages in RAM -> Gmail API users.messages.import.
"""

import argparse
import concurrent.futures
import csv
import datetime
import email.parser
import email.policy
import fcntl
import imaplib
import io
import logging
import os
import re
import socket
import sqlite3
import ssl
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import googleapiclient.discovery as _gdisc
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload


def _safe_handle_message(self, msg):
    """Prevent Python email.generator.BytesGenerator from ASCII-encoding 8-bit message/rfc822 payloads."""
    payload = msg._payload
    if isinstance(payload, bytes):
        self._fp.write(payload)
    elif isinstance(payload, str):
        self._fp.write(payload.encode("ascii", "surrogateescape"))
    else:
        super(_gdisc._BytesGenerator, self)._handle_message(msg)


_gdisc._BytesGenerator._handle_message = _safe_handle_message
imaplib._MAXLINE = 100_000_000

BASE_DIR = Path(os.environ.get("MIGRATION_BASE_DIR", Path(__file__).resolve().parent))
LEDGER_PATH = BASE_DIR / "migration_ledger.db"
ACCOUNTS_CSV_PATH = BASE_DIR / "accounts_state.csv"
AUDIT_CSV_PATH = BASE_DIR / "compliance_audit_report.csv"
SERVICE_ACCOUNT_PATH = BASE_DIR / "service_account.json"
LOGS_DIR = BASE_DIR / "logs"

GMAIL_SCOPES = ["https://mail.google.com/"]
IMAP_HOST = "imap.yandex.com"
IMAP_PORT = 993
PACE_SLEEP_SECONDS = 0.30
MAX_RETRIES = 5
IMAP_BATCH_SIZE = 20
IMAP_BATCH_MAX_BYTES = 15 * 1024 * 1024  # 15 MB cumulative max per batch
RESUMABLE_THRESHOLD_BYTES = 4 * 1024 * 1024  # 4 MB threshold for resumable=True
GMAIL_UPLOAD_WORKERS = 4


def decode_modified_utf7(s: str) -> str:
    """Decode IMAP modified UTF-7 (RFC 3501 section 5.1.3) into UTF-8 string."""
    res = []
    i = 0
    n = len(s)
    while i < n:
        if s[i] == "&":
            end = s.find("-", i + 1)
            if end == -1:
                res.append(s[i:])
                break
            if end == i + 1:
                res.append("&")
            else:
                b64 = s[i + 1 : end].replace(",", "/")
                pad = (4 - len(b64) % 4) % 4
                b64 += "=" * pad
                import base64

                raw = base64.b64decode(b64)
                res.append(raw.decode("utf-16-be", errors="replace"))
            i = end + 1
        else:
            res.append(s[i])
            i += 1
    return "".join(res)


def parse_imap_list_line(line: bytes) -> Optional[Tuple[List[str], str, str]]:
    """
    Parse an IMAP LIST response line like:
    (\\HasNoChildren \\Sent) "|" "Sent"
    Returns (flags_list, delimiter, raw_folder_name).
    """
    text = line.decode("ascii", errors="surrogateescape")
    m = re.match(r'^\((?P<flags>[^)]*)\)\s+(?:"(?P<delim>[^"]*)"|NIL)\s+(?P<rest>.+)$', text)
    if not m:
        return None
    flags = [f.strip() for f in m.group("flags").split() if f.strip()]
    delim = m.group("delim") or "/"
    rest = m.group("rest").strip()
    if rest.startswith('"') and rest.endswith('"') and len(rest) >= 2:
        raw_name = rest[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    else:
        raw_name = rest
    return flags, delim, raw_name


def map_folder_to_gmail_label(
    decoded_folder: str, flags: List[str], delim: str
) -> Tuple[str, bool]:
    """
    Map a Yandex folder (and its IMAP flags) to either a Gmail system label ID
    or a custom label name.
    Returns (label_name_or_id, is_system_label).
    """
    flags_lower = {f.lower() for f in flags}
    norm = decoded_folder.strip()
    if delim and delim != "/":
        norm = norm.replace(delim, "/")
    norm_lower = norm.lower()

    # Check IMAP special-use flags first
    if "\\inbox" in flags_lower or norm_lower in (
        "inbox",
        "inbox_0",
        "входящие",
        "входящие_0",
    ):
        return "INBOX", True
    if "\\sent" in flags_lower or norm_lower in (
        "sent",
        "sent_0",
        "sent items",
        "sent messages",
        "отправленные",
        "отправленные_0",
    ):
        return "SENT", True
    if "\\drafts" in flags_lower or norm_lower in (
        "drafts",
        "drafts_0",
        "draft",
        "черновики",
        "черновики_0",
    ):
        return "DRAFT", True
    if "\\trash" in flags_lower or norm_lower in (
        "trash",
        "trash_0",
        "deleted",
        "deleted items",
        "deleted messages",
        "удаленные",
        "удаленные_0",
        "удалённые",
        "удалённые_0",
    ):
        return "TRASH", True
    if (
        "\\junk" in flags_lower
        or "\\spam" in flags_lower
        or norm_lower in ("spam", "spam_0", "junk", "junk e-mail", "спам", "спам_0")
    ):
        return "SPAM", True

    if norm_lower in ("архив", "архив_0", "archive", "archive_0"):
        return "Archive", False
    if norm_lower in ("исходящие", "outbox"):
        return "Outbox", False

    # Translate Russian parent folder prefixes on nested subfolders (e.g. 'Удаленные/EVIDENCE' -> 'Deleted/EVIDENCE')
    prefix_map = {
        "удаленные/": "Deleted/",
        "удалённые/": "Deleted/",
        "отправленные/": "Sent/",
        "черновики/": "Drafts/",
        "входящие/": "INBOX/",
        "архив/": "Archive/",
        "спам/": "Spam/",
    }
    for ru_pref, en_pref in prefix_map.items():
        if norm_lower.startswith(ru_pref):
            norm = en_pref + norm[len(ru_pref):]
            break

    return norm, False


def init_db(db_path: Path) -> sqlite3.Connection:
    db_lock_path = BASE_DIR / ".db.lock"
    for attempt in range(1, 11):
        try:
            with open(db_lock_path, "w") as db_lock_f:
                fcntl.flock(db_lock_f, fcntl.LOCK_EX)
                try:
                    conn = sqlite3.connect(str(db_path), timeout=60.0)
                    conn.execute("PRAGMA journal_mode=WAL;")
                    conn.execute("PRAGMA busy_timeout=60000;")
                    conn.execute("PRAGMA synchronous=NORMAL;")
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS messages (
                            user_email TEXT,
                            folder TEXT,
                            yandex_uid TEXT,
                            message_id_header TEXT,
                            size_bytes INT,
                            status TEXT,
                            gmail_id TEXT,
                            error_detail TEXT,
                            updated_at TEXT,
                            PRIMARY KEY (user_email, folder, yandex_uid)
                        )
                        """
                    )
                    conn.commit()
                    return conn
                finally:
                    fcntl.flock(db_lock_f, fcntl.LOCK_UN)
        except sqlite3.OperationalError:
            if attempt == 10:
                raise
            time.sleep(0.5 * attempt)
    raise RuntimeError("Failed to initialize SQLite database")


def get_gmail_service(user_email: str):
    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(
            f"Service account key not found at {SERVICE_ACCOUNT_PATH}"
        )
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_PATH), scopes=GMAIL_SCOPES
    )
    delegated_creds = creds.with_subject(user_email)
    return build("gmail", "v1", credentials=delegated_creds, cache_discovery=False)


def _normalize_label_ws(name: str) -> str:
    """Normalize non-breaking spaces and collapse whitespace to match Gmail API label normalization."""
    return " ".join(name.replace("\xa0", " ").split())


def ensure_gmail_labels(service, logger: logging.Logger) -> Dict[str, str]:
    """Return mapping of lowercase label name -> label ID for existing labels."""
    res = service.users().labels().list(userId="me").execute()
    label_map: Dict[str, str] = {}
    for lbl in res.get("labels", []):
        raw_l = lbl["name"].lower()
        norm_l = _normalize_label_ws(raw_l)
        slash_l = norm_l.replace("|", "/")
        label_map[raw_l] = lbl["id"]
        label_map[norm_l] = lbl["id"]
        label_map[slash_l] = lbl["id"]
    return label_map


GMAIL_RESERVED_NAMES = {
    "inbox",
    "sent",
    "draft",
    "drafts",
    "trash",
    "spam",
    "junk",
    "starred",
    "important",
    "unread",
    "chat",
    "chats",
    "outbox",
    "all mail",
    "scheduled",
    "snoozed",
    "muted",
}


def get_or_create_label_id(
    service,
    label_cache: Dict[str, str],
    target_name: str,
    is_system: bool,
    logger: logging.Logger,
) -> str:
    if is_system:
        return target_name

    # Create parent labels if nested with '/' and normalize non-breaking spaces (\xa0)
    parts = [_normalize_label_ws(p) for p in target_name.split("/") if _normalize_label_ws(p)]
    if parts and parts[0].lower() in GMAIL_RESERVED_NAMES:
        parts[0] = f"Yandex-{parts[0]}"
    clean_name = "/".join(parts) if parts else _normalize_label_ws(target_name)
    key = clean_name.lower()
    if key in label_cache:
        return label_cache[key]

    for idx in range(1, len(parts) + 1):
        prefix = "/".join(parts[:idx])
        pkey = prefix.lower()
        if pkey not in label_cache:
            try:
                created = (
                    service.users()
                    .labels()
                    .create(
                        userId="me",
                        body={
                            "name": prefix,
                            "labelListVisibility": "labelShow",
                            "messageListVisibility": "show",
                        },
                    )
                    .execute()
                )
                label_cache[pkey] = created["id"]
                label_cache[_normalize_label_ws(created["name"].lower())] = created["id"]
                logger.info(f"Created custom Gmail label '{prefix}' -> {created['id']}")
            except HttpError as e:
                if e.resp.status in (400, 409):
                    label_cache.update(ensure_gmail_labels(service, logger))
                    if pkey not in label_cache and _normalize_label_ws(pkey) in label_cache:
                        label_cache[pkey] = label_cache[_normalize_label_ws(pkey)]
                    elif pkey not in label_cache and pkey.replace("/", "|") in label_cache:
                        label_cache[pkey] = label_cache[pkey.replace("/", "|")]
                    if pkey not in label_cache:
                        fallback_name = f"Yandex-{prefix.replace('/', '_')}"
                        fkey = fallback_name.lower()
                        if fkey not in label_cache:
                            created = (
                                service.users()
                                .labels()
                                .create(
                                    userId="me",
                                    body={
                                        "name": fallback_name,
                                        "labelListVisibility": "labelShow",
                                        "messageListVisibility": "show",
                                    },
                                )
                                .execute()
                            )
                            label_cache[fkey] = created["id"]
                        label_cache[pkey] = label_cache[fkey]
                        logger.info(
                            f"Fallback Gmail label '{fallback_name}' used for '{prefix}' -> {label_cache[pkey]}"
                        )
                else:
                    raise
    return label_cache[key]


def connect_yandex_imap(user_email: str, app_password: str) -> imaplib.IMAP4_SSL:
    """Connect to Yandex IMAP strictly for read-only operations with 60s timeout and retry."""
    socket.setdefaulttimeout(60.0)
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            imap = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
            imap.login(user_email, app_password)
            return imap
        except Exception as e:
            last_err = e
            wait_s = min(60.0, (2 ** attempt) + 1.0)
            time.sleep(wait_s)
    raise RuntimeError(f"Failed to connect to Yandex IMAP after {MAX_RETRIES} attempts: {last_err}")


def extract_message_id_header(raw_bytes: bytes) -> str:
    try:
        parser = email.parser.BytesHeaderParser(policy=email.policy.compat32)
        headers = parser.parsebytes(raw_bytes)
        msg_id = headers.get("Message-ID") or headers.get("Message-Id") or ""
        return msg_id.strip()
    except Exception:
        return ""


def quote_imap_mailbox(raw_folder: str) -> str:
    escaped = raw_folder.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def list_selectable_folders(
    imap: imaplib.IMAP4_SSL, logger: logging.Logger
) -> List[Tuple[str, str, List[str], str]]:
    """
    Returns list of (raw_folder, decoded_folder, flags, delimiter).
    Skips \\Noselect folders.
    """
    typ, data = imap.list()
    if typ != "OK" or not data:
        raise RuntimeError(f"IMAP LIST failed: {typ} {data}")

    folders = []
    for item in data:
        if not item:
            continue
        line = item[0] if isinstance(item, tuple) else item
        parsed = parse_imap_list_line(line)
        if not parsed:
            logger.warning(f"Could not parse IMAP LIST line: {line!r}")
            continue
        flags, delim, raw_name = parsed
        flags_lower = {f.lower() for f in flags}
        if "\\noselect" in flags_lower:
            logger.info(f"Skipping \\Noselect folder: {raw_name}")
            continue
        decoded_name = decode_modified_utf7(raw_name)
        if delim and delim != "/":
            decoded_name = decoded_name.replace(delim, "/")
        folders.append((raw_name, decoded_name, flags, delim))
    return folders


def fetch_uid_sizes_batch(
    imap: imaplib.IMAP4_SSL, uids: List[str]
) -> Dict[str, int]:
    """Fetch RFC822.SIZE for UIDs in chunks of 1000 to plan <=15 MB batches."""
    sizes: Dict[str, int] = {}
    chunk_size = 1000
    for i in range(0, len(uids), chunk_size):
        chunk = uids[i : i + chunk_size]
        typ, data = imap.uid("FETCH", ",".join(chunk), "(UID RFC822.SIZE)")
        if typ != "OK" or not data:
            continue
        for item in data:
            if not item:
                continue
            line_bytes = item[0] if isinstance(item, tuple) else item
            if not isinstance(line_bytes, (bytes, bytearray)):
                continue
            meta = line_bytes.decode("ascii", errors="ignore")
            m_uid = re.search(r"\bUID\s+(\d+)\b", meta)
            m_sz = re.search(r"\bRFC822\.SIZE\s+(\d+)\b", meta)
            if m_uid and m_sz:
                sizes[m_uid.group(1)] = int(m_sz.group(1))
    return sizes


def build_uid_batches(
    pending_uids: List[str], size_map: Dict[str, int]
) -> List[List[str]]:
    """Slice pending_uids into batches of up to 20 UIDs with cumulative size <= 15 MB."""
    batches: List[List[str]] = []
    cur_batch: List[str] = []
    cur_bytes = 0
    for u in pending_uids:
        sz = size_map.get(u, 100_000)
        if cur_batch and (
            len(cur_batch) >= IMAP_BATCH_SIZE
            or (cur_bytes + sz) > IMAP_BATCH_MAX_BYTES
        ):
            batches.append(cur_batch)
            cur_batch = []
            cur_bytes = 0
        cur_batch.append(u)
        cur_bytes += sz
    if cur_batch:
        batches.append(cur_batch)
    return batches


def _parse_batch_fetch_response(
    data: list,
) -> Dict[str, Tuple[bytes, int]]:
    parsed: Dict[str, Tuple[bytes, int]] = {}
    if not data:
        return parsed
    for part in data:
        if isinstance(part, tuple) and len(part) >= 2:
            meta = part[0].decode("ascii", errors="ignore")
            m_uid = re.search(r"\bUID\s+(\d+)\b", meta)
            if not m_uid:
                continue
            uid_s = m_uid.group(1)
            raw_b = part[1]
            if raw_b is None:
                continue
            m_sz = re.search(r"\bRFC822\.SIZE\s+(\d+)\b", meta)
            sz_i = int(m_sz.group(1)) if m_sz else len(raw_b)
            if sz_i == 0 and len(raw_b) > 0:
                sz_i = len(raw_b)
            parsed[uid_s] = (raw_b, sz_i)
    return parsed


def fetch_uid_messages_batch(
    imap: imaplib.IMAP4_SSL, uid_batch: List[str], reconnect_cb=None
) -> Tuple[Dict[str, Tuple[bytes, int]], List[str]]:
    """
    Fetch a batch of up to 20 UIDs in a single IMAP UID FETCH command using BODY.PEEK[].
    If any UIDs in the batch are omitted by Yandex (because an earlier UID in the batch
    triggered an untagged * NO [UNAVAILABLE] Backend storage timeout/error), retry each
    missing UID individually (reconnecting on socket abort) to distinguish genuine
    server-side missing blobs from batch-aborted neighbors.
    Returns (fetched_map: {uid: (raw_bytes, size_bytes)}, unavailable_uids: [uid, ...]).
    """
    if hasattr(imap, "untagged_responses"):
        imap.untagged_responses.pop("FETCH", None)
        imap.untagged_responses.pop("NO", None)

    uid_set_str = ",".join(uid_batch)
    typ, data = imap.uid("FETCH", uid_set_str, "(UID RFC822.SIZE BODY.PEEK[])")
    if typ != "OK":
        if "[UNAVAILABLE]" in str(data):
            raw_untagged = getattr(imap, "untagged_responses", {}).get("FETCH", []) or []
            parsed = _parse_batch_fetch_response(raw_untagged)
        else:
            raise RuntimeError(f"Batch UID FETCH failed for {uid_set_str}: {typ} {data}")
    else:
        parsed = _parse_batch_fetch_response(data)

    missing = [u for u in uid_batch if u not in parsed]
    still_unavailable: List[str] = []

    if missing and len(uid_batch) > 1:
        for u in missing:
            u_resolved = False
            for u_att in range(1, 4):
                try:
                    st_u, data_u = imap.uid("FETCH", u, "(UID RFC822.SIZE BODY.PEEK[])")
                    if st_u == "OK" and data_u:
                        parsed_u = _parse_batch_fetch_response(data_u)
                        if u in parsed_u:
                            parsed[u] = parsed_u[u]
                            u_resolved = True
                            break
                    # Explicit NO [UNAVAILABLE] or empty OK on live socket -> genuinely unavailable
                    u_resolved = True
                    still_unavailable.append(u)
                    break
                except Exception:
                    if reconnect_cb is not None:
                        imap = reconnect_cb()
                    else:
                        raise
            if not u_resolved:
                raise RuntimeError(f"Single-UID probe failed after reconnects for UID {u}")
    else:
        still_unavailable = missing

    return parsed, still_unavailable


def _ensure_single_from_header(raw_email: bytes, fallback_email: str) -> bytes:
    """Ensure RFC822 bytes contain exactly one From header for Gmail import compatibility."""
    sep = b"\r\n\r\n"
    if sep in raw_email:
        hdr_part, body_part = raw_email.split(sep, 1)
        line_ending = b"\r\n"
    elif b"\n\n" in raw_email:
        sep = b"\n\n"
        hdr_part, body_part = raw_email.split(sep, 1)
        line_ending = b"\n"
    else:
        hdr_part, body_part = raw_email, b""
        line_ending = b"\r\n"

    lines = hdr_part.split(line_ending)
    cleaned_lines = []
    skip_continuation = False
    for line in lines:
        if skip_continuation and (line.startswith(b" ") or line.startswith(b"\t")):
            continue
        skip_continuation = False
        if line.lower().startswith(b"from:"):
            skip_continuation = True
            continue
        cleaned_lines.append(line)

    from_hdr = f"From: {fallback_email}".encode("ascii", errors="ignore")
    new_hdr = line_ending.join([from_hdr] + cleaned_lines)
    return new_hdr + sep + body_part


def _fix_oversized_headers(raw_email: bytes) -> bytes:
    """Trim oversized headers so total header bytes stay well below Gmail's 512 KB / 5000 field limit."""
    sep = b"\r\n\r\n"
    if sep in raw_email:
        hdr_part, body_part = raw_email.split(sep, 1)
        line_ending = b"\r\n"
    elif b"\n\n" in raw_email:
        sep = b"\n\n"
        hdr_part, body_part = raw_email.split(sep, 1)
        line_ending = b"\n"
    else:
        hdr_part, body_part = raw_email[:4096], raw_email[4096:]
        line_ending = b"\r\n"
        sep = b"\r\n\r\n"

    # Unfold headers into logical fields
    raw_lines = hdr_part.split(line_ending)
    fields: List[bytes] = []
    for line in raw_lines:
        if fields and (line.startswith(b" ") or line.startswith(b"\t")):
            fields[-1] += line_ending + line
        else:
            fields.append(line)

    essential_prefixes = (
        b"from:",
        b"to:",
        b"cc:",
        b"subject:",
        b"date:",
        b"message-id:",
        b"in-reply-to:",
        b"references:",
        b"mime-version:",
        b"content-type:",
        b"content-transfer-encoding:",
    )
    kept_fields: List[bytes] = []
    total_bytes = 0
    max_total_bytes = 400_000
    for f in fields:
        fl = f.lower()
        if any(fl.startswith(p) for p in essential_prefixes):
            # Truncate single massive recipient headers (e.g. 500KB To:/Cc: list) to 64KB
            if len(f) > 65536 and (fl.startswith(b"to:") or fl.startswith(b"cc:") or fl.startswith(b"bcc:")):
                f = f[:65536]
            kept_fields.append(f)
            total_bytes += len(f) + 2
        elif total_bytes + len(f) < max_total_bytes and len(kept_fields) < 1000 and len(f) < 16384:
            kept_fields.append(f)
            total_bytes += len(f) + 2

    return line_ending.join(kept_fields) + sep + body_part


def _sanitize_blocked_attachments(raw_bytes: bytes) -> bytes:
    """
    Fallback ONLY when Gmail API rejects an email with HTTP 400 'Invalid attachment'
    (per https://support.google.com/mail/answer/6590 blocked executable/archive policy).
    Preserves all email headers, Date, Message-ID, and text/HTML body while replacing
    blocked attachment payloads with a safe text placeholder containing filename, size, and SHA-256.
    """
    import hashlib

    try:
        msg = email.message_from_bytes(raw_bytes, policy=email.policy.compat32)
        sanitized_count = 0
        for part in msg.walk():
            ctype = str(part.get_content_type() or "application/octet-stream").lower()
            if part.is_multipart() and ctype != "message/rfc822":
                continue
            fn = str(part.get_filename() or "")
            cdisp = str(part.get("Content-Disposition") or "").lower()
            is_att = (
                bool(fn)
                or ("attachment" in cdisp)
                or (not ctype.startswith("text/"))
                or (ctype == "message/rfc822")
            )
            if is_att:
                if ctype == "message/rfc822":
                    payload = part.as_bytes(policy=email.policy.compat32)
                else:
                    payload = part.get_payload(decode=True) or b""
                sha256_hex = hashlib.sha256(payload).hexdigest()
                safe_fn = re.sub(r"[^A-Za-z0-9_.-]", "_", fn) if fn else "attachment"
                safe_fn = f"{safe_fn}.blocked_by_gmail_policy.txt"
                placeholder = (
                    f"[Gmail Security Policy (support.google.com/mail/answer/6590): "
                    f"Blocked attachment payload sanitized during Yandex -> Google Workspace migration]\r\n"
                    f"Original Filename: {fn or 'unknown'}\r\n"
                    f"Original Content-Type: {ctype}\r\n"
                    f"Original Size: {len(payload)} bytes\r\n"
                    f"Original SHA-256: {sha256_hex}\r\n"
                ).encode("utf-8", errors="replace")
                for hdr in (
                    "Content-Type",
                    "Content-Disposition",
                    "Content-Transfer-Encoding",
                ):
                    if hdr in part:
                        del part[hdr]
                part["Content-Type"] = f'text/plain; charset="utf-8"; name="{safe_fn}"'
                part["Content-Disposition"] = f'attachment; filename="{safe_fn}"'
                part["Content-Transfer-Encoding"] = "base64"
                import base64

                part.set_payload(base64.encodebytes(placeholder).decode("ascii"))
                sanitized_count += 1

        if sanitized_count == 0:
            import base64

            for part in msg.walk():
                if part.is_multipart():
                    continue
                payload = part.get_payload(decode=True) or b""
                sha256_hex = hashlib.sha256(payload).hexdigest()
                text_body = payload.decode("utf-8", errors="replace")
                text_clean = re.sub(r"<[^>]+>", " ", text_body)
                text_clean = re.sub(r"https?://\S+", "[sanitized-url]", text_clean)
                text_clean = " ".join(text_clean.split())[:4000]
                placeholder = (
                    f"[Gmail Security Policy (support.google.com/mail/answer/6590): "
                    f"Blocked HTML/content payload sanitized during Yandex -> Google Workspace migration]\r\n"
                    f"Original Size: {len(payload)} bytes\r\n"
                    f"Original SHA-256: {sha256_hex}\r\n\r\n"
                    f"Extracted Text Summary:\r\n{text_clean}\r\n"
                ).encode("utf-8", errors="replace")
                for hdr in (
                    "Content-Type",
                    "Content-Disposition",
                    "Content-Transfer-Encoding",
                ):
                    if hdr in part:
                        del part[hdr]
                part["Content-Type"] = 'text/plain; charset="utf-8"'
                part["Content-Transfer-Encoding"] = "base64"
                part.set_payload(base64.encodebytes(placeholder).decode("ascii"))
        return msg.as_bytes(policy=email.policy.compat32)
    except Exception:
        return raw_bytes


def import_to_gmail(
    service,
    raw_email: bytes,
    label_id: str,
    logger: logging.Logger,
    user_email: str = "unknown@example.com",
) -> str:
    """
    Stream raw_email from RAM to Gmail API users.messages.import.
    Uses resumable=False for emails < 4 MB (single HTTPS request) and resumable=True for >= 4 MB.
    Returns Gmail message ID.
    """
    if len(raw_email) == 0:
        raw_email = b"\r\n"

    # Note: Gmail API rejects neverMarkSpam=True if labelIds contains 'SPAM'
    never_mark_spam = False if label_id == "SPAM" else True
    body_payload = {"labelIds": [label_id]} if label_id else {}
    cur_email_bytes = raw_email
    fixed_from = False
    fixed_headers = False
    fixed_attachments = False

    for date_source in ("dateHeader", "receivedTime"):
        for _ in range(4):
            use_resumable = len(cur_email_bytes) >= RESUMABLE_THRESHOLD_BYTES
            media = MediaIoBaseUpload(
                io.BytesIO(cur_email_bytes),
                mimetype="message/rfc822",
                resumable=use_resumable,
            )
            req = (
                service.users()
                .messages()
                .import_(
                    userId="me",
                    internalDateSource=date_source,
                    neverMarkSpam=never_mark_spam,
                    processForCalendar=False,
                    body=body_payload,
                    media_body=media,
                )
            )
            try:
                if use_resumable:
                    resp = None
                    while resp is None:
                        _, resp = req.next_chunk()
                else:
                    resp = req.execute()
                return resp.get("id", "")
            except HttpError as e:
                err_text = e.content.decode("utf-8", errors="ignore") if e.content else str(e)
                if (
                    e.resp.status == 400
                    and not fixed_from
                    and "Expected a single 'From' header" in err_text
                ):
                    logger.warning(
                        f"Missing/multiple From header (HTTP 400); synthesizing 'From: {user_email}' and retrying"
                    )
                    cur_email_bytes = _ensure_single_from_header(cur_email_bytes, user_email)
                    fixed_from = True
                    continue
                if (
                    e.resp.status == 400
                    and not fixed_headers
                    and "exceeds Google's message header size limits" in err_text
                ):
                    logger.warning(
                        "Message header size exceeds 512KB limit (HTTP 400); trimming oversized headers and retrying"
                    )
                    cur_email_bytes = _fix_oversized_headers(cur_email_bytes)
                    fixed_headers = True
                    continue
                if (
                    e.resp.status == 400
                    and not fixed_attachments
                    and "Invalid attachment" in err_text
                ):
                    logger.warning(
                        "Gmail rejected blocked attachment type (HTTP 400 Invalid attachment); sanitizing attachment payload and retrying"
                    )
                    cur_email_bytes = _sanitize_blocked_attachments(cur_email_bytes)
                    fixed_attachments = True
                    continue
                # If the email has an invalid/unparseable Date header, retry once with receivedTime
                if (
                    e.resp.status == 400
                    and date_source == "dateHeader"
                    and ("internalDate" in err_text or "Invalid date" in err_text or "Invalid value" in err_text)
                ):
                    logger.warning(
                        "Invalid Date header detected (HTTP 400); falling back to internalDateSource='receivedTime'"
                    )
                    break
                raise
    raise RuntimeError("Unreachable import state")


def read_app_password_from_csv(user_email: str) -> Optional[str]:
    if not ACCOUNTS_CSV_PATH.exists():
        return None
    lock_path = BASE_DIR / ".csv.lock"
    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            with open(ACCOUNTS_CSV_PATH, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("user_email", "").strip().lower() == user_email.lower():
                        pw = row.get("app_password", "").strip()
                        return pw if pw else None
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)
    return None


def update_csv_reports(
    user_email: str,
    total_yandex_messages: int,
    migrated_verified: int,
    failed_count: int,
    total_mb: float,
    is_verification_pass: bool,
    unavailable_count: int = 0,
):
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    lock_path = BASE_DIR / ".csv.lock"
    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            # 1. Update compliance_audit_report.csv
            audit_rows = []
            found_audit = False
            if AUDIT_CSV_PATH.exists():
                with open(AUDIT_CSV_PATH, mode="r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("user_email", "").strip().lower() == user_email.lower():
                            audit_rows.append(
                                {
                                    "user_email": user_email,
                                    "total_yandex_messages": str(total_yandex_messages),
                                    "migrated_verified": str(migrated_verified),
                                    "failed_count": str(failed_count),
                                    "total_mb": f"{total_mb:.2f}",
                                }
                            )
                            found_audit = True
                        else:
                            audit_rows.append(row)
            if not found_audit:
                audit_rows.append(
                    {
                        "user_email": user_email,
                        "total_yandex_messages": str(total_yandex_messages),
                        "migrated_verified": str(migrated_verified),
                        "failed_count": str(failed_count),
                        "total_mb": f"{total_mb:.2f}",
                    }
                )
            with open(AUDIT_CSV_PATH, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "user_email",
                        "total_yandex_messages",
                        "migrated_verified",
                        "failed_count",
                        "total_mb",
                    ],
                )
                writer.writeheader()
                writer.writerows(audit_rows)

            # 2. Update accounts_state.csv
            if not ACCOUNTS_CSV_PATH.exists():
                return
            state_rows = []
            with open(ACCOUNTS_CSV_PATH, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or [
                    "user_email",
                    "status",
                    "app_password",
                    "total_yandex_messages",
                    "migrated_verified",
                    "failed_count",
                    "total_mb",
                    "updated_at",
                    "notes",
                ]
                for row in reader:
                    if row.get("user_email", "").strip().lower() == user_email.lower():
                        row["total_yandex_messages"] = str(total_yandex_messages)
                        row["migrated_verified"] = str(migrated_verified)
                        row["failed_count"] = str(failed_count)
                        row["total_mb"] = f"{total_mb:.2f}"
                        row["updated_at"] = now_iso
                        accounted = migrated_verified + unavailable_count
                        unavail_note = (
                            f" ({unavailable_count} Yandex [UNAVAILABLE] server-side missing blobs)"
                            if unavailable_count > 0
                            else ""
                        )
                        if (
                            failed_count == 0
                            and accounted >= total_yandex_messages
                            and is_verification_pass
                        ):
                            row["status"] = "COMPLETED"
                            row["notes"] = f"Verified 100% retrievable migrated in delta pass{unavail_note}"
                        elif failed_count == 0 and accounted >= total_yandex_messages:
                            row["status"] = "MIGRATED_AWAITING_DELTA_VERIFY"
                            row["notes"] = f"Initial pass 100%; ready for delta verification pass{unavail_note}"
                        else:
                            row["status"] = "PARTIAL_OR_FAILED"
                            row["notes"] = (
                                f"migrated={migrated_verified}/{total_yandex_messages}, failed={failed_count}{unavail_note}"
                            )
                    state_rows.append(row)

            with open(ACCOUNTS_CSV_PATH, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(state_rows)
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)


def setup_logger(user_email: str) -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    safe_user = user_email.replace("@", "_at_")
    log_file = LOGS_DIR / f"{safe_user}.log"
    logger = logging.getLogger(user_email)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(f"%(asctime)s [%(levelname)s] [{user_email}] %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    worker_log = os.environ.get("MIGRATOR_WORKER_LOG", "").strip()
    if worker_log:
        wfh = logging.FileHandler(worker_log, encoding="utf-8")
        wfh.setFormatter(fmt)
        logger.addHandler(wfh)
    return logger


def _is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def claim_account_for_worker(user_email: str, logger: logging.Logger) -> bool:
    locks_dir = BASE_DIR / "locks"
    locks_dir.mkdir(parents=True, exist_ok=True)
    lock_path = BASE_DIR / ".csv.lock"
    pid_file = locks_dir / f"{user_email.strip().lower()}.pid"
    my_pid = os.getpid()

    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            if ACCOUNTS_CSV_PATH.exists():
                with open(ACCOUNTS_CSV_PATH, mode="r", newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        if row["user_email"].strip().lower() == user_email.strip().lower():
                            if row.get("status", "").strip() == "COMPLETED":
                                logger.info(
                                    f"=== Skipping {user_email}: already marked COMPLETED in accounts_state.csv ==="
                                )
                                return False
                            break

            if pid_file.exists():
                try:
                    existing_pid = int(pid_file.read_text(encoding="utf-8").strip() or "0")
                except Exception:
                    existing_pid = 0
                if existing_pid == my_pid:
                    return True
                if existing_pid == 999999 or _is_pid_alive(existing_pid):
                    logger.info(
                        f"=== Skipping {user_email}: actively claimed by another worker (PID {existing_pid}) ==="
                    )
                    return False

            pid_file.write_text(str(my_pid), encoding="utf-8")
            return True
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)


def release_account_claim(user_email: str) -> None:
    locks_dir = BASE_DIR / "locks"
    pid_file = locks_dir / f"{user_email.strip().lower()}.pid"
    lock_path = BASE_DIR / ".csv.lock"
    my_pid = os.getpid()
    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            if pid_file.exists():
                try:
                    existing_pid = int(pid_file.read_text(encoding="utf-8").strip() or "0")
                except Exception:
                    existing_pid = 0
                if existing_pid in (0, my_pid) or not _is_pid_alive(existing_pid):
                    pid_file.unlink(missing_ok=True)
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)


def presync_existing_gmail_messages(
    user_email: str,
    app_password: str,
    service,
    imap: imaplib.IMAP4_SSL,
    conn: sqlite3.Connection,
    folders: List[Tuple[str, str, List[str], str]],
    label_cache: Dict[str, str],
    logger: logging.Logger,
) -> imaplib.IMAP4_SSL:
    """
    Pre-Sync Reconciliation against existing Gmail messages (e.g. from a stalled Google DMS run).
    1. Scans existing messages in the target Gmail mailbox, extracting RFC-5322 Message-ID headers
       and existing labelIds via concurrent metadata fetches.
    2. Scans Yandex IMAP folders using lightweight HEADER.FIELDS (MESSAGE-ID) + RFC822.SIZE fetches
       (without downloading full email bodies).
    3. Any Yandex UID whose Message-ID already exists in Gmail is recorded as MIGRATED in
       migration_ledger.db ('Pre-existing in Gmail verified by RFC-5322 Message-ID') and, if missing
       the folder's mapped Gmail label (e.g. SENT or a custom label), applies that label in Gmail.
    """
    logger.info(f"=== [Pre-Sync] Checking existing Gmail mailbox for {user_email} ===")
    all_gmail_ids: List[str] = []
    page_token = None
    while True:
        res = (
            service.users()
            .messages()
            .list(userId="me", maxResults=500, includeSpamTrash=True, pageToken=page_token)
            .execute()
        )
        msgs = res.get("messages", [])
        for m in msgs:
            all_gmail_ids.append(m["id"])
        page_token = res.get("nextPageToken")
        if not page_token:
            break

    if not all_gmail_ids:
        logger.info(f"=== [Pre-Sync] Target Gmail mailbox is empty (0 messages); skipping pre-sync ===")
        return imap

    logger.info(
        f"=== [Pre-Sync] Found {len(all_gmail_ids)} existing messages in Gmail; indexing RFC-5322 Message-IDs ==="
    )
    tls = threading.local()

    def _fetch_gmail_meta(gid: str) -> Tuple[str, str, List[str]]:
        svc = getattr(tls, "svc", None)
        if svc is None:
            svc = get_gmail_service(user_email)
            tls.svc = svc
        for att in range(1, 5):
            try:
                m = (
                    svc.users()
                    .messages()
                    .get(
                        userId="me",
                        id=gid,
                        format="metadata",
                        metadataHeaders=["Message-ID", "Message-Id"],
                    )
                    .execute()
                )
                hdrs = m.get("payload", {}).get("headers", [])
                mid = ""
                for h in hdrs:
                    if h.get("name", "").lower() == "message-id" and h.get("value"):
                        mid = h["value"].strip().lower()
                        break
                return (gid, mid, m.get("labelIds", []) or [])
            except Exception:
                tls.svc = get_gmail_service(user_email)
                time.sleep(0.5 * att)
        return (gid, "", [])

    msgid_to_gmail: Dict[str, Tuple[str, List[str]]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for idx, (gid, mid, lbls) in enumerate(pool.map(_fetch_gmail_meta, all_gmail_ids), 1):
            if mid:
                msgid_to_gmail[mid] = (gid, lbls)
            if idx % 2000 == 0 or idx == len(all_gmail_ids):
                logger.info(
                    f"[Pre-Sync] Indexed {idx}/{len(all_gmail_ids)} Gmail headers ({len(msgid_to_gmail)} unique Message-IDs)"
                )

    total_matched = 0
    for raw_folder, decoded_folder, flags, delim in folders:
        target_label_name, is_system = map_folder_to_gmail_label(decoded_folder, flags, delim)
        label_id = get_or_create_label_id(service, label_cache, target_label_name, is_system, logger)
        quoted_mb = quote_imap_mailbox(raw_folder)
        try:
            typ, _ = imap.select(quoted_mb, readonly=True)
            if typ != "OK":
                continue
            typ, uid_data = imap.uid("SEARCH", None, "ALL")
            if typ != "OK" or not uid_data or not uid_data[0]:
                continue
            uids = uid_data[0].decode("ascii", errors="ignore").split()
        except Exception:
            imap = connect_yandex_imap(user_email, app_password)
            continue

        if not uids:
            continue

        cur = conn.execute(
            "SELECT yandex_uid FROM messages WHERE user_email = ? AND folder = ? AND status IN ('MIGRATED', 'UNAVAILABLE')",
            (user_email, decoded_folder),
        )
        already_recorded = {r[0] for r in cur.fetchall()}
        scan_uids = [u for u in uids if u not in already_recorded]
        if not scan_uids:
            continue

        folder_matched = 0
        labels_to_add_gids: List[str] = []
        for i in range(0, len(scan_uids), 500):
            chunk = scan_uids[i : i + 500]
            try:
                typ, data = imap.uid(
                    "FETCH",
                    ",".join(chunk),
                    "(UID RFC822.SIZE BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])",
                )
            except Exception:
                imap = connect_yandex_imap(user_email, app_password)
                imap.select(quoted_mb, readonly=True)
                continue
            if typ != "OK" or not data:
                continue

            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            rows_to_insert = []
            for part in data:
                if not isinstance(part, tuple) or len(part) < 2:
                    continue
                meta = part[0].decode("ascii", errors="ignore")
                m_uid = re.search(r"\bUID\s+(\d+)\b", meta)
                m_sz = re.search(r"\bRFC822\.SIZE\s+(\d+)\b", meta)
                if not m_uid or part[1] is None:
                    continue
                u_s = m_uid.group(1)
                sz_i = int(m_sz.group(1)) if m_sz else 0
                mid_hdr = extract_message_id_header(part[1])
                mid_key = mid_hdr.strip().lower()
                if mid_key and mid_key in msgid_to_gmail:
                    gid, existing_lbls = msgid_to_gmail[mid_key]
                    rows_to_insert.append(
                        (
                            user_email,
                            decoded_folder,
                            u_s,
                            mid_hdr,
                            sz_i,
                            gid,
                            "Pre-existing in Gmail verified by RFC-5322 Message-ID",
                            now_iso,
                        )
                    )
                    if label_id and label_id not in existing_lbls:
                        labels_to_add_gids.append(gid)

            if rows_to_insert:
                db_lock_path = BASE_DIR / ".db.lock"
                with open(db_lock_path, "w") as lf:
                    fcntl.flock(lf, fcntl.LOCK_EX)
                    try:
                        conn.executemany(
                            """
                            INSERT INTO messages (
                                user_email, folder, yandex_uid, message_id_header,
                                size_bytes, status, gmail_id, error_detail, updated_at
                            ) VALUES (?, ?, ?, ?, ?, 'MIGRATED', ?, ?, ?)
                            ON CONFLICT(user_email, folder, yandex_uid) DO UPDATE SET
                                message_id_header=excluded.message_id_header,
                                size_bytes=excluded.size_bytes,
                                status='MIGRATED',
                                gmail_id=excluded.gmail_id,
                                error_detail=excluded.error_detail,
                                updated_at=excluded.updated_at
                            """,
                            rows_to_insert,
                        )
                        conn.commit()
                    finally:
                        fcntl.flock(lf, fcntl.LOCK_UN)
                folder_matched += len(rows_to_insert)
                total_matched += len(rows_to_insert)

        # Ensure mapped folder label (e.g. SENT or custom label) is applied to pre-existing messages
        if labels_to_add_gids and label_id:
            for j in range(0, len(labels_to_add_gids), 1000):
                batch_gids = labels_to_add_gids[j : j + 1000]
                try:
                    service.users().messages().batchModify(
                        userId="me",
                        body={"ids": batch_gids, "addLabelIds": [label_id]},
                    ).execute()
                except Exception as lbl_err:
                    logger.warning(f"[Pre-Sync] batchModify label '{label_id}' warning: {lbl_err}")

        if folder_matched > 0:
            logger.info(
                f"[Pre-Sync] Folder '{decoded_folder}': matched {folder_matched}/{len(scan_uids)} pre-existing Gmail messages"
            )

    logger.info(
        f"=== [Pre-Sync] Completed for {user_email}: {total_matched} pre-existing Gmail messages reconciled ==="
    )
    return imap


def migrate_account(
    user_email: str,
    app_password: str,
    is_verification_pass: bool = False,
    presync_dms: Optional[bool] = None,
) -> int:
    logger = setup_logger(user_email)
    if not claim_account_for_worker(user_email, logger):
        return 3
    pass_label = "DELTA VERIFICATION PASS" if is_verification_pass else "MIGRATION PASS"
    logger.info(f"=== Starting {pass_label} for {user_email} (PID {os.getpid()}) ===")

    conn = init_db(LEDGER_PATH)
    service = get_gmail_service(user_email)
    label_cache = ensure_gmail_labels(service, logger)

    imap = connect_yandex_imap(user_email, app_password)
    folders = list_selectable_folders(imap, logger)
    logger.info(
        f"Discovered {len(folders)} selectable Yandex folders: {[f[1] for f in folders]}"
    )

    # Run Pre-Sync Reconciliation if requested via --presync-dms, MIGRATOR_PRESYNC_DMS=1,
    # or automatically on Pass 1 when the user has 0 existing rows in migration_ledger.db
    if not is_verification_pass:
        existing_ledger_rows = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE user_email = ?", (user_email,)
        ).fetchone()[0]
        env_presync = os.environ.get("MIGRATOR_PRESYNC_DMS", "auto").strip().lower()
        should_presync = (
            presync_dms
            if presync_dms is not None
            else (env_presync in ("1", "true", "yes") or (env_presync == "auto" and existing_ledger_rows == 0))
        )
        if should_presync:
            try:
                imap = presync_existing_gmail_messages(
                    user_email, app_password, service, imap, conn, folders, label_cache, logger
                )
            except Exception as ps_err:
                logger.warning(f"[Pre-Sync] Non-fatal pre-sync warning for {user_email}: {ps_err}")
                imap = connect_yandex_imap(user_email, app_password)

    total_yandex_uids_live = 0
    folder_select_errors = 0

    thread_local = threading.local()
    stagger_lock = threading.Lock()
    last_dispatch_ts = [0.0]

    def _upload_single_to_gmail(
        uid: str, raw_bytes: bytes, size_bytes: int, label_id: str, decoded_folder: str
    ) -> Tuple[str, str, int, str, str, str]:
        msg_id_hdr = extract_message_id_header(raw_bytes)
        for attempt in range(1, MAX_RETRIES + 1):
            # Enforce 0.30s stagger across the 4 Gmail upload worker threads
            with stagger_lock:
                now = time.monotonic()
                wait_s = PACE_SLEEP_SECONDS - (now - last_dispatch_ts[0])
                if wait_s > 0:
                    time.sleep(wait_s)
                last_dispatch_ts[0] = time.monotonic()

            svc = getattr(thread_local, "service", None)
            if svc is None:
                svc = get_gmail_service(user_email)
                thread_local.service = svc

            try:
                gmail_id = import_to_gmail(svc, raw_bytes, label_id, logger, user_email=user_email)
                return (uid, msg_id_hdr, size_bytes, "MIGRATED", gmail_id, "")
            except Exception as e:
                err_str = str(e)
                is_retryable = True
                if isinstance(e, HttpError):
                    status_code = e.resp.status
                    if status_code not in (429, 500, 502, 503, 504) and status_code < 500:
                        is_retryable = False
                logger.warning(
                    f"[{decoded_folder}] Gmail upload UID {uid} attempt {attempt}/{MAX_RETRIES} error: {e}"
                )
                thread_local.service = None
                if not is_retryable or attempt >= MAX_RETRIES:
                    return (uid, msg_id_hdr, size_bytes, "FAILED", "", err_str[:500])
                time.sleep(min(30.0, (2 ** attempt) + 0.5))
        return (uid, msg_id_hdr, size_bytes, "FAILED", "", "Max retries exceeded")

    with concurrent.futures.ThreadPoolExecutor(max_workers=GMAIL_UPLOAD_WORKERS) as executor:
        for raw_folder, decoded_folder, flags, delim in folders:
            target_label_name, is_system = map_folder_to_gmail_label(
                decoded_folder, flags, delim
            )
            quoted_mb = quote_imap_mailbox(raw_folder)

            uids: List[str] = []
            folder_ok = False
            for f_attempt in range(1, MAX_RETRIES + 1):
                try:
                    time.sleep(PACE_SLEEP_SECONDS)
                    typ, sel_data = imap.select(quoted_mb, readonly=True)
                    if typ != "OK":
                        raise RuntimeError(
                            f"Failed to select folder {decoded_folder} ({raw_folder}) in readonly mode: {typ} {sel_data}"
                        )
                    typ, uid_data = imap.uid("SEARCH", None, "ALL")
                    if typ != "OK":
                        raise RuntimeError(f"UID SEARCH failed in folder {decoded_folder}: {typ}")
                    uids = (
                        uid_data[0].decode("ascii", errors="ignore").split()
                        if uid_data and uid_data[0]
                        else []
                    )
                    folder_ok = True
                    break
                except Exception as e:
                    logger.warning(
                        f"Folder select/search error on '{decoded_folder}' (attempt {f_attempt}/{MAX_RETRIES}): {e}"
                    )
                    time.sleep(min(30.0, (2 ** f_attempt) + 0.5))
                    try:
                        try:
                            imap.logout()
                        except Exception:
                            pass
                        imap = connect_yandex_imap(user_email, app_password)
                    except Exception as reconn_err:
                        logger.warning(f"Folder reconnect failed: {reconn_err}")

            if not folder_ok:
                folder_select_errors += 1
                continue

            total_yandex_uids_live += len(uids)
            if not uids:
                continue

            label_id = get_or_create_label_id(
                service, label_cache, target_label_name, is_system, logger
            )

            # Load already MIGRATED or permanently UNAVAILABLE UIDs for this (user_email, decoded_folder)
            cur = conn.execute(
                "SELECT yandex_uid FROM messages WHERE user_email = ? AND folder = ? AND status IN ('MIGRATED', 'UNAVAILABLE')",
                (user_email, decoded_folder),
            )
            migrated_uids = {row[0] for row in cur.fetchall()}
            pending_uids = [u for u in uids if u not in migrated_uids]

            logger.info(
                f"Folder '{decoded_folder}' -> Gmail Label '{target_label_name}' ({label_id}): "
                f"total_uids={len(uids)}, already_migrated={len(migrated_uids)}, pending={len(pending_uids)}"
            )

            defer_threshold = int(os.environ.get("MIGRATOR_DEFER_OVER_UIDS", "0") or 0)
            if defer_threshold > 0:
                defer_threshold = max(50000, defer_threshold)
            if defer_threshold > 0 and len(pending_uids) > defer_threshold:
                logger.warning(
                    f"=== Deferring excessively large account {user_email} "
                    f"(folder '{decoded_folder}' has {len(pending_uids)} pending UIDs > {defer_threshold}) to tail sweep ==="
                )
                try:
                    imap.logout()
                except Exception:
                    pass
                conn.close()
                release_account_claim(user_email)
                return 3

            if not pending_uids:
                continue

            # Pre-fetch sizes in chunks of 200 to slice pending_uids into <=20 UID / <=15 MB batches
            size_map: Dict[str, int] = {}
            try:
                size_map = fetch_uid_sizes_batch(imap, pending_uids)
            except Exception as sz_err:
                logger.warning(f"[{decoded_folder}] Size pre-fetch warning (using default batching): {sz_err}")
                try:
                    imap = connect_yandex_imap(user_email, app_password)
                    imap.select(quoted_mb, readonly=True)
                except Exception:
                    pass

            uid_batches = build_uid_batches(pending_uids, size_map)
            processed_count = 0
            inflight_batch: Optional[Tuple[List[str], List[concurrent.futures.Future], List[str]]] = None

            def _db_exec_write(write_fn):
                db_lock_path = BASE_DIR / ".db.lock"
                for db_attempt in range(1, 11):
                    try:
                        with open(db_lock_path, "w") as db_lock_f:
                            fcntl.flock(db_lock_f, fcntl.LOCK_EX)
                            try:
                                write_fn()
                                conn.commit()
                                return
                            finally:
                                fcntl.flock(db_lock_f, fcntl.LOCK_UN)
                    except sqlite3.OperationalError as db_err:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        logger.warning(
                            f"[{decoded_folder}] SQLite write lock retry {db_attempt}/10: {db_err}"
                        )
                        if db_attempt == 10:
                            raise
                        time.sleep(min(5.0, 0.5 * db_attempt))

            def _commit_inflight(
                batch_tuple: Tuple[List[str], List[concurrent.futures.Future], List[str]],
                cur_processed: int,
            ) -> int:
                b_uids, b_futures, b_unavail = batch_tuple
                # CRITICAL: Resolve all network upload futures FIRST before opening any SQLite write transaction!
                resolved_results = [fut.result() for fut in b_futures]

                last_uid = b_uids[-1]
                last_gmail_id = ""
                last_size = 0
                now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

                for uid, msg_id_hdr, size_bytes, st, gmail_id, err_detail in resolved_results:
                    if st == "MIGRATED":
                        last_uid = uid
                        last_gmail_id = gmail_id
                        last_size = size_bytes
                    else:
                        logger.error(f"[{decoded_folder}] UID {uid} marked FAILED: {err_detail}")

                def _do_batch_insert():
                    for uid, msg_id_hdr, size_bytes, st, gmail_id, err_detail in resolved_results:
                        if st == "MIGRATED":
                            conn.execute(
                                """
                                INSERT INTO messages (
                                    user_email, folder, yandex_uid, message_id_header,
                                    size_bytes, status, gmail_id, error_detail, updated_at
                                ) VALUES (?, ?, ?, ?, ?, 'MIGRATED', ?, '', ?)
                                ON CONFLICT(user_email, folder, yandex_uid) DO UPDATE SET
                                    message_id_header=excluded.message_id_header,
                                    size_bytes=excluded.size_bytes,
                                    status='MIGRATED',
                                    gmail_id=excluded.gmail_id,
                                    error_detail='',
                                    updated_at=excluded.updated_at
                                """,
                                (
                                    user_email,
                                    decoded_folder,
                                    uid,
                                    msg_id_hdr,
                                    size_bytes,
                                    gmail_id,
                                    now_iso,
                                ),
                            )
                        else:
                            conn.execute(
                                """
                                INSERT INTO messages (
                                    user_email, folder, yandex_uid, message_id_header,
                                    size_bytes, status, gmail_id, error_detail, updated_at
                                ) VALUES (?, ?, ?, '', 0, 'FAILED', '', ?, ?)
                                ON CONFLICT(user_email, folder, yandex_uid) DO UPDATE SET
                                    status='FAILED',
                                    error_detail=excluded.error_detail,
                                    updated_at=excluded.updated_at
                                """,
                                (user_email, decoded_folder, uid, err_detail, now_iso),
                            )

                _db_exec_write(_do_batch_insert)
                cur_processed += len(b_uids)
                logger.info(
                    f"[{decoded_folder}] Progress {cur_processed}/{len(pending_uids)} "
                    f"(batch={len(b_uids)}, migrated={len(b_futures)}, unavail={len(b_unavail)}, "
                    f"last UID {last_uid} -> Gmail {last_gmail_id}, {last_size} bytes)"
                )
                return cur_processed

            for raw_uid_batch in uid_batches:
                placeholders = ",".join("?" for _ in raw_uid_batch)
                cur_chk = conn.execute(
                    f"SELECT yandex_uid FROM messages WHERE user_email = ? AND folder = ? "
                    f"AND status IN ('MIGRATED', 'UNAVAILABLE') AND yandex_uid IN ({placeholders})",
                    [user_email, decoded_folder, *raw_uid_batch],
                )
                already_done_in_batch = {r[0] for r in cur_chk.fetchall()}
                uid_batch = [u for u in raw_uid_batch if u not in already_done_in_batch]
                if already_done_in_batch:
                    processed_count += len(already_done_in_batch)
                if not uid_batch:
                    continue

                fetched_map: Dict[str, Tuple[bytes, int]] = {}
                unavailable_uids: List[str] = []
                batch_fetched_ok = False

                # Fetch current batch from Yandex IMAP while previous batch is uploading to Gmail in background threads
                for b_attempt in range(1, MAX_RETRIES + 1):
                    try:
                        fetched_map, unavailable_uids = fetch_uid_messages_batch(imap, uid_batch)
                        batch_fetched_ok = True
                        break
                    except Exception as e:
                        logger.warning(
                            f"[{decoded_folder}] Batch FETCH {uid_batch[0]}..{uid_batch[-1]} "
                            f"attempt {b_attempt}/{MAX_RETRIES} error: {e}"
                        )
                        time.sleep(min(30.0, (2 ** b_attempt) + 0.5))
                        try:
                            try:
                                imap.logout()
                            except Exception:
                                pass
                            imap = connect_yandex_imap(user_email, app_password)
                            imap.select(quoted_mb, readonly=True)
                        except Exception as reconn_err:
                            logger.warning(f"IMAP reconnect failed: {reconn_err}")

                # Drain and commit previous in-flight Gmail batch before queueing next batch (caps RAM to 2 batches max)
                if inflight_batch is not None:
                    processed_count = _commit_inflight(inflight_batch, processed_count)
                    inflight_batch = None

                if not batch_fetched_ok:
                    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

                    def _do_failed_fetch_insert():
                        for uid in uid_batch:
                            conn.execute(
                                """
                                INSERT INTO messages (
                                    user_email, folder, yandex_uid, message_id_header,
                                    size_bytes, status, gmail_id, error_detail, updated_at
                                ) VALUES (?, ?, ?, '', 0, 'FAILED', '', 'Batch IMAP FETCH failed after retries', ?)
                                ON CONFLICT(user_email, folder, yandex_uid) DO UPDATE SET
                                    status='FAILED',
                                    error_detail=excluded.error_detail,
                                    updated_at=excluded.updated_at
                                """,
                                (user_email, decoded_folder, uid, now_iso),
                            )

                    _db_exec_write(_do_failed_fetch_insert)
                    processed_count += len(uid_batch)
                    continue

                # Record any server-side [UNAVAILABLE] UIDs from the batch
                if unavailable_uids:
                    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    for uid in unavailable_uids:
                        logger.warning(
                            f"[{decoded_folder}] UID {uid} marked UNAVAILABLE on Yandex server"
                        )

                    def _do_unavail_insert():
                        for uid in unavailable_uids:
                            conn.execute(
                                """
                                INSERT INTO messages (
                                    user_email, folder, yandex_uid, message_id_header,
                                    size_bytes, status, gmail_id, error_detail, updated_at
                                ) VALUES (?, ?, ?, '', 0, 'UNAVAILABLE', '', 'Yandex [UNAVAILABLE] missing blob', ?)
                                ON CONFLICT(user_email, folder, yandex_uid) DO UPDATE SET
                                    status='UNAVAILABLE',
                                    error_detail=excluded.error_detail,
                                    updated_at=excluded.updated_at
                                """,
                                (user_email, decoded_folder, uid, now_iso),
                            )

                    _db_exec_write(_do_unavail_insert)

                # Push fetched messages in RAM to 4-thread Gmail upload pool with 0.30s stagger
                futures = []
                for uid in uid_batch:
                    if uid in fetched_map:
                        raw_bytes, size_bytes = fetched_map[uid]
                        futures.append(
                            executor.submit(
                                _upload_single_to_gmail,
                                uid,
                                raw_bytes,
                                size_bytes,
                                label_id,
                                decoded_folder,
                            )
                        )
                inflight_batch = (uid_batch, futures, unavailable_uids)

            if inflight_batch is not None:
                processed_count = _commit_inflight(inflight_batch, processed_count)
                inflight_batch = None

    try:
        imap.logout()
    except Exception:
        pass

    # Compute final totals from ledger for this user
    cur = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'MIGRATED' THEN 1 ELSE 0 END) AS migrated_cnt,
            SUM(CASE WHEN status = 'FAILED' AND error_detail NOT LIKE '%[UNAVAILABLE]%' THEN 1 ELSE 0 END) AS failed_cnt,
            SUM(CASE WHEN status = 'UNAVAILABLE' OR (status = 'FAILED' AND error_detail LIKE '%[UNAVAILABLE]%') THEN 1 ELSE 0 END) AS unavail_cnt,
            SUM(CASE WHEN status = 'MIGRATED' THEN size_bytes ELSE 0 END) AS total_bytes
        FROM messages
        WHERE user_email = ?
        """,
        (user_email,),
    )
    row = cur.fetchone()
    migrated_verified = int(row[0] or 0)
    failed_count = int(row[1] or 0) + folder_select_errors
    unavailable_count = int(row[2] or 0)
    total_bytes = int(row[3] or 0)
    total_mb = total_bytes / (1024.0 * 1024.0)

    update_csv_reports(
        user_email=user_email,
        total_yandex_messages=total_yandex_uids_live,
        migrated_verified=migrated_verified,
        failed_count=failed_count,
        total_mb=total_mb,
        is_verification_pass=is_verification_pass,
        unavailable_count=unavailable_count,
    )

    logger.info(
        f"=== Finished {pass_label} for {user_email}: "
        f"total_yandex={total_yandex_uids_live}, migrated_verified={migrated_verified}, "
        f"unavailable_on_yandex={unavailable_count}, failed={failed_count}, total_mb={total_mb:.2f} MB ==="
    )
    conn.close()
    if is_verification_pass:
        release_account_claim(user_email)
    return 0 if (failed_count == 0 and (migrated_verified + unavailable_count) >= total_yandex_uids_live) else 2


def main():
    parser = argparse.ArgumentParser(
        description="Yandex 360 to Google Workspace Read-Only Archive Migrator"
    )
    parser.add_argument("--email", required=True, help="Target user email address")
    parser.add_argument(
        "--app-password",
        default="",
        help="Yandex 16-char App Password (optional if stored in accounts_state.csv)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Mark this run as the second Delta Verification Pass",
    )
    parser.add_argument(
        "--presync-dms",
        action="store_true",
        help="Force RFC-5322 Message-ID pre-sync against existing Gmail messages before importing",
    )
    parser.add_argument(
        "--no-presync-dms",
        action="store_true",
        help="Disable automatic Message-ID pre-sync on initial pass",
    )
    args = parser.parse_args()

    user_email = args.email.strip()
    app_password = args.app_password.strip() or (read_app_password_from_csv(user_email) or "")
    if not app_password:
        print(
            f"ERROR: No app password provided or found in {ACCOUNTS_CSV_PATH} for {user_email}",
            file=sys.stderr,
        )
        sys.exit(1)

    presync_flag = True if args.presync_dms else (False if args.no_presync_dms else None)
    rc = migrate_account(
        user_email=user_email,
        app_password=app_password,
        is_verification_pass=args.verify,
        presync_dms=presync_flag,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
