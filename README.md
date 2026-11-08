# Yandex 360 to Google Workspace Enterprise Email Migrator & Post-Mortem Incident Guide

A battle-tested, zero-disk-footprint, read-only IMAP-to-Gmail API migration engine (`imap.yandex.com:993` $\rightarrow$ Google Workspace `users.messages.import`) engineered after Google's native **Data Migration Service (DMS / IMAP Migration)** repeatedly stalled and silently skipped millions of messages across an enterprise corpus (~10 million messages / ~8.3 TB).

---

## Table of Contents
1. [Incident Report: Why Google Workspace IMAP Migration (DMS) Fails on Yandex 360](#1-incident-report-why-google-workspace-imap-migration-dms-fails-on-yandex-360)
2. [How This Engine Recovers Every Retrievable Email (Architecture & Fixes)](#2-how-this-engine-recovers-every-retrievable-email-architecture--fixes)
3. [Repository Structure](#3-repository-structure)
4. [Prerequisites & Google Workspace Setup](#4-prerequisites--google-workspace-setup)
5. [Step-by-Step Usage Guide](#5-step-by-step-usage-guide)
6. [Database Schema & Audit Reports](#6-database-schema--audit-reports)

---

## 1. Incident Report: Why Google Workspace IMAP Migration (DMS) Fails on Yandex 360

When migrating large enterprise organizations (hundreds of mailboxes, 100 GB – 500 GB per mailbox, 10+ years of history) from **Yandex 360** to **Google Workspace** using Google's built-in **Data Migration Service (DMS)**, engineers routinely encounter silent data loss, stalled progress bars, and `95%+` incomplete migrations.

Below is the root-cause analysis from production incident investigations across ~10 million Yandex UIDs:

### Root Cause 1: Yandex Legacy Storage Blobs (`NO [UNAVAILABLE]`) Poisoning Multi-UID Batches
In Yandex 360's distributed storage backend, older mailboxes frequently contain "ghost" or corrupted UIDs: the UID still exists in the folder's IMAP index (`UID SEARCH ALL`), and `UID FETCH <uid> (RFC822.SIZE)` even returns a valid byte size, **but the underlying storage blob is missing on Yandex's object store**.
- When an IMAP client requests a batch of UIDs—e.g. `UID FETCH 1001,1002,...,1020 (BODY.PEEK[])`—and **even one** UID in that batch is missing its blob on Yandex:
  1. Yandex streams untagged `* <seq> FETCH (UID ... BODY[] {size} ...)` responses for the healthy UIDs prior to the corrupted one, and then terminates the command with a tagged `NO [UNAVAILABLE]` response, **or**
  2. Yandex abruptly drops the TCP connection with `* BYE problems with connection` / `Connection reset by peer` (`ECONNRESET`) / `EOF`.
- **Why Google DMS Fails:** When Google DMS receives a tagged `NO [UNAVAILABLE]` or socket reset on a batch fetch, it discards the batch, backs off exponentially, and eventually marks the entire folder range as failed or stalls indefinitely on large `INBOX` folders (300,000–600,000 UIDs).
- **The Neighbor-Poisoning Trap in Naive Fallback Loops:** Even if a custom client falls back to fetching missing UIDs one-by-one (`for uid in missing_batch:`), when the corrupted UID triggers a Yandex `* BYE problems with connection` socket abort, **every subsequent healthy UID in that fallback loop will immediately fail with `Broken pipe` / `EOF` on the dead socket** unless the client explicitly catches the socket abort, reconnects, re-selects the mailbox (`READ-ONLY`), and resumes at the exact next UID. In production audits across hundreds of thousands of UIDs initially flagged `UNAVAILABLE` during batch fetches, **over 75% were healthy neighbor emails** that were recovered simply by harvesting untagged `FETCH` responses before `NO [UNAVAILABLE]` and reconnecting the socket inside the 1-by-1 probe loop.

### Root Cause 2: Yandex Aggressive Per-Mailbox Connection & Command Throttling
- Yandex 360 enforces strict per-mailbox concurrent connection limits and bandwidth windows on `imap.yandex.com:993`.
- Google DMS opens multiple parallel folder sync workers per user without adaptive backoff when Yandex resets sockets (`[Errno 54] Connection reset by peer`), causing DMS workers to thrash in a reconnect loop while transferring near-zero bytes.

### Root Cause 3: Modified UTF-7 Folder Encodings & Localized System Folders
- Yandex 360 mailboxes often contain a mix of Russian (`Отправленные`, `Удаленные`, `Черновики`, `Спам`, `Архив`) and English folder names encoded in **IMAP Modified UTF-7 (RFC 3501 §5.1.3)** alongside `|` or `/` hierarchy delimiters and non-breaking spaces (`\xa0`).
- Standard IMAP migration tools frequently fail to map localized system folders to native Gmail system labels (`SENT`, `TRASH`, `DRAFT`, `SPAM`), or fail with `HTTP 400 Invalid label name` when creating deeply nested custom hierarchies.

### Root Cause 4: Why `imapsync` & Standard IMAP `APPEND` Tools Fail (The 2.5 GB/day Gmail IMAP Cap vs. Gmail REST API)
When Google DMS stalls, engineers typically reach for **`imapsync`** or generic IMAP-to-IMAP copy scripts (`imap.yandex.com:993` $\rightarrow$ `imap.gmail.com:993`). On large enterprise mailboxes (50 GB – 500 GB per user), IMAP `APPEND` into Gmail fails in practice:
- **Google's Undocumented ~2.5 GB/day IMAP Upload Bandwidth Cap:** Gmail's IMAP server (`imap.gmail.com`) enforces a strict per-account daily upload limit of roughly **2.5 GB/day** on `APPEND` commands (`NO [OVERQUOTA] Account exceeded command or bandwidth limits`). At 2.5 GB/day, migrating a single 120 GB executive mailbox takes **48+ days** of continuous lockouts.
- **Why This Engine Uses the Gmail REST API (`users.messages.import`):** Instead of IMAP `APPEND`, `migrator.py` authenticates via **Google Cloud Service Account Domain-Wide Delegation** and streams raw RFC 822 payloads directly to the **Gmail REST API (`users.messages.import`)** (`resumable=False` single HTTPS request for `< 4 MB`, `resumable=True` chunked streaming for `>= 4 MB`):
  - **Bypasses the 2.5 GB/day IMAP Upload Cap:** Sustains **50–150+ GB/day per user** without triggering IMAP bandwidth lockouts.
  - **Native Archive Controls Unavailable in IMAP:** Sets `neverMarkSpam=True` (preventing Gmail's spam filter from silently burying 10 years of historical mail), `processForCalendar=False` (preventing thousands of phantom calendar invite emails from firing to external attendees), and `internalDateSource="dateHeader"` (preserving exact original message timestamps and thread ordering).

| Capability / Failure Mode | Google Workspace DMS | `imapsync` (IMAP `APPEND`) | `gyb` (Got Your Back) | **This Engine (`migrator.py`)** |
| :--- | :--- | :--- | :--- | :--- |
| **Upload Protocol** | Internal Google Sync | IMAP `APPEND` (`imap.gmail.com`) | Gmail REST API | **Gmail REST API (`users.messages.import`)** |
| **Daily Throughput per Mailbox** | Throttled / Stalls on Yandex | **Capped at ~2.5 GB/day** (`OVERQUOTA`) | 50–150 GB/day | **50–150+ GB/day (Resumable HTTPS)** |
| **Yandex `NO [UNAVAILABLE]` Batch Poisoning** | Aborts batch / stalls folder | Drops batch or aborts run | N/A (Gmail-only source) | **Harvests untagged `FETCH` + 1-by-1 auto-reconnect** |
| **`HTTP 400 Invalid attachment` (`.exe`/`.zip`)** | Silently drops entire email | Rejected by Gmail (`NO`) | Fails / skips email | **Auto-sanitizes blocked MIME part + SHA-256 receipt** |
| **`HTTP 400` Oversized Headers (`>512 KB`)** | Silently drops entire email | Rejected by Gmail | Fails / skips email | **Unfolds & trims header bloat to `<400 KB` in RAM** |
| **`HTTP 400` Missing/Duplicate `From:` or Bad `Date:`** | Silently drops entire email | Often rejected | Fails / skips email | **Synthesizes `From:` & falls back to `receivedTime`** |
| **Resuming Half-Failed Google DMS Mailboxes** | Creates duplicates or stalls | Slow header scan over IMAP | N/A | **Built-in RFC-5322 `Message-ID` Pre-Sync + Label Fix** |

### Root Cause 5: Strict Gmail API `users.messages.import` Payload Rejections (`HTTP 400`) & Python 8-Bit MIME Crash
Even when using the Gmail REST API, historical enterprise emails in Yandex contain malformed MIME structures or legacy attachments that Google's `users.messages.import` endpoint rejects outright with **`HTTP 400 Bad Request`** (causing standard API tools to permanently drop those emails):
1. **`HTTP 400 Invalid attachment` (Google Security Policy — `support.google.com/mail/answer/6590`):**
   - **The Problem:** Any historical email containing a legacy `.exe`, `.bat`, `.cmd`, `.scr`, `.msi`, `.vbs`, or an archive (`.zip`/`.tar`/`.gz`) containing blocked extensions—or certain flagged HTML payloads—causes Gmail API to reject the **entire email** with `400 Bad Request: Invalid attachment`. Standard tools drop the whole message, losing critical contract/accounting text and breaking thread history.
   - **How `migrator.py` Recovers It:** `_sanitize_blocked_attachments()` catches `HTTP 400 Invalid attachment` in RAM, walks the MIME tree, and replaces **only** the offending attachment part with a clean `.blocked_by_gmail_policy.txt` forensic receipt containing the **Original Filename, Content-Type, Byte Size, and `SHA-256` cryptographic hash**, while preserving all original headers, `Date`, `Message-ID`, and email body text, then immediately retries the import.
2. **`HTTP 400 Message header size exceeds Google's message header size limits` (`>512 KB` / `>5,000` fields):**
   - **The Problem:** Old enterprise announcement threads with thousands of `To:`/`Cc:` recipients or circular Exchange/relay `X-*` headers exceed Gmail's hard `512 KB` total header limit.
   - **How `migrator.py` Recovers It:** `_fix_oversized_headers()` unfolds the raw RFC 822 header block in RAM, strips non-essential `X-*` bloat, caps massive `To:`/`Cc:`/`Bcc:` lines at `64 KB` (`<400 KB` total), preserves `From`, `Subject`, `Date`, `Message-ID`, `In-Reply-To`, `References`, and `Content-Type`, and retries.
3. **`HTTP 400 Expected a single 'From' header`:**
   - **The Problem:** Legacy ERP, scanner, fax, or cron emails that have either **zero** `From:` headers or **duplicate** `From:` headers are rejected with `400 Bad Request: Expected a single 'From' header`.
   - **How `migrator.py` Recovers It:** `_ensure_single_from_header()` strips duplicate/malformed `From:` lines in RAM, synthesizes a compliant `From: <user_email>` header, and retries.
4. **`HTTP 400 Invalid internalDate` / Unparseable `Date:` Header:**
   - **The Problem:** Malformed or non-RFC-2822 `Date:` headers cause `internalDateSource="dateHeader"` to fail with `HTTP 400 Invalid value / Invalid date`.
   - **How `migrator.py` Recovers It:** Automatically catches the `400` date error and retries the import with `internalDateSource="receivedTime"`.
5. **Python 3 Standard Library `email.generator.BytesGenerator` 8-Bit `message/rfc822` Crash (`UnicodeEncodeError`):**
   - **The Problem:** When `google-api-python-client` serializes multipart upload requests containing raw 8-bit bytes inside `MediaIoBaseUpload(..., mimetype="message/rfc822")`, Python's standard library `BytesGenerator._handle_message` attempts to ASCII-encode the payload and crashes with `UnicodeEncodeError: 'ascii' codec can't encode characters`.
   - **How `migrator.py` Fixes It:** `_safe_handle_message()` patches `_BytesGenerator._handle_message` at startup to write raw `bytes` directly to the stream buffer without ASCII re-encoding.

---

## 2. How This Engine Recovers Every Retrievable Email (Architecture & Fixes)

```
┌──────────────────────────────┐         Read-Only TLS (993)         ┌──────────────────────────────┐
│      Yandex 360 IMAP         │ ──────────────────────────────────▶ │    In-Memory Stream Engine   │
│  SELECT <folder> (READONLY)  │   1. UID SEARCH ALL + RFC822.SIZE   │       (migrator.py)          │
│  UID FETCH ... (BODY.PEEK[]) │   2. Size-Capped Batch Fetch        │                              │
└──────────────────────────────┘   3. Untagged Harvest on NO         │  • 0 Bytes Written to Disk   │
                                   4. Auto-Reconnect 1-by-1 Fallback │  • HTTP 400 MIME Sanitizer   │
                                                                     └──────────────┬───────────────┘
                                                                                    │
                                      ┌─────────────────────────────────────────────┼─────────────────────────────────────────────┐
                                      ▼                                             ▼                                             ▼
                       ┌─────────────────────────────┐               ┌─────────────────────────────┐               ┌─────────────────────────────┐
                       │   SQLite WAL Audit Ledger   │               │  Google Workspace Gmail API │               │   1-by-1 Rescue Orchestrator│
                       │    (migration_ledger.db)    │               │   (users.messages.import)   │               │ (rescue_unavailable_all.py) │
                       │                             │               │                             │               │                             │
                       │ • Tracks every (user,       │               │ • Resumable upload >= 4 MB  │               │ • Re-verifies every         │
                       │   folder, yandex_uid)       │               │ • 50-150+ GB/day REST API   │               │   UNAVAILABLE UID 1-by-1    │
                       │ • RFC-5322 Message-ID Index │               │ • Pre-Sync Deduplication    │               │ • Auto-reconnects on BYE    │
                       └─────────────────────────────┘               └─────────────────────────────┘               └─────────────────────────────┘
```

1. **Untagged `FETCH` Harvesting + Reconnecting 1-by-1 Fallback (`migrator.py` & `rescue_unavailable_all.py`):**
   - Before fetching bodies, the engine queries `RFC822.SIZE` and groups UIDs into batches of at most `20` messages or `15 MB` (`IMAP_BATCH_MAX_BYTES`).
   - If Yandex returns a tagged `NO [UNAVAILABLE]` on a batch, the engine **directly inspects `imap.untagged_responses['FETCH']`** and harvests every healthy message body that Yandex already transmitted before hitting the bad UID.
   - For any remaining missing UIDs in the batch, the engine probes them **1-by-1 (`UID FETCH <uid> (BODY.PEEK[])`)**. If a corrupted UID causes Yandex to drop the socket (`EOF` / `Broken pipe` / `* BYE`), the engine catches the socket error, opens a fresh SSL connection, re-selects the folder `READ-ONLY`, and continues testing the remaining UIDs in the batch so **zero healthy neighbors are ever lost**.
2. **Built-In Pre-Sync Reconciliation Against Existing Google DMS Mailboxes (`--presync-dms`):**
   - If an account was partially migrated by Google DMS before switching to this tool, `presync_existing_gmail_messages()` in `migrator.py` automatically runs on `Pass 1` (whenever `migration_ledger.db` has `0` prior rows for that user, or when `--presync-dms` is passed).
   - It scans the target Gmail mailbox first, extracts all existing `Message-ID` headers (`format="metadata"`), and matches them against Yandex's lightweight `HEADER.FIELDS (MESSAGE-ID)` inventory **without downloading email bodies**.
   - Already-migrated messages are recorded in `migration_ledger.db` as `MIGRATED` (`Pre-existing in Gmail verified by RFC-5322 Message-ID`) and skipped without creating duplicates. Furthermore, if DMS imported a `Sent` or subfolder message into `All Mail`/`INBOX` without applying its `SENT` or custom folder label, `presync_existing_gmail_messages()` automatically batch-applies the missing Gmail label via `users.messages.batchModify`.
3. **Automatic In-Memory Healing of All Gmail `HTTP 400` Rejections:**
   - Sanitizes blocked attachments (`.blocked_by_gmail_policy.txt` with `SHA-256`), trims `>512 KB` headers, synthesizes missing/duplicate `From:` headers, and falls back to `receivedTime` on corrupt `Date:` headers—all in RAM without temp files.
4. **Zero Disk Footprint & Strictly Read-Only Source Access:**
   - Every email payload is held in `io.BytesIO` in RAM and streamed directly to `users.messages.import` (`resumable=False` for `< 4 MB`, `resumable=True` chunked upload for `>= 4 MB`).
   - All Yandex folders are opened with `imap.select(quoted_mailbox, readonly=True)` and fetched via `BODY.PEEK[]`—source mailboxes are never modified.

---

## 3. Repository Structure

| File | Purpose |
| :--- | :--- |
| `migrator.py` | Core read-only Yandex IMAP to Gmail API migration engine, folder-to-label mapper, MIME sanitizer, pre-sync deduplicator, and delta verifier. |
| `run_sequential_all.py` | Multi-worker account orchestrator (`--worker-id 1..N --num-workers N`). Manages per-account provisioning, Pass 1 (Migration), Pass 2 (`--verify` Delta Pass), and CSV reporting. |
| `rescue_unavailable_all.py` | Multi-worker 1-by-1 `UNAVAILABLE` UID auditor and delta rescue orchestrator (`--init-queue`, `--worker-id 1..N`). Re-verifies every `UNAVAILABLE` UID in `migration_ledger.db` with auto-reconnect and imports any responding message. |
| `provision_user.py` | Optional headless Playwright utility to automate generating 16-character Yandex App Passwords (`id.yandex.com/security/app-passwords`) and enabling IMAP in Yandex Mail settings if disabled. |
| `accounts_state.example.csv` | Template for `accounts_state.csv` (tracks per-account status, app password, message counts, and MiB volume). |
| `app_passwords.example.csv` | Template for `app_passwords.csv` (stores `user_email`, `master_password`, `app_password`). |
| `service_account.example.json` | Template for `service_account.json` (Google Cloud Service Account key with Domain-Wide Delegation). |
| `requirements.txt` | Python dependencies (`google-api-python-client`, `google-auth`, `google-auth-httplib2`, `playwright`). |

---

## 4. Prerequisites & Google Workspace Setup

### 1. Google Cloud Service Account & Domain-Wide Delegation
1. In the [Google Cloud Console](https://console.cloud.google.com/), enable the **Gmail API**.
2. Create a Service Account and download its JSON key file as `service_account.json` in the repository root.
3. Copy the Service Account's **Client ID** (`client_id` in `service_account.json`).
4. In the **Google Workspace Admin Console** (`Security` $\rightarrow$ `Access and data control` $\rightarrow$ `API controls` $\rightarrow$ `Manage Domain Wide Delegation`), add a new API client with your Service Account's Client ID and grant the scope:
   ```text
   https://mail.google.com/
   ```

### 2. Yandex 360 App Passwords & UI Localization Note
Yandex 360 requires a 16-character **App Password** for IMAP access (`imap.yandex.com:993`).
- If you already have app passwords (or IMAP passwords) for your users, place them in `accounts_state.csv` and `app_passwords.csv` (see `accounts_state.example.csv`).
- If you need to generate app passwords automatically from a temporary master password, use `provision_user.py --email user@yourdomain.com --password <master_password>`.

> **Important Localization Note (`provision_user.py`, `run_sequential_all.py`, `migrator.py`):**
> Because Yandex ID (`id.yandex.com`), Yandex Mail (`mail.yandex.com`), and Yandex 360 Admin (`admin.yandex.com`) render button labels according to the account's or organization's country/language setting, the browser automation and folder mapping scripts match visible text in **English** and **Russian** by default:
> - **`provision_user.py`:** Matches onboarding, App Password creation, and IMAP-enable buttons (`"Next"`, `"Continue"`, `"Create"`, `"Далее"`, `"Создать"`, `"Email clients"`, `"Почтовые программы"`, `"Save"`, `"Сохранить"`).
> - **`run_sequential_all.py` (optional Admin password reset):** Matches Yandex 360 Admin UI controls (`"Search"` / `"Поиск"`, `"Change password"` / `"Изменить пароль"`, `"Save"` / `"Сохранить"`, `"Cancel"` / `"Отмена"`, `"Close"` / `"Закрыть"`, `"Back"` / `"Назад"`).
> - **`migrator.py` (`map_folder_to_gmail_label`):** Maps English and Russian system folder names and subfolder prefixes (`Отправленные`, `Удаленные`, `Черновики`, `Спам`, `Архив`) to Gmail labels.
>
> **If your Yandex accounts or Admin Console use another language**, you must either switch the Yandex interface language to English/Russian or add your localized button labels and folder names to the selector lists in `provision_user.py`, `run_sequential_all.py`, and `migrator.py` before running automated provisioning.

### 3. Python Environment Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 5. Step-by-Step Usage Guide

### Step 1: Prepare `accounts_state.csv`
Copy the example template and list all mailboxes to migrate:
```bash
cp accounts_state.example.csv accounts_state.csv
cp app_passwords.example.csv app_passwords.csv
# Place your Google Service Account key at ./service_account.json
```

### Step 2: Run a Single Mailbox Migration & Delta Verification
To migrate a single account (`Pass 1`) and immediately run a delta verification pass (`Pass 2`):
```bash
# Pass 1: Full Read-Only Migration Pass
# (Automatically runs RFC-5322 Message-ID Pre-Sync if user has 0 rows in migration_ledger.db;
#  use --presync-dms to force pre-sync or --no-presync-dms to skip on brand-new empty mailboxes)
.venv/bin/python migrator.py --email alice@example.com --app-password abcdefghijklmnop --presync-dms

# Pass 2: Delta Verification Pass (audits all folders, imports newly arrived mail, retries any transient failures)
.venv/bin/python migrator.py --email alice@example.com --app-password abcdefghijklmnop --verify
```

### Step 3: Run Multi-Worker Batch Migration Across All Accounts
To migrate an entire organization concurrently across `N` workers (recommended: `3` to `8` workers):
```bash
# Launch 4 parallel workers (each worker processes its own slice of accounts_state.csv)
for wid in 1 2 3 4; do
  .venv/bin/python -u run_sequential_all.py --worker-id $wid --num-workers 4 &
done
```

### Step 4: Run the 1-by-1 `UNAVAILABLE` UID Rescue & Compliance Sweep
After all accounts complete `Pass 1` and `Pass 2`, run `rescue_unavailable_all.py` to individually probe every UID marked `UNAVAILABLE` in `migration_ledger.db` (slicing large accounts into 800-UID tasks across workers, reconnecting on socket aborts, and importing every responding message):
```bash
# 1. Initialize the shared interleaved task queue (locks/unavail_rescue_queue.json)
.venv/bin/python -u rescue_unavailable_all.py --init-queue

# 2. Launch parallel rescue workers (recommended: 4 to 8 workers across multiple accounts; 2 workers if only 1 account remains)
for wid in 1 2 3 4 5 6; do
  .venv/bin/python -u rescue_unavailable_all.py --worker-id $wid &
done
```
> **Throttling Tip:** If only a single large account remains at the end of the queue, scale down to **2 workers** so Yandex does not reset connections due to per-mailbox concurrency limits.

---

## 6. Database Schema & Audit Reports

All state is persisted transactionally in `migration_ledger.db` (SQLite with `WAL` mode enabled):

```sql
CREATE TABLE messages (
    user_email TEXT,
    folder TEXT,
    yandex_uid TEXT,
    message_id_header TEXT,
    size_bytes INT,
    status TEXT,          -- 'MIGRATED', 'UNAVAILABLE', or 'FAILED'
    gmail_id TEXT,        -- Gmail API message ID or 'DMS_PREEXISTING'
    error_detail TEXT,    -- Audit trail (e.g. 'Verified 1-by-1 Yandex [UNAVAILABLE] missing blob')
    updated_at TEXT,
    PRIMARY KEY (user_email, folder, yandex_uid)
);
```

At the end of every pass, `accounts_state.csv` and `compliance_audit_report.csv` are atomically updated so that for every account:
$$\text{Total Yandex UIDs} = \text{Migrated \& Verified} + \text{Confirmed Server-Side Unavailable} + \text{Failed (0)}$$
