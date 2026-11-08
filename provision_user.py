#!/usr/bin/env python3
"""
Isolated Playwright User Provisioner for Yandex 360 Migration.
1. Logs into passport.yandex.com as the user with TEMP_MASTER_PASSWORD (from --password or MIGRATION_TEMP_PASSWORD).
2. Completes one-time Yandex ID upgrade/Terms of Use/EULA screens if shown.
3. Opens https://id.yandex.com/security/app-passwords:
   - Enables "Use app passwords" ([aria-checked="false"]) if not yet enabled.
   - Clicks Mail (IMAP, POP3, SMTP), enters 'gw-migration', clicks Далее/Next/Create.
   - Extracts the 16-character App Password.
4. Verifies read-only IMAP login on imap.yandex.com:993 and saves app_password to accounts_state.csv.
"""

import argparse
import csv
import datetime
import fcntl
import imaplib
import os
import re
import socket
import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE_DIR = Path(os.environ.get("MIGRATION_BASE_DIR", Path(__file__).resolve().parent))
ACCOUNTS_CSV_PATH = BASE_DIR / "accounts_state.csv"
APP_PASSWORDS_CSV_PATH = BASE_DIR / "app_passwords.csv"
CHROME_EXEC = os.environ.get("CHROME_EXECUTABLE_PATH", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
DEFAULT_MASTER_PASSWORD = os.environ.get("MIGRATION_TEMP_PASSWORD", "ChangeMe!Mig2026")


def test_imap_login(user_email: str, password: str) -> bool:
    socket.setdefaulttimeout(30.0)
    for attempt in range(1, 5):
        try:
            time.sleep(1.5 * attempt)
            imap = imaplib.IMAP4_SSL("imap.yandex.com", 993)
            imap.login(user_email, password)
            imap.logout()
            return True
        except Exception as e:
            print(f"[IMAP check] {user_email} attempt {attempt}/4 returned: {e}")
            time.sleep(2.0 * attempt)
    return False


def save_to_app_passwords_csv(csv_path: Path, user_email: str, master_pw: str, app_pw: str, now_iso: str):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    found = False
    fieldnames = ["user_email", "master_password", "app_password", "updated_at"]
    if csv_path.exists():
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("user_email", "").strip().lower() == user_email.lower():
                    rows.append({
                        "user_email": user_email,
                        "master_password": master_pw,
                        "app_password": app_pw,
                        "updated_at": now_iso,
                    })
                    found = True
                else:
                    rows.append(r)
    if not found:
        rows.append({
            "user_email": user_email,
            "master_password": master_pw,
            "app_password": app_pw,
            "updated_at": now_iso,
        })
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def update_account_csv_password(
    user_email: str, working_pw: str, status: str, notes: str, master_pw: str = DEFAULT_MASTER_PASSWORD
):
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    lock_path = BASE_DIR / ".csv.lock"
    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            if working_pw:
                save_to_app_passwords_csv(APP_PASSWORDS_CSV_PATH, user_email, master_pw, working_pw, now_iso)

            if not ACCOUNTS_CSV_PATH.exists():
                return
            rows = []
            with open(ACCOUNTS_CSV_PATH, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for r in reader:
                    if r["user_email"].strip().lower() == user_email.lower():
                        if working_pw:
                            r["app_password"] = working_pw
                        r["status"] = status
                        r["updated_at"] = now_iso
                        r["notes"] = notes
                    rows.append(r)
            with open(ACCOUNTS_CSV_PATH, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)


def provision_user(user_email: str, master_password: str) -> str:
    print(f"=== Provisioning Yandex token for {user_email} ===")
    last_err = None
    for attempt in range(1, 4):
        app_password = ""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(executable_path=CHROME_EXEC, headless=True)
                ctx = browser.new_context(locale="en-US")
                page = ctx.new_page()

                page.goto("https://passport.yandex.com/auth", wait_until="domcontentloaded")
                page.locator('input[type="text"]').first.wait_for(state="visible", timeout=15000)
                time.sleep(1.0)
                page.locator('input[type="text"]').first.fill(user_email)
                time.sleep(0.3)
                page.locator('button:has-text("Next"), button[type="submit"]').first.click()

                page.locator('input[type="password"]').first.wait_for(state="visible", timeout=15000)
                time.sleep(0.5)
                page.locator('input[type="password"]').first.fill(master_password)
                page.locator('button:has-text("Continue"), button:has-text("Next"), button:has-text("Далее"), button[type="submit"]').first.click()
                time.sleep(3.5)

                # Complete any one-time Yandex ID onboarding/EULA screens ("Continue" -> "Ещё" -> "OK" -> "Not now")
                for _ in range(10):
                    try:
                        url_now = page.url
                        print(f"Post-login URL: {url_now}")
                        if "passport.yandex" in url_now and "upgrade" in url_now:
                            clicked = False
                            for b in page.locator("button").all():
                                t = (b.inner_text() or "").strip()
                                if t and t not in ("Show details", "Подробнее", ""):
                                    print(f"Clicking onboarding button: {t}")
                                    b.click()
                                    clicked = True
                                    time.sleep(2.5)
                                    break
                            if clicked:
                                continue
                        for label in ("Continue", "OK", "Not now", "Skip", "Далее"):
                            loc = page.locator(f'button:has-text("{label}")')
                            if loc.count() > 0 and loc.first.is_visible():
                                loc.first.click()
                                time.sleep(2.5)
                                break
                        else:
                            break
                    except Exception:
                        time.sleep(1.5)

                # Navigate to App Passwords page
                page.goto("https://id.yandex.com/security/app-passwords", wait_until="domcontentloaded")
                time.sleep(4.0)

                # Enable "Use app passwords" switch if currently off
                sw = page.locator('[aria-checked="false"]').first
                if sw.count() > 0:
                    print("Enabling app token switch...")
                    sw.click(force=True)
                    time.sleep(2.0)

                # If an app password already exists, click 'Создать пароль приложения' / 'Create' first
                imap_card = page.get_by_text("IMAP, POP3, SMTP")
                if imap_card.count() == 0 or not imap_card.first.is_visible():
                    create_new_btn = page.locator(
                        'button:has-text("Создать"), a:has-text("Создать"), button:has-text("Create"), a:has-text("Create"), [role="button"]:has-text("Создать"), [role="button"]:has-text("Create")'
                    )
                    if create_new_btn.count() > 0:
                        create_new_btn.first.click(force=True)
                        time.sleep(1.5)

                # Click Mail (IMAP, POP3, SMTP) card (.last targets the creation card even if a token already exists)
                page.get_by_text("IMAP, POP3, SMTP").last.click(force=True)
                time.sleep(1.8)

                # Fill 'gw-migration' as name and click Далее / Next / Create
                page.locator('input[type="text"]').last.fill(f"gw-migration-{attempt}")
                time.sleep(0.5)
                page.locator(
                    'button:has-text("Далее"), button:has-text("Next"), button:has-text("Create")'
                ).first.click()
                time.sleep(2.5)

                body_text = page.evaluate("() => document.body.innerText")
                lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]
                for ln in reversed(lines):
                    if re.fullmatch(r"[a-z0-9]{16}", ln):
                        app_password = ln
                        break

                if app_password:
                    # Quick single-shot check while browser session is still open;
                    # if IMAP is disabled on this mailbox, enable it in mail.yandex.com/#setup/client
                    imap_ok = False
                    try:
                        time.sleep(1.5)
                        imap = imaplib.IMAP4_SSL("imap.yandex.com", 993)
                        imap.login(user_email, app_password)
                        imap.logout()
                        imap_ok = True
                    except Exception as imap_err:
                        print(f"[IMAP quick-check] {user_email}: {imap_err} -> attempting to enable IMAP in Yandex Mail settings...")
                        try:
                            page.goto("https://mail.yandex.com/#/setup/client", wait_until="domcontentloaded", timeout=30000)
                            time.sleep(5.0)
                            for txt in ("Почтовые программы", "Email clients"):
                                loc = page.get_by_text(txt)
                                if loc.count() > 0:
                                    loc.first.click(force=True)
                                    time.sleep(3.0)
                                    break
                            imap_lbl = page.locator('label:has-text("imap.yandex")')
                            if imap_lbl.count() > 0:
                                imap_lbl.first.click(force=True)
                                time.sleep(1.5)
                            save_btn = page.locator(
                                'button:has-text("Сохранить"), button:has-text("Save")'
                            )
                            if save_btn.count() > 0:
                                save_btn.first.click(force=True)
                                print(f"[IMAP enable] {user_email}: clicked label:has-text('imap.yandex') and Save!")
                                time.sleep(4.0)
                        except Exception as mail_err:
                            print(f"[IMAP enable] {user_email} warning: {mail_err}")

                browser.close()

            if not app_password:
                raise RuntimeError(f"Could not extract 16-char token for {user_email}")

            print(f"Generated 16-char token for {user_email}: {app_password}")
            # Save immediately before IMAP test so it is never lost on transient socket drop
            update_account_csv_password(
                user_email,
                app_password,
                "PROVISIONED",
                "App Password generated",
                master_pw=master_password,
            )
            if imap_ok or test_imap_login(user_email, app_password):
                print(f"IMAP login verified for {user_email}!")
                return app_password
            return app_password
        except Exception as e:
            last_err = e
            print(f"[provision_user] Attempt {attempt}/3 error for {user_email}: {e}")
            time.sleep(3.0)

    raise RuntimeError(f"Provisioning failed for {user_email} after 3 attempts: {last_err}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", default=DEFAULT_MASTER_PASSWORD)
    args = parser.parse_args()
    provision_user(args.email.strip(), args.password.strip())


if __name__ == "__main__":
    main()
