#!/usr/bin/env python3
"""mail.tm poller for the temp-mail site.

Runs on GitHub Actions (whose egress REACHES mail.tm — Cloudflare Worker
egress is blocked by mail.tm with HTTP 403, so the Worker never calls
mail.tm directly). Every run (schedule: every 5 min + manual dispatch):
  1. Fetches mail.tm's live domains (they rotate) and pushes them to the
     Worker, so new addresses are created on a real domain.
  2. Provisions pending addresses: creates the mail.tm account with the
     password the Worker stored, logs in, and pushes the credentials
     (account_id + token) back to the Worker.
  3. Syncs new messages for every provisioned account into the Worker,
     which archives them in D1 permanently (mail.tm deletes messages
     after 7 days; the address itself stays valid until deleted).

Env: WORKER_URL (e.g. https://temp-mail.msma8399.workers.dev),
     SYNC_SECRET (shared secret, also set as a Worker secret).
Exit 0 even if individual addresses fail — one bad address must not
fail the whole run. A non-zero exit means the Worker itself is down.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

MAILTM = "https://api.mail.tm"
WORKER = os.environ["WORKER_URL"].rstrip("/")
SECRET = os.environ["SYNC_SECRET"]
UA = "temp-mail-poller/1.0"


def log(msg):
    print(msg, flush=True)


def http(method, url, payload=None, headers=None, timeout=30):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Accept": "application/json",
        "User-Agent": UA,
        "Content-Type": "application/json",
        **(headers or {}),
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, {"raw": raw[:300]}
    except Exception as e:  # network error
        return 0, {"error": str(e)}


def http_retry(method, url, payload=None, headers=None, attempts=4):
    """Retry 403/429/5xx/network errors with backoff (mail.tm's /token
    intermittently 403s for valid credentials). Returns (status, body)."""
    last = (0, {"error": "no attempt"})
    for i in range(attempts):
        status, body = http(method, url, payload, headers)
        last = (status, body)
        if status == 200 or status == 201:
            return status, body
        if status in (403, 429, 500, 502, 503, 0):
            if i < attempts - 1:
                time.sleep(1.5 * (i + 1))
                continue
        return status, body
    return last


def wapi(method, path, payload=None):
    return http(method, WORKER + path, payload,
                headers={"x-sync-secret": SECRET})


def mailtm_token(address, password):
    status, body = http_retry("POST", MAILTM + "/token",
                              {"address": address, "password": password})
    if status == 200 and body.get("token"):
        return body["token"]
    return None


def main():
    summary = {"domains": [], "provisioned": [], "provision_failed": [],
               "messages_synced": 0, "accounts_synced": 0, "errors": []}

    # 1. Live domains -> Worker
    status, domains_raw = http_retry("GET", MAILTM + "/domains?page=1", attempts=3)
    domains = []
    if status == 200:
        # /domains returns a plain JSON array (not a Hydra collection).
        members = domains_raw if isinstance(domains_raw, list) else (domains_raw.get("hydra:member") or [])
        domains = [d["domain"] for d in members
                   if d.get("domain") and d.get("isActive", True)]
    if domains:
        s, _ = wapi("POST", "/api/sync/domains", {"domains": domains})
        if s == 200:
            summary["domains"] = domains
            log(f"domains pushed: {domains}")
        else:
            summary["errors"].append(f"push domains -> HTTP {s}")
            log(f"ERROR: push domains -> HTTP {s}")
    else:
        summary["errors"].append("mail.tm /domains failed")
        log("ERROR: could not fetch mail.tm domains")

    # 2. Provision pending addresses
    s, pending = wapi("GET", "/api/sync/pending")
    if s != 200:
        log(f"ERROR: GET /sync/pending -> HTTP {s}")
        print(json.dumps(summary))
        return 1
    for row in pending:
        address = row["address"]
        password = row["password"]
        status, acc = http_retry("POST", MAILTM + "/accounts",
                                 {"address": address, "password": password})
        if status == 422:
            # Address already exists on mail.tm — maybe ours (retry with
            # our password); if login fails it belongs to someone else.
            token = mailtm_token(address, password)
            if token:
                status_me, me = http_retry(
                    "GET", MAILTM + "/me",
                    headers={"Authorization": "Bearer " + token}, attempts=2)
                account_id = me.get("id", "") if status_me == 200 else ""
                s2, _ = wapi("POST", "/api/sync/credentials",
                             {"address": address,
                              "account_id": account_id,
                              "token": token})
                if s2 == 200:
                    summary["provisioned"].append(address)
                    log(f"provisioned (recovered): {address}")
                    continue
            wapi("POST", "/api/sync/drop-pending", {"address": address})
            summary["provision_failed"].append({"address": address, "reason": "taken on mail.tm"})
            log(f"FAILED provision (taken): {address}")
            continue
        if status != 201:
            summary["provision_failed"].append({"address": address, "reason": f"account create HTTP {status}"})
            log(f"FAILED provision {address}: account create HTTP {status}")
            continue
        token = mailtm_token(address, password)
        if not token:
            summary["provision_failed"].append({"address": address, "reason": "token login failed"})
            log(f"FAILED provision {address}: token login failed")
            continue
        s2, _ = wapi("POST", "/api/sync/credentials",
                     {"address": address, "account_id": acc.get("id", ""), "token": token})
        if s2 == 200:
            summary["provisioned"].append(address)
            log(f"provisioned: {address}")
        else:
            summary["provision_failed"].append({"address": address, "reason": f"push credentials HTTP {s2}"})
            log(f"FAILED provision {address}: push credentials HTTP {s2}")

    # 3. Sync messages for provisioned accounts
    s, accounts = wapi("GET", "/api/sync/accounts")
    if s != 200:
        log(f"ERROR: GET /sync/accounts -> HTTP {s}")
        print(json.dumps(summary))
        return 1
    for row in accounts:
        address = row["address"]
        password = row["password"]
        token = mailtm_token(address, password)
        if not token:
            summary["errors"].append(f"{address}: token login failed")
            log(f"ERROR sync {address}: token login failed")
            continue
        headers = {"Authorization": "Bearer " + token}
        new_ids = []
        for page in (1, 2, 3):
            status, data = http_retry("GET", f"{MAILTM}/messages?page={page}", headers=headers, attempts=2)
            if status != 200:
                break
            members = data if isinstance(data, list) else (data.get("hydra:member") or [])
            if not members:
                break
            new_ids.extend(m["id"] for m in members if m.get("id"))
            if len(members) < 30:
                break
        if not new_ids:
            summary["accounts_synced"] += 1
            continue
        s2, known = wapi("POST", "/api/sync/known", {"address": address, "ids": new_ids})
        known_set = set(known.get("known", [])) if s2 == 200 else set()
        fresh = [i for i in new_ids if i not in known_set]
        msgs = []
        for mid in fresh:
            status, detail = http_retry("GET", f"{MAILTM}/messages/{mid}", headers=headers, attempts=2)
            if status != 200:
                continue
            html = detail.get("html")
            msgs.append({
                "id": mid,
                "from": detail.get("from") or "",
                "subject": detail.get("subject") or "(no subject)",
                "text": detail.get("text") or "",
                "html": html if isinstance(html, list) else ([html] if html else []),
                "createdAt": detail.get("createdAt") or "",
            })
        if msgs:
            s3, res = wapi("POST", "/api/sync/messages", {"address": address, "messages": msgs})
            if s3 == 200:
                summary["messages_synced"] += res.get("inserted", 0)
                log(f"synced {res.get('inserted', 0)} new message(s) for {address}")
            else:
                summary["errors"].append(f"{address}: push messages HTTP {s3}")
                log(f"ERROR sync {address}: push messages HTTP {s3}")
        summary["accounts_synced"] += 1

    log("SUMMARY: " + json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
