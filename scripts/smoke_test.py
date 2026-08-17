"""End-to-end smoke test for LinkPlease against the real Pseudogram API.

This script:
  1. starts the FastAPI app as a subprocess,
  2. creates a rule,
  3. posts signed comment events (with a repeat comment, a redelivery, a
     non-match, and one forged-signature request),
  4. waits for the workers to finish, and
  5. verifies /stats AND independently checks dm_ids against the API.

Usage:
    PSEUDOGRAM_API_KEY=<key> python scripts/smoke_test.py
"""

import hmac
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time

import httpx

BASE = "https://pseudogram-api.onrender.com"
KEY = os.environ.get("PSEUDOGRAM_API_KEY", "")
PORT = int(os.environ.get("SMOKE_PORT", "8123"))
APP_URL = f"http://127.0.0.1:{PORT}"

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def sign(body: bytes) -> str:
    return "sha256=" + hmac.new(KEY.encode(), body, hashlib.sha256).hexdigest()


def comment_event(event_id: str, comment_id: str, user_id: str, username: str, text: str) -> dict:
    return {
        "event_id": event_id,
        "event_type": "comment.created",
        "sent_at": "2026-08-15T09:14:22.481Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_44de1b",
            "text": text,
            "created_at": "2026-08-15T09:14:21.900Z",
            "from": {"user_id": user_id, "username": username},
        },
    }


def post_webhook(client: httpx.Client, payload: dict, forge: bool = False):
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    headers["X-PseudoGram-Signature"] = ("bad" + sign(body)) if forge else sign(body)
    return client.post(f"{APP_URL}/webhook", content=body, headers=headers)


def main() -> int:
    if not KEY:
        print("ERROR: set PSEUDOGRAM_API_KEY")
        return 2

    db_path = os.path.join(tempfile.mkdtemp(), "linkplease.db")

    env = dict(os.environ)
    env.update({
        "PSEUDOGRAM_API_KEY": KEY,
        "DATABASE_PATH": db_path,
        "VERIFY_SIGNATURES": "true",
        "RATE_LIMIT_PER_MINUTE": "9",
        "RECONCILE_INTERVAL": "2.0",
        "DISPATCH_INTERVAL": "0.05",
    })

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(PORT), "--workers", "1"],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    client = httpx.Client(base_url=APP_URL, timeout=30)

    try:
        # Wait for readiness.
        for _ in range(100):
            try:
                if client.get("/").status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        else:
            raise RuntimeError("server did not start")

        # Create a rule.
        r = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Here's the price list"})
        assert r.status_code == 201, r.text
        print("rule:", r.json())

        # Fire events.
        events = [
            comment_event("evt_a", "cmt_a", "usr_a", "alice", "PRICE please"),
            comment_event("evt_b", "cmt_b", "usr_b", "bob", "price"),
            comment_event("evt_c", "cmt_c", "usr_c", "carol", "I want the PRICE list"),
            comment_event("evt_a2", "cmt_a2", "usr_a", "alice", "PRICE again!"),  # same user -> dup
            comment_event("evt_a", "cmt_a", "usr_a", "alice", "PRICE please"),   # redelivery -> dup
            comment_event("evt_d", "cmt_d", "usr_d", "dave", "hello world"),      # no match
        ]
        for ev in events:
            resp = post_webhook(client, ev)
            assert resp.status_code == 200, resp.text
        print("posted", len(events), "events (all 200)")

        # Forged signature must be rejected.
        forged = post_webhook(client, events[0], forge=True)
        assert forged.status_code == 401, forged.text
        print("forged signature rejected with", forged.status_code)

        # Wait for stats to settle (queued -> 0).
        stats = None
        for _ in range(200):
            stats = client.get("/stats").json()
            if stats["queued"] == 0 and stats["sent"] >= 3:
                break
            time.sleep(0.5)
        print("final stats:", stats)

        assert stats["sent"] == 3, f"expected 3 sent, got {stats}"
        assert stats["failed"] == 0, f"expected 0 failed, got {stats}"
        assert stats["queued"] == 0, f"expected 0 queued, got {stats}"
        assert stats["duplicates_blocked"] == 2, f"expected 2 blocked, got {stats}"

        # Independent check: read dm_ids from the DB and confirm delivered.
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT id, dm_id, status FROM dms").fetchall()
        con.close()
        print(f"dms in db: {len(rows)}")
        for row in rows:
            if row["status"] == "delivered":
                r = client.get(f"https://pseudogram-api.onrender.com/v1/dm/{row['dm_id']}",
                               headers={"X-API-Key": KEY})
                body = r.json()
                assert body.get("status") == "delivered", body
                print(f"  confirmed delivered: {row['dm_id']}")
        delivered = [r for r in rows if r["status"] == "delivered"]
        assert len(delivered) == 3, rows

        print("\nSMOKE TEST PASSED")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        client.close()


if __name__ == "__main__":
    sys.exit(main())
