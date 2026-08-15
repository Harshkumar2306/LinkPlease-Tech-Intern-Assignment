"""Load test: fire ~500 comment events in ~10s and verify the invariants.

Checks, in order:
  1. every webhook returns 200 (no events dropped at the door),
  2. the dispatcher processes every event,
  3. the dedup produces the expected number of DMs and duplicates_blocked,
  4. the rate limiter never exceeds 9 sends per rolling 60s (no 429s).

Usage:
    PSEUDOGRAM_API_KEY=<key> python scripts/load_test.py
"""

import asyncio
import hashlib
import hmac
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time

import httpx

KEY = os.environ.get("PSEUDOGRAM_API_KEY", "")
PORT = int(os.environ.get("LOAD_PORT", "8125"))
APP_URL = f"http://127.0.0.1:{PORT}"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def sign(body: bytes) -> str:
    return "sha256=" + hmac.new(KEY.encode(), body, hashlib.sha256).hexdigest()


def comment_event(i: int, user_id: str, text: str) -> dict:
    return {
        "event_id": f"evt_{i}",
        "event_type": "comment.created",
        "sent_at": "2026-08-15T09:14:22.481Z",
        "data": {
            "comment_id": f"cmt_{i}",
            "post_id": "post_44de1b",
            "text": text,
            "created_at": "2026-08-15T09:14:21.900Z",
            "from": {"user_id": user_id, "username": f"user_{user_id}"},
        },
    }


async def fire(client: httpx.AsyncClient, events: list) -> list:
    sem = asyncio.Semaphore(60)

    async def one(ev: dict):
        async with sem:
            body = json.dumps(ev).encode()
            headers = {
                "Content-Type": "application/json",
                "X-PseudoGram-Signature": sign(body),
            }
            return await client.post("/webhook", content=body, headers=headers)

    return await asyncio.gather(*(one(ev) for ev in events))


def build_events() -> list:
    events = []
    # 400 unique users matching PRICE
    for i in range(400):
        events.append(comment_event(i, f"u{i}", "PRICE please"))
    # 80 unique users matching SALE
    for i in range(80):
        events.append(comment_event(400 + i, f"u{400 + i}", "SALE now"))
    # 20 users already in the PRICE group comment again -> duplicates
    for i in range(20):
        events.append(comment_event(500 + i, f"u{i}", "PRICE again"))
    # 20 redeliveries of the first 20 PRICE events -> duplicates
    for i in range(20):
        events.append(comment_event(i, f"u{i}", "PRICE please"))
    return events  # 520 events total


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
        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    async def run():
        async with httpx.AsyncClient(base_url=APP_URL, timeout=60) as client:
            for _ in range(100):
                try:
                    if (await client.get("/")).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)

            # Rules
            await client.post("/rules", json={"keyword": "PRICE", "dm_message": "prices"})
            await client.post("/rules", json={"keyword": "SALE", "dm_message": "sales"})

            events = build_events()
            t0 = time.time()
            responses = await fire(client, events)
            elapsed = time.time() - t0

            codes = {}
            for r in responses:
                codes[r.status_code] = codes.get(r.status_code, 0) + 1
            print(f"fired {len(events)} events in {elapsed:.1f}s; status codes: {codes}")
            assert codes.get(200, 0) == len(events), codes

            # Wait for the dispatcher to drain everything.
            for _ in range(100):
                stats = (await client.get("/stats")).json()
                con = sqlite3.connect(db_path)
                pending = con.execute("SELECT COUNT(*) FROM events WHERE processed=0").fetchone()[0]
                con.close()
                if pending == 0:
                    break
                await asyncio.sleep(0.2)

            con = sqlite3.connect(db_path)
            con.row_factory = sqlite3.Row
            total_events = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            total_dms = con.execute("SELECT COUNT(*) FROM dms").fetchone()[0]
            dup = con.execute("SELECT value FROM counters WHERE name='duplicates_blocked'").fetchone()[0]
            con.close()

            print(f"events stored={total_events} (expected 520)")
            print(f"dms created={total_dms} (expected 480)")
            print(f"duplicates_blocked={dup} (expected 40)")
            assert total_events == 520
            assert total_dms == 480
            assert dup == 40

            # Now watch the rate limiter for ~90s: no window may exceed 9 sends,
            # and no DM may have hit a 429.
            print("observing rate limiter for 90s...")
            windows = []
            start = time.time()
            while time.time() - start < 90:
                con = sqlite3.connect(db_path)
                rows = con.execute(
                    "SELECT sent_at FROM send_log ORDER BY sent_at"
                ).fetchall()
                con.close()
                windows.append(len(rows))
                await asyncio.sleep(2.0)

            # Check the actual send_log spacing: max sends in any 60s window.
            con = sqlite3.connect(db_path)
            times = [r[0] for r in con.execute(
                "SELECT sent_at FROM send_log ORDER BY sent_at").fetchall()]
            rate_limited = con.execute(
                "SELECT COUNT(*) FROM dms WHERE last_error='rate_limited'").fetchone()[0]
            con.close()

            max_in_window = 0
            j = 0
            for i in range(len(times)):
                while times[i] - times[j] > 60:
                    j += 1
                max_in_window = max(max_in_window, i - j + 1)

            print(f"send_log entries so far: {len(times)}")
            print(f"max sends in any rolling 60s window: {max_in_window} (must be <= 9)")
            print(f"DMs that hit a 429: {rate_limited} (must be 0)")
            assert max_in_window <= 9
            assert rate_limited == 0

            final_stats = await client.get("/stats")
            print("stats after 90s:", final_stats.json())
            print("\nLOAD TEST PASSED")

    try:
        asyncio.run(run())
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
