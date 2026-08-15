"""Regression tests for the three confirmed defects.

These run against an isolated SQLite database and do NOT hit the live
Pseudogram API, so they are deterministic and can run offline / in CI:

  * Bug 1 — a cancelled (comment.deleted-before-send) DM must release its
            dm_dedup claim so the same user can be DMed on a later comment.
  * Bug 2 — a reconciliation read that never returns delivered/failed is
            retried a bounded number of times and then marked failed.
  * Bug 3 — a 2xx send response without a dm_id is not accepted; it is retried
            under the normal bounded policy and eventually failed.

Usage:
    python scripts/test_regressions.py
"""

import asyncio
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config, db, matcher, reconciler, sender  # noqa: E402


def _comment_created(event_id, comment_id, user_id, text):
    return {
        "event_id": event_id,
        "event_type": "comment.created",
        "sent_at": "2026-08-15T00:00:00Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_x",
            "text": text,
            "created_at": "2026-08-15T00:00:00Z",
            "from": {"user_id": user_id, "username": "u_" + user_id},
        },
    }


def _comment_deleted(event_id, comment_id):
    return {
        "event_id": event_id,
        "event_type": "comment.deleted",
        "sent_at": "2026-08-15T00:00:00Z",
        "data": {"comment_id": comment_id},
    }


async def _insert_event(event_id, event_type, payload):
    await db.execute(
        "INSERT INTO events (event_id, event_type, payload, received_at, processed) "
        "VALUES (?, ?, ?, ?, 0)",
        (event_id, event_type, json.dumps(payload), time.time()),
    )


async def _drain_events():
    while await matcher.process_batch(limit=1000):
        pass


async def _insert_rule(rule_id="r1", keyword="PRICE", message="prices"):
    await db.execute(
        "INSERT INTO rules (rule_id, keyword, dm_message, created_at) VALUES (?, ?, ?, ?)",
        (rule_id, keyword, message, time.time()),
    )


async def _insert_dm(dm_id, status, *, dm_id_api=None, attempt=1, send_retries=0,
                     reconcile_retries=0, comment_id="c", user_id="u"):
    now = time.time()
    await db.execute(
        """
        INSERT INTO dms (id, rule_id, user_id, username, comment_id, message, status,
                         attempt, dm_id, send_retries, reconcile_retries,
                         next_action_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (dm_id, "r1", user_id, "u_" + user_id, comment_id, "msg", status,
         attempt, dm_id_api, send_retries, reconcile_retries,
         now, now, now),
    )


async def test_bug1_cancelled_dm_releases_dedup():
    await _insert_rule()

    # 1) comment.created -> claim + one pending DM.
    await _insert_event("e1", "comment.created",
                        _comment_created("e1", "c1", "u1", "PRICE please"))
    await _drain_events()
    dms = await db.fetchall("SELECT * FROM dms")
    dedup = await db.fetchall("SELECT * FROM dm_dedup")
    assert len(dms) == 1 and dms[0]["status"] == "pending", dms
    assert len(dedup) == 1, dedup

    # 2) comment.deleted before the DM is sent -> cancel + release the claim.
    await _insert_event("e2", "comment.deleted", _comment_deleted("e2", "c1"))
    await _drain_events()
    dms = await db.fetchall("SELECT * FROM dms ORDER BY created_at")
    dedup = await db.fetchall("SELECT * FROM dm_dedup")
    assert dms[0]["status"] == "cancelled", dms
    assert len(dedup) == 0, f"dedup claim must be released, got {len(dedup)} rows"

    # 3) same user comments again -> must NOT be blocked forever.
    await _insert_event("e3", "comment.created",
                        _comment_created("e3", "c2", "u1", "PRICE again"))
    await _drain_events()
    dms = await db.fetchall("SELECT * FROM dms ORDER BY created_at")
    dedup = await db.fetchall("SELECT * FROM dm_dedup")
    pending = [d for d in dms if d["status"] == "pending"]
    assert len(dms) == 2, dms
    assert len(pending) == 1, f"expected a fresh pending DM, got {dms}"
    assert len(dedup) == 1, f"expected a fresh claim, got {dedup}"

    dup = await db.fetchone("SELECT value FROM counters WHERE name='duplicates_blocked'")
    assert dup["value"] == 0, f"nothing should be blocked as duplicate, got {dup['value']}"
    print("  Bug 1 OK: a cancelled DM releases its dedup claim")


async def test_bug2_reconcile_bounded_retry():
    config.MAX_RECONCILE_RETRIES = 3

    # (a) 200 with a malformed/unrecognized body (no "status") -> bounded retry.
    await _insert_dm("dm1", "accepted", dm_id_api="api_dm1")
    for i in range(config.MAX_RECONCILE_RETRIES - 1):
        dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm1'")
        await reconciler._apply([dm], [(200, {})])
        dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm1'")
        assert dm["status"] == "accepted", dm
        assert dm["reconcile_retries"] == i + 1, dm
    # The final read trips the bound and the DM is failed.
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm1'")
    await reconciler._apply([dm], [(200, {})])
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm1'")
    assert dm["status"] == "failed", dm
    assert dm["last_error"] == "reconcile_failed_after_retries", dm

    # (b) non-200 (e.g. 500) -> bounded retry.
    await _insert_dm("dm2", "accepted", dm_id_api="api_dm2")
    for _ in range(config.MAX_RECONCILE_RETRIES):
        dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm2'")
        await reconciler._apply([dm], [(500, {"error": "internal_error"})])
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm2'")
    assert dm["status"] == "failed", dm

    # (c) "queued" is a normal transient state: it must NOT consume a retry.
    await _insert_dm("dm3", "accepted", dm_id_api="api_dm3")
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm3'")
    await reconciler._apply([dm], [(200, {"status": "queued"})])
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm3'")
    assert dm["status"] == "accepted" and dm["reconcile_retries"] == 0, dm

    # (d) delivered still works.
    await _insert_dm("dm4", "accepted", dm_id_api="api_dm4")
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm4'")
    await reconciler._apply([dm], [(200, {"status": "delivered"})])
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm4'")
    assert dm["status"] == "delivered", dm

    print("  Bug 2 OK: reconciliation reads are bounded; an accepted DM can no "
          "longer be stranded forever")


class _FakeClient:
    def __init__(self, code, body, headers=None):
        self.code = code
        self.body = body
        self.headers = headers or {}

    async def send_dm(self, recipient_user_id, message, comment_id, idempotency_key):
        return self.code, self.body, self.headers


async def test_bug3_missing_dm_id():
    config.MAX_SEND_RETRIES = 3
    await _insert_dm("dm5", "pending")

    # 2xx but no dm_id -> must NOT be accepted; retried under bounded policy.
    client = _FakeClient(200, {"status": "queued"})
    for i in range(config.MAX_SEND_RETRIES - 1):
        dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm5'")
        await sender._send_one(client, dm)
        dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm5'")
        assert dm["status"] == "pending", dm
        assert dm["send_retries"] == i + 1, dm
        assert dm["last_error"] == "accepted_without_dm_id", dm

    # The final retry trips the bound and the DM is failed.
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm5'")
    await sender._send_one(client, dm)
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm5'")
    assert dm["status"] == "failed", dm
    assert dm["send_retries"] == config.MAX_SEND_RETRIES, dm

    # Sanity: a real 2xx with a dm_id is still accepted normally.
    await _insert_dm("dm6", "pending")
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm6'")
    await sender._send_one(_FakeClient(202, {"dm_id": "api_dm6", "status": "queued"}), dm)
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm6'")
    assert dm["status"] == "accepted" and dm["dm_id"] == "api_dm6", dm
    assert dm["reconcile_retries"] == 0, dm

    print("  Bug 3 OK: a 2xx without dm_id is never accepted; normal accept still works")


async def test_bug4_sender_reserve_and_cancel():
    # (a) a pending DM can be reserved (pending -> sending).
    await _insert_dm("dm7", "pending")
    assert await sender._reserve("dm7") is True
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm7'")
    assert dm["status"] == "sending", dm

    # (b) a cancelled DM cannot be reserved again.
    await _insert_dm("dm8", "cancelled")
    assert await sender._reserve("dm8") is False
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm8'")
    assert dm["status"] == "cancelled", dm

    # (c) a pending DM whose comment is deleted gets cancelled + claim released.
    await db.execute(
        "INSERT INTO comments (comment_id, user_id, username, text, created_at, is_deleted) "
        "VALUES ('c9', 'u9', 'u9', 'x', 0.0, 1)"
    )
    await db.execute(
        "INSERT INTO dm_dedup (rule_id, user_id, dm_id, created_at) VALUES ('r1', 'u9', 'dm9', 0.0)"
    )
    await _insert_dm("dm9", "pending", comment_id="c9", user_id="u9")
    assert await sender._is_comment_deleted("c9") is True
    await sender._cancel_deleted(
        {"id": "dm9", "rule_id": "r1", "user_id": "u9", "comment_id": "c9"}
    )
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm9'")
    assert dm["status"] == "cancelled", dm
    dedup = await db.fetchall("SELECT * FROM dm_dedup WHERE rule_id = 'r1' AND user_id = 'u9'")
    assert len(dedup) == 0, f"claim must be released, got {dedup}"

    # (d) startup recovery resets a stale 'sending' DM back to 'pending'.
    await _insert_dm("dm10", "sending")
    await db.recover_stale_sending()
    dm = await db.fetchone("SELECT * FROM dms WHERE id = 'dm10'")
    assert dm["status"] == "pending", dm

    # (e) /stats counts 'sending' as queued (never as sent).
    from app import stats as stats_mod
    s = await stats_mod.compute()
    assert s["queued"] >= 1, s

    print("  Bug 4 OK: sender reservation closes the cancel/send race; stale "
          "'sending' DMs recover on startup")


async def main():
    db_path = os.path.join(tempfile.mkdtemp(), "linkplease.db")
    await db.init(db_path)
    try:
        await test_bug1_cancelled_dm_releases_dedup()
        await test_bug2_reconcile_bounded_retry()
        await test_bug3_missing_dm_id()
        await test_bug4_sender_reserve_and_cancel()
        print("\nALL REGRESSION TESTS PASSED")
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
