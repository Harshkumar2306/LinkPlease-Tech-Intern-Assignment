"""The dispatcher: turn raw webhook events into queued DMs.

The webhook endpoint only *stores* events and returns 200 immediately. This
worker polls for unprocessed events and does the real matching work in the
background, so the webhook never blocks.

For every comment.created event we:

  1. Match its text (case-insensitive substring) against all rules.
  2. For each matching rule, atomically "claim" the (rule_id, user_id) pair via
     INSERT OR IGNORE on dm_dedup.
     - Claim succeeds  -> create a pending DM.
     - Claim fails     -> this is a duplicate (redelivered event, or the user
                          commented again); increment duplicates_blocked.

Because the claim is a database unique-constraint insert, the deduplication is
race-safe: two identical events arriving within the same instant can never both
claim the pair.
"""

import asyncio
import json
import logging
import time
import uuid

from . import config
from . import db

log = logging.getLogger("linkplease.matcher")


async def _load_rules(conn) -> list:
    cur = await conn.execute("SELECT rule_id, keyword, dm_message FROM rules")
    return await cur.fetchall()


async def _mark_processed(conn, event_pk: int) -> None:
    await conn.execute("UPDATE events SET processed = 1 WHERE id = ?", (event_pk,))


async def _handle_comment_created(conn, event_pk: int, data: dict) -> None:
    comment_id = data.get("comment_id")
    sender = data.get("from") or {}
    user_id = sender.get("user_id")
    username = sender.get("username")
    text = data.get("text") or ""
    created_at = data.get("created_at")

    # Without an identity we cannot DM anyone, so there is nothing to do.
    if not comment_id or not user_id:
        await _mark_processed(conn, event_pk)
        return

    # Out-of-order handling: if a comment.deleted arrived *before* the
    # comment.created for this comment, there is a tombstone and we must not DM.
    cur = await conn.execute(
        "SELECT is_deleted FROM comments WHERE comment_id = ?", (comment_id,)
    )
    row = await cur.fetchone()
    if row is not None and row["is_deleted"]:
        await _mark_processed(conn, event_pk)
        return

    # Record the comment (upsert, in case a deleted event already created a row).
    await conn.execute(
        """
        INSERT INTO comments (comment_id, user_id, username, text, created_at, is_deleted)
        VALUES (?, ?, ?, ?, ?, 0)
        ON CONFLICT(comment_id) DO UPDATE SET
            user_id = excluded.user_id,
            username = excluded.username,
            text = excluded.text,
            created_at = excluded.created_at
        """,
        (comment_id, user_id, username, text, created_at),
    )

    rules = await _load_rules(conn)
    lowered = text.lower()

    for rule in rules:
        keyword = rule["keyword"]
        if not keyword or keyword.lower() not in lowered:
            continue

        now = time.time()
        dm_id = uuid.uuid4().hex

        # Atomically claim (rule_id, user_id). rowcount == 0 means it already
        # exists, i.e. we must NOT send again.
        cur = await conn.execute(
            """
            INSERT OR IGNORE INTO dm_dedup (rule_id, user_id, dm_id, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (rule["rule_id"], user_id, dm_id, now),
        )

        if cur.rowcount == 1:
            await conn.execute(
                """
                INSERT INTO dms (
                    id, rule_id, user_id, username, comment_id, message,
                    status, attempt, dm_id, send_retries, next_action_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 1, NULL, 0, ?, ?, ?)
                """,
                (dm_id, rule["rule_id"], user_id, username, comment_id,
                 rule["dm_message"], now, now, now),
            )
        else:
            await conn.execute(
                "UPDATE counters SET value = value + 1 WHERE name = 'duplicates_blocked'"
            )

    await _mark_processed(conn, event_pk)


async def _handle_comment_deleted(conn, event_pk: int, data: dict) -> None:
    comment_id = data.get("comment_id")
    if not comment_id:
        await _mark_processed(conn, event_pk)
        return

    now = time.time()
    # Upsert a tombstone. If the comment.created arrives later, it will see
    # is_deleted = 1 and skip sending.
    await conn.execute(
        """
        INSERT INTO comments (comment_id, user_id, username, text, created_at, is_deleted)
        VALUES (?, NULL, NULL, NULL, NULL, 1)
        ON CONFLICT(comment_id) DO UPDATE SET is_deleted = 1
        """,
        (comment_id,),
    )

    # If a DM for this comment is still waiting to be sent, cancel it AND release
    # its dedup claim in the same transaction. The claim means "we owe this
    # (rule, user) a DM"; once we cancel without ever sending, keeping the claim
    # would permanently block that user from being DMed for that rule again.
    # DMs already accepted by the API cannot be un-sent, so they (and their
    # claims) are deliberately left alone.
    cur = await conn.execute(
        "SELECT id, rule_id, user_id FROM dms WHERE comment_id = ? AND status = 'pending'",
        (comment_id,),
    )
    pending = await cur.fetchall()
    for dm in pending:
        await conn.execute(
            "DELETE FROM dm_dedup WHERE rule_id = ? AND user_id = ?",
            (dm["rule_id"], dm["user_id"]),
        )
        await conn.execute(
            "UPDATE dms SET status = 'cancelled', updated_at = ? WHERE id = ?",
            (now, dm["id"]),
        )

    await _mark_processed(conn, event_pk)


async def _process_event(conn, event: dict) -> None:
    try:
        payload = json.loads(event["payload"])
    except (ValueError, TypeError):
        await _mark_processed(conn, event["id"])
        return

    event_type = payload.get("event_type")
    data = payload.get("data") or {}

    if event_type == "comment.created":
        await _handle_comment_created(conn, event["id"], data)
    elif event_type == "comment.deleted":
        await _handle_comment_deleted(conn, event["id"], data)
    else:
        # Unknown event types are safely ignored.
        await _mark_processed(conn, event["id"])


async def process_batch(limit: int = 100) -> int:
    """Process up to `limit` unprocessed events. Returns how many were handled."""
    rows = await db.fetchall(
        "SELECT id, event_id, event_type, payload FROM events "
        "WHERE processed = 0 ORDER BY id LIMIT ?",
        (limit,),
    )
    if not rows:
        return 0

    for row in rows:
        # One transaction per event: matching + claims + processed-flag are
        # atomic, so a crash mid-event rolls back and the event is retried.
        async with db.transaction() as conn:
            await _process_event(conn, row)
    return len(rows)


async def dispatcher_loop() -> None:
    while True:
        try:
            handled = await process_batch()
            if handled == 0:
                await asyncio.sleep(config.DISPATCH_INTERVAL)
        except Exception:  # noqa: BLE001 - a bad event must never kill the worker
            log.exception("dispatcher error; will retry")
            await asyncio.sleep(0.5)
