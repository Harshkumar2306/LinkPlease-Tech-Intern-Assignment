"""The sender: drain pending DMs to the mock API, respecting the rate limit.

A single sender worker is enough because the API throttles us to ~9 sends per
minute anyway. Before each send the worker atomically moves the DM
``pending -> sending``; this reservation is what makes a concurrent
``comment.deleted`` (which only cancels ``pending`` rows) safe: once reserved,
the DM will be sent, and a DM still ``pending`` can no longer be sent out from
under the dispatcher. On a transient failure the DM returns to ``pending``
(with backoff); on startup ``recover_stale_sending`` resets any ``sending`` row
left behind by a crash.

Retry policy:

* 2xx (accepted)          -> store the returned dm_id, move to "accepted" for
                             reconciliation. The body's status is "queued"; it
                             is NOT delivered yet.
* 429 (rate limited)      -> back off for Retry-After (+ jitter), retry with the
                             SAME idempotency key.
* >=500 (internal error)  -> retry with exponential backoff, same key.
* 4xx (malformed/auth)    -> permanent; give up immediately.

Idempotency key
---------------
The key is "<dm_id>:<attempt>". Retries *before* acceptance reuse the same key,
so if a request actually succeeded on the server but the response was lost in a
500, the retry returns the original dm_id instead of sending a duplicate. A new
attempt (after a *confirmed* failed delivery) uses a new key, because reusing
the old key would just hand back the failed dm_id.
"""

import asyncio
import logging
import random
import time

from . import config
from . import db
from .api_client import PseudoGramClient
from .rate_limiter import RateLimiter

log = logging.getLogger("linkplease.sender")


def _retry_after(headers: dict, default: float = 5.0) -> float:
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return default
    try:
        return max(float(raw), default)
    except (TypeError, ValueError):
        return default


async def _is_comment_deleted(comment_id: str) -> bool:
    row = await db.fetchone(
        "SELECT is_deleted FROM comments WHERE comment_id = ?", (comment_id,)
    )
    return row is not None and bool(row["is_deleted"])


async def _cancel_deleted(dm: dict) -> None:
    """Cancel a DM whose comment was deleted before it was ever sent, and
    release its dedup claim so the user is not blocked from a future DM.

    Used when a DM becomes (or is found) 'pending' after its comment was already
    deleted — e.g. a re-queued retry whose comment got deleted meanwhile. The
    claim release + cancel are done atomically.
    """
    now = time.time()
    async with db.transaction() as conn:
        await conn.execute(
            "DELETE FROM dm_dedup WHERE rule_id = ? AND user_id = ?",
            (dm["rule_id"], dm["user_id"]),
        )
        await conn.execute(
            "UPDATE dms SET status = 'cancelled', updated_at = ? WHERE id = ?",
            (now, dm["id"]),
        )


async def _reserve(dm_id: str) -> bool:
    """Atomically claim a pending DM for sending (pending -> sending).

    Returns True if this call won the reservation. Returns False if the DM is no
    longer 'pending' (it was cancelled by a concurrent comment.deleted), in which
    case it must NOT be sent.
    """
    cur = await db.execute(
        "UPDATE dms SET status = 'sending', updated_at = ? "
        "WHERE id = ? AND status = 'pending'",
        (time.time(), dm_id),
    )
    return cur.rowcount == 1


async def _send_one(client: PseudoGramClient, dm: dict) -> None:
    idempotency_key = f"{dm['id']}:{dm['attempt']}"
    code, body, headers = await client.send_dm(
        dm["user_id"], dm["message"], dm["comment_id"], idempotency_key
    )
    now = time.time()

    if 200 <= code < 300:
        dm_id = body.get("dm_id")
        if not dm_id:
            # The API returned a success but no dm_id, so we have nothing to
            # reconcile and must NOT move to "accepted". Treat it as a transient
            # send failure: retry under the normal bounded policy with the SAME
            # idempotency key, so if the server actually did create a DM, the
            # retry returns the original dm_id instead of double-sending.
            retries = dm["send_retries"] + 1
            backoff = min(2 ** retries, 120)  # 2,4,8,... capped at 120s
            await db.execute(
                """
                UPDATE dms SET status = 'pending', send_retries = ?,
                    last_error = 'accepted_without_dm_id', next_action_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (retries, now + backoff, now, dm["id"]),
            )
            if retries >= config.MAX_SEND_RETRIES:
                await _give_up(dm["id"])
            return

        # Accepted with a real dm_id. The real delivery happens later;
        # reconciliation will confirm. Reset both retry counters for the new
        # delivery/reconciliation cycle.
        await db.execute(
            """
            UPDATE dms
            SET status = 'accepted', dm_id = ?, send_retries = 0,
                reconcile_retries = 0, last_error = NULL,
                next_action_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (dm_id, now + config.RECONCILE_INTERVAL, now, dm["id"]),
        )
        return

    if code == 429:
        backoff = _retry_after(headers) + random.uniform(0, 2)
        retries = dm["send_retries"] + 1
        await db.execute(
            """
            UPDATE dms SET status = 'pending', send_retries = ?,
                last_error = 'rate_limited', next_action_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (retries, now + backoff, now, dm["id"]),
        )
        if retries >= config.MAX_SEND_RETRIES:
            await _give_up(dm["id"])
        return

    if code == 0 or code >= 500:
        # Transient: transport error or server error. Safe to retry.
        retries = dm["send_retries"] + 1
        backoff = min(2 ** retries, 120)  # 2,4,8,... capped at 120s
        await db.execute(
            """
            UPDATE dms SET status = 'pending', send_retries = ?, last_error = ?,
                next_action_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (retries, f"http_{code}", now + backoff, now, dm["id"]),
        )
        if retries >= config.MAX_SEND_RETRIES:
            await _give_up(dm["id"])
        return

    # Any other 4xx is a permanent, unrecoverable error (bad payload, bad key).
    await db.execute(
        "UPDATE dms SET status = 'failed', last_error = ?, updated_at = ? WHERE id = ?",
        (str(body)[:200], now, dm["id"]),
    )


async def _give_up(dm_id: str) -> None:
    await db.execute(
        "UPDATE dms SET status = 'failed', updated_at = ? WHERE id = ?",
        (time.time(), dm_id),
    )


async def sender_loop(client: PseudoGramClient, limiter: RateLimiter) -> None:
    while True:
        try:
            dm = await db.fetchone(
                """
                SELECT * FROM dms
                WHERE status = 'pending' AND next_action_at <= ?
                ORDER BY created_at
                LIMIT 1
                """,
                (time.time(),),
            )
            if dm is None:
                await asyncio.sleep(0.2)
                continue

            # If the comment was deleted while this DM was still queued (e.g. a
            # re-queued retry, or a delete that raced the dispatcher), do not
            # send it. Cancel it and release the dedup claim.
            if await _is_comment_deleted(dm["comment_id"]):
                await _cancel_deleted(dm)
                continue

            # Block until the rate limiter grants a slot, then send.
            await limiter.acquire()

            # Atomically reserve the DM for sending. A concurrent comment.deleted
            # only cancels 'pending' rows, so winning this reservation means no
            # delete can cancel the DM out from under us before we send it.
            if not await _reserve(dm["id"]):
                continue

            await _send_one(client, dm)
        except Exception:  # noqa: BLE001 - keep the worker alive no matter what
            log.exception("sender error; will retry")
            await asyncio.sleep(0.5)
