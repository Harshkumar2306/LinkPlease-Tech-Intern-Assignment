"""The reconciler: confirm delivery of accepted DMs (Part C).

A 2xx from /v1/dm/send only means "accepted", not "delivered". Roughly 15% of
accepted DMs later end up failed. We poll GET /v1/dm/{id} (which does NOT count
against the rate limit) and:

* delivered -> mark terminal success.
* failed    -> increment the attempt counter and re-queue for a fresh send
               (with a NEW idempotency key), up to MAX_ATTEMPTS.
* queued    -> keep waiting; check again in RECONCILE_INTERVAL seconds.
* non-200 / malformed / unrecognized -> bounded read retry; after
               MAX_RECONCILE_RETRIES we give up and mark the DM "failed" so it
               can never sit in "accepted" (and skew /stats) forever.

Because a confirmed failure is re-queued rather than dropped, a DM the API
"accepted but later failed" is never silently lost.
"""

import asyncio
import logging
import time

from . import config
from . import db
from .api_client import PseudoGramClient

log = logging.getLogger("linkplease.reconciler")


async def _due_batch(limit: int = 200) -> list:
    return await db.fetchall(
        """
        SELECT * FROM dms
        WHERE status = 'accepted' AND next_action_at <= ?
        ORDER BY next_action_at
        LIMIT ?
        """,
        (time.time(), limit),
    )


async def _reconcile_retry_or_fail(conn, dm, now) -> None:
    """Bounded retry for a reconciliation read that never reaches a terminal
    state (non-200, or a 200 whose body is not a recognized status). After
    MAX_RECONCILE_RETRIES such reads we give up and mark the DM failed, so it
    can never remain 'accepted' forever.

    This counter is separate from send_retries/attempt: it only counts *read*
    failures, not delivery attempts.
    """
    retries = dm["reconcile_retries"] + 1
    if retries >= config.MAX_RECONCILE_RETRIES:
        await conn.execute(
            """
            UPDATE dms SET status = 'failed',
                last_error = 'reconcile_failed_after_retries',
                reconcile_retries = ?, updated_at = ?
            WHERE id = ?
            """,
            (retries, now, dm["id"]),
        )
    else:
        await conn.execute(
            """
            UPDATE dms SET reconcile_retries = ?, next_action_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (retries, now + config.RECONCILE_INTERVAL, now, dm["id"]),
        )


async def _apply(batch: list, results: list) -> None:
    now = time.time()
    async with db.transaction() as conn:
        for dm, (code, body) in zip(batch, results):
            if code == 200:
                status = body.get("status")
                if status == "delivered":
                    await conn.execute(
                        """
                        UPDATE dms SET status = 'delivered', reconcile_retries = 0,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (now, dm["id"]),
                    )
                elif status == "failed":
                    if dm["attempt"] >= config.MAX_ATTEMPTS:
                        await conn.execute(
                            """
                            UPDATE dms SET status = 'failed',
                                last_error = 'delivery_failed_after_retries', updated_at = ?
                            WHERE id = ?
                            """,
                            (now, dm["id"]),
                        )
                    else:
                        # Re-queue for a fresh send with a new attempt/key.
                        await conn.execute(
                            """
                            UPDATE dms SET status = 'pending', attempt = ?, dm_id = NULL,
                                send_retries = 0, reconcile_retries = 0,
                                next_action_at = ?, updated_at = ?
                            WHERE id = ?
                            """,
                            (dm["attempt"] + 1, now, now, dm["id"]),
                        )
                elif status == "queued":
                    # Still queued on the API side; check again soon. This is a
                    # normal transient state, NOT a read failure, so it must not
                    # consume a reconciliation retry.
                    await conn.execute(
                        "UPDATE dms SET next_action_at = ?, updated_at = ? WHERE id = ?",
                        (now + config.RECONCILE_INTERVAL, now, dm["id"]),
                    )
                else:
                    # 200 with a missing/unrecognized status -> bounded read retry.
                    await _reconcile_retry_or_fail(conn, dm, now)
            else:
                # Non-200 (incl. transport error code 0) -> bounded read retry.
                await _reconcile_retry_or_fail(conn, dm, now)


async def reconciler_loop(client: PseudoGramClient) -> None:
    while True:
        try:
            batch = await _due_batch()
            if not batch:
                await asyncio.sleep(0.3)
                continue

            # Reads are free (not rate-limited), so poll the batch concurrently.
            results = await asyncio.gather(
                *(client.get_dm(dm["dm_id"]) for dm in batch)
            )
            await _apply(batch, results)
        except Exception:  # noqa: BLE001 - keep the worker alive
            log.exception("reconciler error; will retry")
            await asyncio.sleep(0.5)
