"""Sliding-window rate limiter for DM sends.

The mock API allows 10 sends per rolling 60 seconds. We track every send
timestamp in the ``send_log`` table so the limiter's memory survives restarts:
after a crash it still knows how many sends happened in the last minute.

We deliberately allow one fewer than the API's limit (9 by default) so that
clock-skew or a rolling-window boundary can never push us over and trigger a
429.
"""

import asyncio
import time

from . import config
from . import db


class RateLimiter:
    def __init__(self, limit: int, window: float = 60.0) -> None:
        self.limit = limit
        self.window = window
        # Small safety buffer added to every wait so we never fire on the exact
        # boundary of the API's own rolling window.
        self.buffer = 0.3

    async def acquire(self) -> None:
        """Block until a send slot is available, then reserve it."""
        while True:
            now = time.time()
            cutoff = now - self.window

            async with db.transaction() as conn:
                # Drop timestamps that have aged out of the window.
                await conn.execute("DELETE FROM send_log WHERE sent_at < ?", (cutoff,))

                cur = await conn.execute("SELECT COUNT(*) FROM send_log")
                count = (await cur.fetchone())[0]

                if count < self.limit:
                    await conn.execute(
                        "INSERT INTO send_log (sent_at) VALUES (?)", (now,)
                    )
                    return

                cur = await conn.execute("SELECT MIN(sent_at) FROM send_log")
                oldest = (await cur.fetchone())[0]

            # Sleep until the oldest send leaves the window, plus a little buffer.
            wait = (oldest + self.window) - now + self.buffer
            if wait > 0:
                await asyncio.sleep(wait)
            # Loop back around and re-check.


_limiter: RateLimiter | None = None


def get() -> RateLimiter:
    """Return the process-wide limiter, creating it lazily."""
    global _limiter
    if _limiter is None:
        _limiter = RateLimiter(config.RATE_LIMIT_PER_MINUTE)
    return _limiter
