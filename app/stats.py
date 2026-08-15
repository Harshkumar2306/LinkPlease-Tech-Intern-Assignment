"""Live stats for GET /stats.

The four numbers are computed straight from the database in one transaction, so
they are mutually consistent even while the workers are busy:

* sent               = DMs the API confirmed as delivered
* failed             = DMs we gave up on after retries
* queued             = DMs still pending (waiting to send / retry), sending
                       (in-flight send), or accepted (waiting on reconciliation)
* duplicates_blocked = times we declined to send because it would be a duplicate
"""

from . import db


async def compute() -> dict:
    async with db.transaction() as conn:
        async def count(sql: str, params: tuple = ()) -> int:
            cur = await conn.execute(sql, params)
            return (await cur.fetchone())[0]

        sent = await count("SELECT COUNT(*) FROM dms WHERE status = 'delivered'")
        failed = await count("SELECT COUNT(*) FROM dms WHERE status = 'failed'")
        queued = await count(
            "SELECT COUNT(*) FROM dms WHERE status IN ('pending', 'sending', 'accepted')"
        )

        cur = await conn.execute(
            "SELECT value FROM counters WHERE name = 'duplicates_blocked'"
        )
        row = await cur.fetchone()
        duplicates_blocked = row["value"] if row else 0

    return {
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates_blocked,
    }
