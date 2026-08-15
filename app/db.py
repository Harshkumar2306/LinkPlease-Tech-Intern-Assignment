"""SQLite access layer.

SQLite is the single source of truth for the whole system. Events, rules, DMs,
the deduplication index, and the send log all live here, so a process restart
never loses a queued DM.

Concurrency model
-----------------
The whole app runs in one event loop (single uvicorn worker). A module-level
``asyncio.Lock`` serialises every SQLite operation, so we never touch the
connection from two coroutines at once. Network calls are always made *outside*
the lock; the lock is only held for short local operations.
"""

import asyncio
import os
import time
from contextlib import asynccontextmanager

import aiosqlite

_conn: aiosqlite.Connection | None = None
_lock = asyncio.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    rule_id     TEXT PRIMARY KEY,
    keyword     TEXT NOT NULL,
    dm_message  TEXT NOT NULL,
    created_at  REAL NOT NULL
);

-- Every received webhook payload, including redeliveries. event_id is NOT
-- unique here because the mock API redelivers ~8% of events on purpose; the
-- deduplication of *DMs* happens in dm_dedup instead.
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     TEXT NOT NULL,
    received_at REAL NOT NULL,
    processed   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_processed ON events(processed, id);

-- Comment state, used to handle out-of-order comment.deleted events.
CREATE TABLE IF NOT EXISTS comments (
    comment_id  TEXT PRIMARY KEY,
    user_id     TEXT,
    username    TEXT,
    text        TEXT,
    created_at  REAL,
    is_deleted  INTEGER NOT NULL DEFAULT 0
);

-- (rule_id, user_id) -> dm. The PRIMARY KEY is the "never DM the same person
-- twice for the same rule" guarantee, enforced atomically by the database.
CREATE TABLE IF NOT EXISTS dm_dedup (
    rule_id     TEXT NOT NULL,
    user_id     TEXT NOT NULL,
    dm_id       TEXT NOT NULL,
    created_at  REAL NOT NULL,
    PRIMARY KEY (rule_id, user_id)
);

-- One row per DM we intend to send. status is:
--   pending   -> waiting to be sent (or waiting on a send retry)
--   accepted  -> API returned a dm_id; waiting for reconciliation
--   delivered -> terminal success
--   failed    -> terminal, gave up after retries
--   cancelled -> the comment was deleted before we sent it
CREATE TABLE IF NOT EXISTS dms (
    id             TEXT PRIMARY KEY,
    rule_id        TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    username       TEXT,
    comment_id     TEXT NOT NULL,
    message        TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    attempt        INTEGER NOT NULL DEFAULT 1,
    dm_id          TEXT,
    send_retries   INTEGER NOT NULL DEFAULT 0,
    reconcile_retries INTEGER NOT NULL DEFAULT 0,
    next_action_at REAL NOT NULL,
    last_error     TEXT,
    created_at     REAL NOT NULL,
    updated_at     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dms_status ON dms(status, next_action_at);
CREATE INDEX IF NOT EXISTS idx_dms_comment ON dms(comment_id);

-- Simple counters (e.g. duplicates_blocked) so /stats is a pure read.
CREATE TABLE IF NOT EXISTS counters (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

-- Persisted send timestamps. This is the rate limiter's memory: because it is
-- on disk, a restart cannot "forget" recent sends and accidentally breach the
-- rolling 60s limit.
CREATE TABLE IF NOT EXISTS send_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    sent_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_send_log_sent_at ON send_log(sent_at);
"""


async def init(path: str) -> None:
    """Open the database, apply the schema, and seed the counters row."""
    global _conn
    # Make sure the parent directory exists (useful for ./data/linkplease.db).
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    _conn = await aiosqlite.connect(path)
    _conn.row_factory = aiosqlite.Row
    # WAL lets readers proceed while a writer holds the write lock.
    await _conn.execute("PRAGMA journal_mode=WAL")
    # NORMAL is a good balance between durability and speed for this workload.
    await _conn.execute("PRAGMA synchronous=NORMAL")
    await _conn.execute("PRAGMA busy_timeout=5000")
    await _conn.executescript(_SCHEMA)

    # Lightweight migration: databases created before reconcile_retries existed
    # do not have the column. Add it without touching any existing rows.
    cur = await _conn.execute("PRAGMA table_info(dms)")
    columns = {row[1] for row in await cur.fetchall()}
    if "reconcile_retries" not in columns:
        await _conn.execute(
            "ALTER TABLE dms ADD COLUMN reconcile_retries INTEGER NOT NULL DEFAULT 0"
        )

    await _conn.execute(
        "INSERT OR IGNORE INTO counters(name, value) VALUES('duplicates_blocked', 0)"
    )
    await _conn.commit()


async def close() -> None:
    global _conn
    if _conn is not None:
        await _conn.close()
        _conn = None


async def recover_stale_sending() -> None:
    """Return any DM stuck in 'sending' back to 'pending' on startup.

    A DM is only ever 'sending' while the (single) sender worker is actively
    sending it. On a fresh process there is no live sender, so a 'sending' row
    must be a crash leftover from a previous process. Reset it so it is re-sent
    with the *same* idempotency key — the API returns the original dm_id if it
    actually accepted the earlier request, so no duplicate is created.
    """
    await execute(
        "UPDATE dms SET status = 'pending', updated_at = ? WHERE status = 'sending'",
        (time.time(),),
    )


@asynccontextmanager
async def lock():
    """Hold the global DB lock across several statements (no commit boundary)."""
    async with _lock:
        yield


async def execute(sql: str, params: tuple = ()) -> aiosqlite.Cursor:
    """Run a single statement and commit it. Do NOT call inside transaction()."""
    assert _conn is not None, "db.init() must run before db.execute()"
    async with _lock:
        cursor = await _conn.execute(sql, params)
        await _conn.commit()
        return cursor


async def fetchone(sql: str, params: tuple = ()) -> aiosqlite.Row | None:
    async with _lock:
        cursor = await _conn.execute(sql, params)
        return await cursor.fetchone()


async def fetchall(sql: str, params: tuple = ()) -> list[aiosqlite.Row]:
    async with _lock:
        cursor = await _conn.execute(sql, params)
        return await cursor.fetchall()


@asynccontextmanager
async def transaction():
    """Run several statements atomically.

    Yields the raw connection; use ``await conn.execute(...)`` inside the block.
    On any exception the whole block is rolled back. The block is committed when
    it exits normally. The global lock is held for the entire block, so nothing
    else can interleave.
    """
    assert _conn is not None, "db.init() must run before db.transaction()"
    async with _lock:
        await _conn.execute("BEGIN IMMEDIATE")
        try:
            yield _conn
        except BaseException:
            await _conn.rollback()
            raise
        else:
            await _conn.commit()
