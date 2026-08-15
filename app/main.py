"""FastAPI application for LinkPlease.

Routes (the graded API contract):

    POST /webhook   -> receive comment events, verify signature, return 200 fast
    POST /rules     -> create a keyword -> DM rule
    GET  /stats     -> live delivery numbers

The webhook deliberately does almost nothing synchronously: it verifies the
signature, stores the raw event, and returns. Three background workers (started
in the lifespan) do the real work asynchronously:

    dispatcher  -> match events against rules, enqueue DMs (with dedup)
    sender      -> send DMs to the mock API under a rate limiter
    reconciler  -> confirm delivery and retry DMs the API later fails
"""

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import config
from . import db
from . import matcher
from . import reconciler
from . import sender
from . import signature
from . import stats
from .api_client import PseudoGramClient
from .rate_limiter import RateLimiter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("linkplease.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not config.API_KEY:
        log.warning(
            "PSEUDOGRAM_API_KEY is not set. DM sending and webhook signature "
            "verification will not work until it is configured."
        )

    await db.init(config.DATABASE_PATH)
    # Recover any DM a previous process left in 'sending' (crashed mid-send).
    await db.recover_stale_sending()
    client = PseudoGramClient()
    limiter = RateLimiter(config.RATE_LIMIT_PER_MINUTE)

    tasks = [
        asyncio.create_task(matcher.dispatcher_loop()),
        asyncio.create_task(sender.sender_loop(client, limiter)),
        asyncio.create_task(reconciler.reconciler_loop(client)),
    ]

    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.close()
        await db.close()


app = FastAPI(title="LinkPlease", lifespan=lifespan)


class RuleIn(BaseModel):
    keyword: str
    dm_message: str


@app.get("/")
async def root() -> dict:
    return {"ok": True, "service": "linkplease"}


@app.post("/webhook")
async def webhook(request: Request):
    # Read the raw bytes exactly as received; the signature covers these bytes.
    body = await request.body()

    if config.VERIFY_SIGNATURES:
        header = request.headers.get("X-PseudoGram-Signature")
        if not signature.verify(body, header, config.API_KEY):
            return JSONResponse(status_code=401, content={"error": "invalid_signature"})

    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return JSONResponse(status_code=400, content={"error": "invalid_json"})

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    if not event_id or not event_type:
        return JSONResponse(status_code=400, content={"error": "missing_fields"})

    # Store and return immediately; the dispatcher does the real work.
    await db.execute(
        """
        INSERT INTO events (event_id, event_type, payload, received_at, processed)
        VALUES (?, ?, ?, ?, 0)
        """,
        (event_id, event_type, body.decode("utf-8"), time.time()),
    )
    return {"status": "ok"}


@app.post("/rules", status_code=201)
async def create_rule(rule: RuleIn):
    keyword = (rule.keyword or "").strip()
    message = (rule.dm_message or "").strip()
    if not keyword or not message:
        raise HTTPException(status_code=400, detail="keyword and dm_message are required")

    rule_id = uuid.uuid4().hex
    await db.execute(
        "INSERT INTO rules (rule_id, keyword, dm_message, created_at) VALUES (?, ?, ?, ?)",
        (rule_id, keyword, message, time.time()),
    )
    return {"rule_id": rule_id, "keyword": keyword, "dm_message": message}


@app.get("/stats")
async def get_stats():
    return await stats.compute()
