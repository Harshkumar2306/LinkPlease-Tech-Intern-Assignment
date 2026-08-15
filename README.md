# LinkPlease — Tech Intern Assignment

A small, production-minded version of LinkPlease's core loop: someone comments a
keyword on a post, and we DM them the price list. Built on top of the
[Pseudogram mock API](https://pseudogram-api.onrender.com).

**Stack:** Python 3.11, FastAPI, `httpx`, `aiosqlite` (SQLite). No framework
magic — everything is plain code you can read top to bottom.

## What it does

| Part | Status | Notes |
|------|--------|-------|
| A — rules, matching, dedup, no silent loss | ✅ | `POST /rules`, `POST /webhook`, SQLite-backed queue |
| B — signature verification, live `/stats` | ✅ | HMAC-SHA256 over the raw body; stats computed in one transaction |
| C — reconciliation, `comment.deleted`, load | ✅ | `GET /v1/dm/{id}` poll + retry, tombstones/cancel, 520-event load test |

The one number that cannot be faked is `sent`: a DM is only counted as `sent`
after the mock API *confirms* it as `delivered` (its `202`/`200` is only
"accepted", not "delivered").

## Architecture

```
                 POST /webhook
   (verify HMAC, store raw event, return 200 in <1ms)
                       │
                       ▼
              ┌─────────────────┐
              │     SQLite      │   single source of truth for *everything*
              │  events / rules │   (survives restarts — nothing is in memory)
              │  dms / dedup    │
              │  counters/log   │
              └─────────────────┘
              ▲       ▲       ▲
              │       │       │
        dispatcher  sender  reconciler
```

Three background workers run forever (started in the FastAPI `lifespan`):

1. **dispatcher** — polls unprocessed events, matches comment text
   (case-insensitive substring) against rules, and *atomically claims* the
   `(rule_id, user_id)` pair. The claim is an `INSERT OR IGNORE` into a table
   with that pair as the primary key, so **two identical events arriving within
   the same millisecond can never both send** — the database decides, not our
   code.
2. **sender** — sends `pending` DMs to `POST /v1/dm/send` under a
   sliding-window **rate limiter** (max 9 per rolling 60s). Before each send it
   atomically reserves the DM (`pending → sending`) so a concurrent
   `comment.deleted` (which only cancels `pending` rows) can no longer cancel it
   out from under the sender. Retries 500/429 with backoff, reusing the same
   `Idempotency-Key`, so a retry after an uncertain failure can never create a
   duplicate DM. On startup it resets any DM a crash left `sending`.
3. **reconciler** — polls `GET /v1/dm/{id}` for every `accepted` DM (reads are
   not rate-limited). `delivered` → done; `failed` → re-queue for a fresh send
   with a *new* key, up to `MAX_ATTEMPTS`; `queued` → keep waiting.

The webhook does **not** do the real work inline. It verifies the signature,
inserts one row, and returns — so it can never block and start dropping events.

## The graded API contract

### `POST /webhook`

Receives `comment.created` / `comment.deleted` events. Verifies
`X-PseudoGram-Signature` (HMAC-SHA256 of the raw body, keyed by your API key),
stores the event, returns `200` immediately. Rejects forged requests with `401`.

### `POST /rules`

```json
// request
{ "keyword": "PRICE", "dm_message": "Here's the price list: ..." }
// response 201
{ "rule_id": "…", "keyword": "PRICE", "dm_message": "…" }
```

### `GET /stats`

```json
{ "sent": 142, "failed": 3, "queued": 8, "duplicates_blocked": 57 }
```

- `sent` — the API confirmed `delivered`
- `failed` — gave up after all retries
- `queued` — `pending` (waiting to send/retry), `sending` (in-flight), or
  `accepted` (awaiting reconcile)
- `duplicates_blocked` — declined sends due to dedup (see *FAILURES.md* for the
  exact definition)

## Run locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Apply, then get your key
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H 'Content-Type: application/json' \
  -d '{"name":"Your Name","email":"you@example.com","phone":"+91…","whatsapp":"+91…","linkedin_url":"https://linkedin.com/in/you"}'

curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H 'Content-Type: application/json' -d '{"email":"you@example.com"}'

# 2. Run
export PSEUDOGRAM_API_KEY="<your key>"
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

> **The key you deploy with must be the same key you submit with**, or webhook
> signature verification will reject every event.

## Test it

Two scripts exercise the real API (no mocks):

```bash
# correctness: 3 sends + repeat + redelivery + forged signature
PSEUDOGRAM_API_KEY=<key> python scripts/smoke_test.py

# load: 520 events in ~1s, dedup, and rate-limit invariants (~90s)
PSEUDOGRAM_API_KEY=<key> python scripts/load_test.py
```

To test *their* simulator against a deployed URL:

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H 'Content-Type: application/json' -H "X-API-Key: $PSEUDOGRAM_API_KEY" \
  -d '{"webhook_url":"https://your-app.example.com/webhook","count":500,"duration_seconds":10}'
# then check the truth endpoint against your /stats
```

## Deploy

Three options, all pinned to **one worker** (see *FAILURES.md* #1 for why):

- **Render** — push and use `render.yaml` (attaches a persistent disk for the DB).
- **Docker** — `docker build -t linkplease .` then run with a mounted volume for
  `/data` and `PSEUDOGRAM_API_KEY` set.
- **Any PaaS** — it's just `uvicorn app.main:app --workers 1`; set the env vars
  below and make sure the SQLite file lives on a **persistent disk**.

## Configuration

All via environment variables (see `.env.example`):

| Variable | Default | Meaning |
|----------|---------|---------|
| `PSEUDOGRAM_API_KEY` | — | **Required.** Your API key. |
| `PSEUDOGRAM_BASE_URL` | `https://pseudogram-api.onrender.com` | Mock API root |
| `DATABASE_PATH` | `linkplease.db` | SQLite file (must be on persistent disk) |
| `VERIFY_SIGNATURES` | `true` | Reject forged webhooks |
| `RATE_LIMIT_PER_MINUTE` | `9` | Max sends per rolling 60s (API allows 10) |
| `MAX_ATTEMPTS` | `5` | Confirmed delivery attempts before giving up |
| `MAX_SEND_RETRIES` | `20` | Send-level retries before giving up |
| `MAX_RECONCILE_RETRIES` | `10` | Read retries for a DM stuck in `accepted` before giving up |
| `RECONCILE_INTERVAL` | `3.0` | Seconds between delivery checks |
| `DISPATCH_INTERVAL` | `0.1` | Dispatcher poll interval |
| `HTTP_TIMEOUT` | `15.0` | Outbound HTTP timeout |


