# Loom outline (3 minutes, screen + voice, no editing)

Answer these two questions. Talk normally; the goal is to show you understand
what you built, not polish. Suggested structure below — don't read it verbatim.

## 0. 20 seconds — what it is

Show the repo and the running app. One line: "This is a small LinkPlease: a
webhook receives comment events, matches them against rules, and DMs the
commenter — with dedup, retries, and delivery reconciliation on top of a hostile
mock API."

## 1. (~70 seconds) One tradeoff I made, and what I gave up

The tradeoff to talk about: **one process, one sender worker, because correctness
depends on a single serialised writer.**

- The dedup is an `INSERT OR IGNORE` into a `(rule_id, user_id)` primary-key
  table. The rate limiter is a sliding window persisted in SQLite. Both are only
  safe because one process serialises all writes.
- What I gave up: horizontal scaling and redundancy. If that one process dies, I
  have no second worker to take over instantly.
- Why it's still the right call: the API only allows ~10 DMs/minute, so a single
  sender is *never* the bottleneck. The throughput ceiling is the API's rate
  limit, not my worker count. I traded scalability I can't use for a dedup
  guarantee I can't do without.

(Alternative if you prefer: the conservative 9-per-minute rate limit — I gave up
~10% throughput for a hard guarantee of never breaching the limit.)

## 2. (~70 seconds) What I'd do differently with one more week

- Replace the SQLite-polling workers with a real **outbox + message queue**
  (Postgres `SKIP LOCKED` or Redis streams), so multiple workers could run while
  keeping a distributed lock for the rate limiter and dedup — the thing I
  explicitly gave up above.
- Add **observability**: counters/gauges for queue depth, send latency, 429/500
  rates, and reconciliation lag, so I could tune backoff and `RECONCILE_INTERVAL`
  from real data instead of constants.
- Build a **chaos harness**: run their `/v1/simulate` against a public URL in a
  loop, crash the process mid-run on purpose, and assert zero lost/duplicate DMs
  against `/v1/simulate/{id}/truth` automatically — turning the edge cases in
  FAILURES.md into regression tests.

## 3. 30 seconds — show it working

Run `scripts/smoke_test.py` (or the live app) and point at the three routes:
`POST /rules`, `POST /webhook` (show a forged request getting 401), and
`GET /stats` where `sent` matches what the API actually delivered.

## 4. 20 seconds — close honestly

"Everything is in FAILURES.md — the multi-worker hazard, the accept-then-crash
window, the reconciliation lag, and the duplicates_blocked definition. Ask me
about any of them."
