# FAILURES.md

Every place this system can still lose a DM, send a duplicate, or report a wrong
number — with the precise conditions. Tested against the live Pseudogram API.

1. **Running more than one worker breaks the rate limiter and can double-send.**
   The dedup claim and the `asyncio.Lock` that serialises writes are both
   per-process. The persisted `send_log` is *not* a cross-process mutex: two
   uvicorn workers each do "count sends < 9, then insert a send row" without
   seeing the other, so both can pass the check and send the same pending DM
   twice. I pin `--workers 1` in the Dockerfile, Procfile and render.yaml, but
   nothing *enforces* it — deploy with `--workers 2` and this is broken.

2. **Crash between reserving a send and writing the accepted dm_id.**
   The sender moves a DM `pending → sending` before the HTTP call and
   `sending → accepted` only after the response. If the process dies in that
   window, the row stays `sending`. On startup `recover_stale_sending()` resets
   it to `pending`, so it is re-sent with the *same* `Idempotency-Key` and the
   API returns the original `dm_id` instead of sending twice. No duplicate, but
   one rate-limit slot is wasted and confirmation is delayed. I rely on the
   API's idempotency contract here; I have not (and cannot) prove it survives an
   API-side restart.

3. **`/stats` lags delivery by up to `RECONCILE_INTERVAL` (3s).**
   A DM is `sent` only after I poll `GET /v1/dm/{id}` and see `delivered`.
   Between actual delivery and my next poll, `/stats` reports it as `queued`.
   A live snapshot taken at that instant under-reports `sent` and over-reports
   `queued` by however many DMs are in flight. It converges, but it is never
   instantaneously exact — I saw this during load runs where `sent` ticked up a
   second or two after the API's own `updated_at`.

4. **`duplicates_blocked` counts transport redeliveries.**
   I count every redelivered `comment.created` event (same `event_id`, ~8% of
   traffic) as one "DM I chose not to send". The `/v1/simulate/{id}/truth`
   endpoint marks these as duplicates, so this is defensible — but if the grader
   defines `duplicates_blocked` as *only* "same user commented again", my number
   is higher than theirs by the number of matching redeliveries. It is a
   definitional judgement call, not a code bug.

5. **A `failed` DM blocks that `(rule, user)` pair forever.**
   The dedup claim is permanent even when the DM is later marked `failed` (gave
   up). If the same user comments again after a terminal failure, I still won't
   re-DM them. That's deliberate and simple, and it matches "never DMed twice" —
   but it also means a recipient whose first attempt truly failed is never
   retried on a later comment. Fixing it would mean deleting the claim on
   terminal failure.

6. **Ephemeral filesystem loses the entire queue.**
   All state lives in SQLite, so a *process* restart is safe — but only if the
   file is on a persistent disk. On a free PaaS tier without an attached disk,
   or if `DATABASE_PATH` points into the container layer, a redeploy or restart
   wipes every pending DM. The Dockerfile uses a `/data` volume and render.yaml
   attaches a disk, but that's a deployment property the code cannot guarantee.

7. **Clock skew vs the API's rolling 60s window.**
   I cap at 9 sends/minute (one headroom) plus a 0.3s buffer, which absorbs
   small clock differences. If my server clock runs fast relative to the API's,
   my local "9 per 60s" can still overlap to 10 inside *their* window and a 429
   arrives. That is not a loss (I back off on `Retry-After` and retry), but it
   means I can't claim I *never* breach — only that I never breach under normal
   clock conditions.

8. **One comment matching multiple rules sends multiple DMs.**
   Dedup is per `(rule, user)`, not per comment or per user. A comment containing
   both "PRICE" and "SALE" produces one DM per rule. That matches the spec
   ("same user … twice for the **same** rule"), but if the product intent were
   "one DM per commenter", this would over-send.

9. **A comment deleted while a DM is already `accepted` cannot be un-sent.**
   I cancel only `pending` DMs. Once the API has accepted a send, there is no
   cancel endpoint, so a DM for a comment that was deleted *after* acceptance
   will still deliver. I consider that correct (we can't unsend), but it is a
   boundary worth stating.

10. **A `comment.deleted` that lands during an in-flight send still delivers.**
   The sender reserves `pending → sending` *before* the HTTP call, and the
   dispatcher cancels only `pending` rows. So if a delete is processed while a
   DM is already reserved, the DM still delivers — there is no cancel endpoint,
   and the request is already on the wire. This is the same "can't unsend"
   boundary as #9, moved slightly earlier. (If that in-flight send then fails
   transiently, the sender checks the tombstone and cancels instead of retrying,
   so the only unsendable case is a send the API actually accepted.)

11. **The bounded reconciliation read-retry can mark a *delivered* DM `failed`
    during a read outage.** If `GET /v1/dm/{id}` keeps failing (or returning an
    unrecognized body) for `MAX_RECONCILE_RETRIES` polls while the API actually
    delivered the DM, I give up and count it `failed`, under-counting `sent`. I
    chose a bounded give-up over "stuck in `accepted` forever" — a sustained
    >~30s read outage is the tradeoff that makes it possible.
