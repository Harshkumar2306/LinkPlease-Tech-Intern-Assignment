"""Application configuration.

Everything here is read from environment variables so that the same code can run
locally, in Docker, or on a PaaS (Render / Fly / Railway) without changes.
"""

import os


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


# The Pseudogram mock API.
PSEUDOGRAM_BASE_URL = os.environ.get(
    "PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com"
).rstrip("/")

# Your API key from POST /v1/keygen. Required for both DM sending and webhook
# signature verification. The key you deploy with MUST be the same key you
# submit with, or signature verification will reject every webhook.
API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "")

# SQLite database file. SQLite is the single source of truth for every queued
# DM, rule, and event, so nothing is lost if the process restarts.
DATABASE_PATH = os.environ.get("DATABASE_PATH", "linkplease.db")

# Part B: verify the X-PseudoGram-Signature header on POST /webhook.
VERIFY_SIGNATURES = _env_bool("VERIFY_SIGNATURES", True)

# The mock API allows 10 DM sends per rolling 60 seconds. We deliberately send
# at most 9 to leave headroom for clock-skew / window-boundary differences, so
# we never breach the rate limit.
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "9"))

# Maximum number of distinct, confirmed delivery attempts for a single DM before
# we give up and mark it "failed".
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))

# Maximum number of send-level retries (HTTP 500 / 429 / network errors) before
# we give up on a DM that has never been accepted by the API.
MAX_SEND_RETRIES = int(os.environ.get("MAX_SEND_RETRIES", "20"))

# Maximum number of reconciliation *read* retries (non-200, or 200 with a
# malformed/unrecognized body) for a single accepted DM before we give up and
# mark it "failed". This bounds GET /v1/dm/{id} polling that never reaches a
# terminal status, so /stats can never show such a DM as queued forever.
MAX_RECONCILE_RETRIES = int(os.environ.get("MAX_RECONCILE_RETRIES", "10"))

# How often (seconds) the reconciler polls GET /v1/dm/{id} for accepted DMs.
# Reads are not rate-limited, so a short interval is safe.
RECONCILE_INTERVAL = float(os.environ.get("RECONCILE_INTERVAL", "3.0"))

# How often the dispatcher wakes to look for unprocessed events.
DISPATCH_INTERVAL = float(os.environ.get("DISPATCH_INTERVAL", "0.1"))

# Timeout (seconds) for outbound HTTP calls to the mock API.
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "15.0"))
