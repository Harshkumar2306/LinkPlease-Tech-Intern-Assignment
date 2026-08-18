# 🚀 LinkPlease — Tech Intern Assignment

> A resilient, production-minded automation engine for Instagram creators built on top of the hostile [Pseudogram API](https://pseudogram-api.onrender.com).

---

## 📌 Submission Details

| Field | Details |
|---|---|
| **Candidate Name** | Harsh Kumar |
| **Email** | `harsh_kumar@srmap.edu.in` |
| **Deployed API Base URL** | [`https://linkplease-harsh.onrender.com`](https://linkplease-harsh.onrender.com) |
| **Demo & Walkthrough Video** | [Watch on Google Drive](https://drive.google.com/file/d/1Y_dAVjxp8iNauc3aMzO3IFHMwR38ZRCm/view?usp=sharing) |
| **Parts Completed** | **Part A + Part B + Part C** (100% Scope) |
| **Failure Analysis** | [`FAILURES.md`](./FAILURES.md) |

---

## 🎯 What It Does

| Part | Status | Description |
|---|:---:|---|
| **Part A — Core Engine** | ✅ | Dynamic rule creation (`POST /rules`), case-insensitive substring matching, atomic SQLite dedup per `(rule, user)`, and queue persistence ensuring zero silent message loss. |
| **Part B — Security & Stats** | ✅ | HMAC-SHA256 constant-time webhook signature verification (`X-PseudoGram-Signature`), rejecting forged requests with `401`. Real-time, atomic `/stats` endpoint. |
| **Part C — Reconciliation & Load** | ✅ | Background reconciliation loop (`GET /v1/dm/{id}`) to detect and retry accepted DMs that later failed. `comment.deleted` tombstoning. Sliding-window rate limiter (9 req/min). |

---

## 🏗️ Architecture & Design

```mermaid
graph TB
    subgraph Client["🌐 External Platform & Users"]
        IG["👤 Instagram Commenter"]:::extNode
        CREATOR["🎨 Creator Post"]:::extNode
        PSEUDO["⚡ Pseudogram Hostile Mock API"]:::extApiNode
        IG -->|"1. Comments 'PRICE'"| CREATOR
        CREATOR -->|"2. Platform Event"| PSEUDO
        PSEUDO -->|"3. POST /webhook (HMAC-SHA256)"| WH
        SND -->|"6. POST /v1/dm/send (9 req/min)"| PSEUDO
        REC -->|"7. GET /v1/dm/{id} (Poll Status)"| PSEUDO
        PSEUDO -->|"8. Direct Message Delivered"| IG
    end

    subgraph Backend["⚙️ LinkPlease Engine (Render - Docker)"]
        WH["📥 FastAPI Webhook Handler<br/><code>POST /webhook</code>"]:::apiNode
        RULES["📋 Rule Management<br/><code>POST /rules</code>"]:::apiNode
        STATS["📊 Real-Time Metrics<br/><code>GET /stats</code>"]:::apiNode

        subgraph Workers["🔄 Async Background Worker Loops"]
            DISP["🔍 Dispatcher Engine<br/><code>Regex & Keyword Matching</code>"]:::workerNode
            DEDUP{"Atomic Dedup Check<br/><code>INSERT OR IGNORE</code>"}:::decisionNode
            RL["⏱️ Sliding-Window Limiter<br/><code>Max 9 Sends / rolling 60s</code>"]:::limiterNode
            SND["📤 Sender Worker<br/><code>Idempotency-Key & Backoff</code>"]:::workerNode
            REC["🩺 Reconciler Loop<br/><code>Status Sync & Retry</code>"]:::workerNode
            BLK["🚫 Block & Count (+1)"]:::blockNode

            DISP --> DEDUP
            DEDUP -->|"Unique (rule_id, user_id)"| RL
            DEDUP -->|"Duplicate Event"| BLK
            RL --> SND
        end

        DB[("🗄️ SQLite Engine (Single Source of Truth)<br/><code>events • rules • dms • dedup</code>")]:::dbNode

        WH -->|"Raw Event Buffer (<1ms)"| DB
        RULES -->|"Store Rule"| DB
        DB -->|"Fetch Unprocessed Events"| DISP
        SND -->|"Update State (pending→sending→accepted)"| DB
        REC -->|"Update State (delivered/failed)"| DB
        DB -->|"Atomic Aggregation Query"| STATS
    end

    %% Premium Modern Palette
    classDef default font-family:system-ui,sans-serif,font-size:13px;
    classDef extNode fill:#fef2f2,stroke:#f87171,stroke-width:2px,color:#991b1b;
    classDef extApiNode fill:#fff1f2,stroke:#e11d48,stroke-width:2px,color:#881337;
    classDef apiNode fill:#e0e7ff,stroke:#6366f1,stroke-width:2px,color:#1e1b4b;
    classDef workerNode fill:#f3e8ff,stroke:#a855f7,stroke-width:2px,color:#581c87;
    classDef decisionNode fill:#fef3c7,stroke:#f59e0b,stroke-width:2px,color:#78350f;
    classDef limiterNode fill:#ecfdf5,stroke:#10b981,stroke-width:2px,color:#064e3b;
    classDef blockNode fill:#fee2e2,stroke:#ef4444,stroke-width:2px,color:#7f1d1d;
    classDef dbNode fill:#e0f2fe,stroke:#0284c7,stroke-width:2.5px,color:#0c4a6e;

    style Client fill:#fffbeb,stroke:#fcd34d,stroke-width:2px,color:#92400e
    style Backend fill:#f8fafc,stroke:#64748b,stroke-width:2px,color:#0f172a
    style Workers fill:#f5f3ff,stroke:#c4b5fd,stroke-width:2px,color:#4c1d95
```

### 1. Fast, Non-Blocking Ingestion (`app/main.py`)
- The `/webhook` handler verifies the HMAC signature and writes the raw event directly to SQLite.
- It returns `200 OK` in `< 1ms`, completely decoupling ingestion from processing and guaranteeing zero dropped webhooks under burst load.

### 2. Atomic Dispatcher (`app/matcher.py`)
- Polls unprocessed events and matches comment text against active rules.
- Atomically claims the `(rule_id, user_id)` pair using `INSERT OR IGNORE`.
- **Guarantee:** Two identical events arriving within the same millisecond can never produce duplicate DMs. The database enforces this, not application memory.

### 3. Rate-Limited Sender (`app/sender.py` & `app/rate_limiter.py`)
- Sends queued DMs under a **sliding-window rate limiter** capped at 9 requests per rolling 60 seconds (providing a safe margin below the 10/min API limit).
- Atomically transitions status (`pending → sending`) before dispatching HTTP calls.
- Attaches an `Idempotency-Key` to every outbound send, preventing duplicate DMs even when retrying after network timeouts.

### 4. Background Reconciler (`app/reconciler.py`)
- The Pseudogram API returns `202 Accepted` on send, but ~15% of accepted DMs fail asynchronously.
- The reconciler polls `GET /v1/dm/{dm_id}`:
  - `delivered` → Marks status as delivered (increments `sent` in `/stats`).
  - `failed` → Re-queues the DM with a fresh idempotency key up to `MAX_ATTEMPTS`.
  - `queued` → Waits for the platform to process.

---

## 📡 API Contract

### `POST /webhook`
Receives comment events from Pseudogram. Validates `X-PseudoGram-Signature` (`sha256=<hex>`).
```bash
# Valid Signature -> 200 OK
# Forged Signature -> 401 Unauthorized
```

### `POST /rules`
Creates a matching rule for automatic DM triggers.
```json
// Request
{
  "keyword": "PRICE",
  "dm_message": "Here is the price list: https://example.com/pricing"
}

// Response (201 Created)
{
  "rule_id": "f485b75de2e64986890edc922133f39b",
  "keyword": "PRICE",
  "dm_message": "Here is the price list: https://example.com/pricing"
}
```

### `GET /stats`
Returns live system metrics computed in a single atomic transaction.
```json
{
  "sent": 142,
  "failed": 3,
  "queued": 8,
  "duplicates_blocked": 57
}
```
- **`sent`**: Confirmed delivered by the platform.
- **`failed`**: Terminal failures after exhausting all retry backoffs.
- **`queued`**: In-flight, pending dispatch, or awaiting reconciliation.
- **`duplicates_blocked`**: Redundant events blocked by dedup constraints.

---

## 🧪 Local Setup & Verification

### 1. Installation
```bash
# Create virtual environment
python3 -m venv .venv && source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Environment Variables
```bash
export PSEUDOGRAM_API_KEY="your_api_key_here"
export PSEUDOGRAM_BASE_URL="https://pseudogram-api.onrender.com"
export DATABASE_PATH="linkplease.db"
```

### 3. Run Automated Test Suites
```bash
# Correctness Suite: rules, dedup, redeliveries, 401 signature rejection
python3 scripts/smoke_test.py

# High-Concurrency Load Suite: 520 events, rate-limit invariants
python3 scripts/load_test.py
```

### 4. Run Server Locally
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

---

## 🔍 Failure Modes & Tradeoffs

For a complete, transparent analysis of all known edge cases, race conditions, and architectural boundaries (including single-worker process locking, ephemeral disk constraints, and clock-skew buffering), see [`FAILURES.md`](./FAILURES.md).



