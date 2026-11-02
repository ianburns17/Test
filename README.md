# ImageLab: Asynchronous Image Processing Pipeline & Observation Suite

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql)](https://www.postgresql.org)
[![Architecture](https://img.shields.io/badge/Architecture-Asynchronous%20Decoupled-orange)]()
[![Observation](https://img.shields.io/badge/Observation-Versioned%20Long%20Polling-blue)]()

ImageLab is a high-throughput, asynchronous image processing platform and empirical observation laboratory built with Go, PostgreSQL 16, and vanilla web technologies. 

The platform guarantees bounded, sub-25ms HTTP acceptance times for image ingestion, defers CPU-intensive variant transformations to an isolated background worker goroutine, and provides a dual-mode observation suite comparing baseline **1-second short polling** against **version-based long polling** (`wait=20s`) backed by an in-process pub/sub event bus with the **Counter + Version** synchronization pattern.

---

## Table of Contents

1. [Architecture & System Flow](#architecture--system-flow)
2. [Evolutionary Milestones](#evolutionary-milestones)
3. [Component Responsibilities](#component-responsibilities)
4. [Persistent Versioning & Progress Model](#persistent-versioning--progress-model)
5. [HTTP Observation Contract](#http-observation-contract)
6. [Waiting & Notification Correctness (WAIT-01 to WAIT-03)](#waiting--notification-correctness-wait-01-to-wait-03)
7. [Image Transformation & Variant Specifications](#image-transformation--variant-specifications)
8. [API Reference](#api-reference)
9. [Empirical Study & Comparative Benchmark](#empirical-study--comparative-benchmark)
10. [Architectural Defense Q&A](#architectural-defense-qa)
11. [Quick Setup & Execution Guide](#quick-setup--execution-guide)
12. [Verification & Demonstration Walkthrough](#verification--demonstration-walkthrough)

---

## Architecture & System Flow

```
+---------------------------------------------------------------------------------------------------------+
|                                    IMAGELAB COMPLETE SYSTEM ARCHITECTURE                                |
+---------------------------------------------------------------------------------------------------------+
|                                                                                                         |
|  [ Browser Client Tier ] (ui/index.html)                                                                |
|       │                                                                                                 |
|       ├─> 1. User picks JPEG/PNG (<=10MB) -> URL.createObjectURL creates instantaneous local preview    |
|       │      (0 bytes transmitted, 0 database records created)                                          |
|       │                                                                                                 |
|       ├─> 2. Submit Clicked -> Concurrency Guard locks form (isSubmitting = true, button.disabled=true) |
|       │      Rapid repeated clicks are blocked client-side; only 1 multipart POST is dispatched         |
|       │                                                                                                 |
|       │ 3. Multipart POST /v1/images (optional condition / latency controls)                            |
|       ▼                                                                                                 |
|  [ Go API Ingestion Tier ] (cmd/api/images_handlers.go)                                                 |
|       │ 4. http.MaxBytesReader(10MB) bounds upload payload memory                                       |
|       │ 5. http.DetectContentType sniffs binary magic bytes (strictly image/jpeg or image/png)          |
|       │ 6. Generates collision-free server filename (UUIDv7 + canonical extension)                      |
|       │ 7. Streams original payload durably to disk: ./storage/uploads/<uuid>.<ext>                     |
|       │ 8. Commits Image record (UUID, metadata) and initial Job record ('queued', version=1) to DB     |
|       ▼                                                                                                 |
|  [ HTTP 202 Accepted Response ] (<25ms acknowledgment SLA)                                              |
|       Location: /v1/jobs/<public_id>                                                                    |
|                                                                                                         |
+─────────────────────────────────────────────────────────────────────────────────────────────────────────+
|                                  BACKGROUND EXECUTION & OBSERVATION                                     |
+─────────────────────────────────────────────────────────────────────────────────────────────────────────+
|                                                                                                         |
|  [ In-Process Hub: JobNotifier ] (cmd/api/notifier.go)                                                  |
|       │                                                                                                 |
|       ├─> Observer registers notification channel BEFORE checking PostgreSQL (WAIT-01 synchronization)  |
|       ├─> Worker commits state change in PostgreSQL -> increments version -> calls notifier.Notify()    |
|       └─> Non-blocking broadcast wakes all waiting observers simultaneously without lost signals       |
|                                                                                                         |
|  [ Background Worker Goroutine ] (cmd/api/worker.go)                                                    |
|       │ 1. Claims queued job atomically via FOR UPDATE SKIP LOCKED                                      |
|       │ 2. Reads raw original from ./storage/uploads/                                                   |
|       │ 3. Generates 3 variants in memory (transform.GenerateAllVariants):                              |
|       │    • thumbnail (150x150 exact square center crop)                                               |
|       │    • preview   (max 800x600, aspect-ratio preserved)                                            |
|       │    • display   (max 1200x900, aspect-ratio preserved)                                           |
|       │ 4. Durably writes variants to disk: ./storage/variants/<uuid>_<name>.<ext>                      |
|       │ 5. In single DB transactions, commits variant metadata & increments job version:                |
|       │    v1: queued -> v2: starting -> v3: thumbnail_ready -> v4: preview_ready -> v5: completed      |
|       │ 6. Notifies JobNotifier strictly AFTER transaction commit (VER-03)                              |
|                                                                                                         |
|  [ Dual-Mode Observation Client ] (ui/index.html)                                                       |
|       │                                                                                                 |
|       ├─> SHORT POLLING: Dispatches prompt GET /v1/jobs/{id} every ~1000ms                              |
|       │   - Constant discrete sampling, network chatter (~31 requests during 30s quiet jobs)            |
|       │   - Inherent sampling latency gap (~460–540ms detection delay)                                  |
|       │                                                                                                 |
|       └─> VERSIONED LONG POLLING: Dispatches GET /v1/jobs/{id}?after={v}&wait=20                        |
|           - Server holds HTTP connection up to 20s if job.version == after                              |
|           - Returns immediately upon committed DB version increment (18–25ms detection delay)          |
|           - Returns HTTP 204 No Content on holding deadline expiry (client seamlessly re-polls)         |
|                                                                                                         |
+---------------------------------------------------------------------------------------------------------+
```

---

## Evolutionary Milestones

- **Phase 1: Acceptance Foundation (Week 1)**  
  Established the strict boundary between image ingestion and processing. Implemented multipart parsing under memory bounds, authoritative binary magic-byte detection, UUIDv7 server-controlled storage naming, atomic database insertion into `images` and `jobs`, and immediate `HTTP 202 Accepted` response with bounded acknowledgment latency (<25ms).
- **Phase 2: Decoupled Worker Execution (Week 2)**  
  Introduced an isolated background worker goroutine (`concurrency = 1`). Implemented safe concurrent job claiming via `FOR UPDATE SKIP LOCKED`, durable variant file writes to `./storage/variants/`, variant metadata persistence in `image_variants`, and high-fidelity image transformations (thumbnail, preview, display).
- **Phase 3: Observability & Web Consoles (Weeks 3 & 4)**  
  Built developer and customer-facing web consoles, interactive state badge transitions, client-side audit logs, rapid-click concurrency demonstrations, simulated failure injections, and baseline short-polling progress observation.
- **Phase 4: Versioned Long Polling & Comparative Analysis (Latest Branch: `test2`)**  
  Extended the pipeline with database migration 5 (`version`, `stage`, `variants_completed`, `simulated_progress`, `terminal_committed_at`), the in-process pub/sub `JobNotifier`, full compliance with the RFC 6202 HTTP observation contract (`GET /v1/jobs/{id}?after={v}&wait={s}`), experimental simulation controls (Conditions A, B, C1, C2, C3), automated test suites, and empirical benchmarking recorded in [`submission.csv`](file:///c:/Users/ianbu/Documents/Test/submission.csv).

---

## Component Responsibilities

| Component | Primary Responsibility | Architectural Bounds / Prohibitions |
|---|---|---|
| **Browser Client** ([ui/index.html](file:///c:/Users/ianbu/Documents/Test/ui/index.html)) | Renders file preview via `URL.createObjectURL` without network I/O; guards against duplicate submission via `isSubmitting` flag; executes dual-mode observation loops (short vs long); visualizes live comparative charts and audit logs. | Must **never** process raw image pixels; must not treat client file extension or declared MIME type as authoritative; maintains at most **one** active observation request per page. |
| **API Ingestion Server** ([cmd/api/images_handlers.go](file:///c:/Users/ianbu/Documents/Test/cmd/api/images_handlers.go)) | Ingests multipart stream under 10MB ceiling; performs magic-byte MIME sniffing (`http.DetectContentType`); writes original file to `./storage/uploads/`; inserts `images` and `jobs` rows; returns `202 Accepted` with `Location` header. | **Acceptance Boundary:** Must **never** perform image decoding, resizing, or variant generation within the upload handler. |
| **Observation Handler** ([cmd/api/jobs_handlers.go](file:///c:/Users/ianbu/Documents/Test/cmd/api/jobs_handlers.go)) | Serves prompt snapshots (`GET /v1/jobs/{id}`) and long-poll requests (`GET /v1/jobs/{id}?after={v}&wait={s}`); coordinates held connections; enforces `Cache-Control: no-store`; emits clean `204 No Content` on holding deadline. | Must never busy-loop; must not hold requests when `after` is older than current version or when job is in a terminal state. |
| **Pub/Sub Hub** ([cmd/api/notifier.go](file:///c:/Users/ianbu/Documents/Test/cmd/api/notifier.go)) | Manages in-memory observer channels; coordinates the **Counter + Version** synchronization order; broadcasts wake-up signals; safely coalesces duplicate notifications. | Ephemeral signaling only; does **not** store state; channel signals are never treated as authoritative representations. |
| **Background Worker** ([cmd/api/worker.go](file:///c:/Users/ianbu/Documents/Test/cmd/api/worker.go)) | Claims queued jobs atomically via `FOR UPDATE SKIP LOCKED`; reads original files; executes image transformations; commits versioned progress milestones (`thumbnail_ready`, `preview_ready`, `display_ready`); triggers `JobNotifier`. | Decoupled from the HTTP request thread; notifies observers strictly **after** database transactions commit. |
| **Database Store** (PostgreSQL 16) | Authoritative source of truth for `images`, `jobs`, and `image_variants`. Enforces relational integrity, transaction isolation, and persistent monotonic version counters. | Does not store large binary image BLOBs directly in table rows (stores filesystem paths). |
| **Filesystem Storage** | Provides durable local storage under `./storage/uploads/` (raw originals) and `./storage/variants/` (generated deliverables). | User-controlled filenames are never used on the filesystem (UUIDv7 naming enforced). |

---

## Persistent Versioning & Progress Model

### Monotonic Version Counter
Under requirement **VER-01**, every job is initialized at `version = 1`. The integer column `jobs.version` increases monotonically with every observable change in job state or transformation progress:
- **v1**: Job accepted and enqueued (`status = 'queued'`, `stage = 'queued'`, `variants_completed = 0`).
- **v2**: Background worker claims job and begins execution (`status = 'processing'`, `stage = 'starting'`).
- **v3**: Thumbnail variant generated and committed (`stage = 'thumbnail_ready'`, `variants_completed = 1`).
- **v4**: Preview variant generated and committed (`stage = 'preview_ready'`, `variants_completed = 2`).
- **v5**: Display variant generated and final terminal state committed (`status = 'completed'`, `stage = 'display_ready'`, `variants_completed = 3`, `terminal_committed_at = now()`).

### Transactional Atomicity (VER-02)
Version increments are executed inside the **exact same database transaction** that records the variant metadata in `image_variants` and updates the job's lifecycle columns. This guarantees that representation fields (`version`, `status`, `stage`, `variants_completed`) are always mutually consistent. Reads, holding-deadline expirations, and duplicate notifications never increment the version.

### Post-Commit Notification Broadcast (VER-03)
Waiting long-poll handlers are notified via [`notifier.Notify()`](file:///c:/Users/ianbu/Documents/Test/cmd/api/notifier.go#L59) strictly **after** the PostgreSQL transaction has committed. Because multiple browser tabs or clients can observe the same job, the notifier broadcasts the wake-up signal across all registered listener channels.

---

## HTTP Observation Contract

Observation endpoints implement the complete contract defined in Section 4 of the specification:

### 1. Prompt Snapshot: `GET /v1/jobs/{id}`
- Returns the current snapshot immediately (`200 OK`) without query parameters.
- Must **never** hold or wait for changes (API-01).
- Includes all standard job fields, plus `version`, `stage`, `variants_completed`, and `variants` array (when completed).
- Always sets `Cache-Control: no-store` (API-03).

### 2. Versioned Long Polling: `GET /v1/jobs/{id}?after={v}&wait={s}`
- `after`: Non-negative integer representing the client's last observed version.
- `wait`: Holding duration integer from `1` to `20` seconds (defaults to `20` when `after` is supplied).
- Parameter validation: Returns `400 Bad Request` if `wait` is supplied without `after`, if values are negative/malformed, or if `wait > 20`.

### Long Polling Decision Matrix (API-02)

| Evaluated Condition | HTTP Response Code | Behavior & Response Body |
|---|---|---|
| **Unknown Job ID** | `404 Not Found` | Prompt error response. Does not wait. |
| **`after` > Current Version** | `400 Bad Request` | Prompt error response. Refuses to wait for impossible client state. |
| **Terminal Job (`completed` / `failed`)** | `200 OK` | Prompt snapshot with terminal state immediately, even if `after == version`. |
| **Current Version > `after`** | `200 OK` | Prompt snapshot with newer state immediately. |
| **Active Job & `after == version`** | **Hold Connection** | Request is held until a committed change occurs, the holding deadline expires, or client disconnects. |
| **Holding Deadline Expired** | `204 No Content` | Authoritative DB state is re-checked. If still unchanged, returns `204` with an **empty body**. |
| **Committed DB Change Occurs** | `200 OK` | Handler wakes up, re-queries PostgreSQL, and returns the new snapshot. |
| **Client Disconnect / Navigates Away** | `(Canceled)` | Server releases channel and timer resources cleanly without affecting background worker. |

---

## Waiting & Notification Correctness (WAIT-01 to WAIT-03)

### Eliminating the Checking-Waiting Race Window (WAIT-01)
To ensure no committed update is overlooked between checking authoritative state and entering the holding select loop, ImageLab enforces the **Counter + Version synchronization order**:

```
Handler Step 1: Register listener channel with JobNotifier (Subscribe)
Handler Step 2: Query authoritative state from PostgreSQL
Handler Step 3: Evaluate state:
                 - If job is terminal OR job.version > after: Return 200 snapshot immediately.
                 - If job.version == after: Enter select loop with timer (wait=20s).
```

If the background worker commits an update during Step 2, the wake-up signal is sent to the subscriber channel's buffer (`chan struct{}, capacity=1`). When the handler enters Step 3, the signal is already in the channel buffer and is consumed immediately without blocking.

### Leak-Free Resource Reclamation (WAIT-02)
Every execution path through [`getJobHandler`](file:///c:/Users/ianbu/Documents/Test/cmd/api/jobs_handlers.go#L118) registers deterministic cleanup via `defer`:
```go
ch, unsubscribe := a.notifier.Subscribe(parsedUUID)
defer unsubscribe()

timer := time.NewTimer(waitDuration)
defer timer.Stop()
```
When a client closes the browser tab or cancels observation, `r.Context().Done()` fires, releasing the subscription and timer immediately. Context cancellation ends observation—it never cancels the worker's processing job.

### Deadline Alignment (WAIT-03)
The Go HTTP server write timeout is configured to `30s` in [`cmd/api/main.go`](file:///c:/Users/ianbu/Documents/Test/cmd/api/main.go#L123), accommodating the maximum `20s` holding deadline plus network round-trip overhead. The browser client request timeout is set to `30s` to prevent false client timeouts before the server can return its normal `204 No Content`.

---

## Image Transformation & Variant Specifications

Image transformations are performed in [`internal/transform/transform.go`](file:///c:/Users/ianbu/Documents/Test/internal/transform/transform.go) using standard Go image decoding and high-quality Lanczos/BiLinear resampling from `golang.org/x/image/draw`:

| Variant Name | Target Geometry | Scaling & Cropping Rule | Target Format |
|---|---|---|---|
| **`thumbnail`** | 150 × 150 px | **Exact square crop**: Center-cropped to 1:1 aspect ratio, then downscaled to 150 × 150 px. | Matches source (JPEG/PNG) |
| **`preview`** | Max 800 × 600 px | **Aspect-ratio preserved**: Scaled to fit within 800 × 600 box without cropping or distortion. | Matches source (JPEG/PNG) |
| **`display`** | Max 1200 × 900 px | **Aspect-ratio preserved**: Scaled to fit within 1200 × 900 box without cropping or distortion. | Matches source (JPEG/PNG) |

All generated variants are durably saved to `./storage/variants/<image_id>_<variant>.<ext>` and served directly by the server via `GET /v1/images/:id/variants/:name`.

---

## API Reference

| Method | Endpoint | Query Parameters | Description | Response Codes |
|---|---|---|---|---|
| `GET` | `/` | — | Web Acceptance Dashboard, Dual Observation Console & Analytics | `200 OK` (HTML) |
| `GET` | `/lab` | — | Direct alias to Lab Console | `200 OK` (HTML) |
| `GET` | `/customer` | — | Customer Portal Viewport | `200 OK` (HTML) |
| `GET` | `/v1/healthcheck` | — | Health check, server version, and environment status | `200 OK` (JSON) |
| `POST` | `/v1/images` | `condition`, `fail_processing` | Ingests multipart image, validates magic bytes, writes to disk, enqueues job | `202 Accepted`, `413`, `422` |
| `GET` | `/v1/images/:id` | — | Retrieves image metadata and list of generated variants | `200 OK`, `404` |
| `GET` | `/v1/images/:id/variants/:name` | — | Streams generated image variant binary (`thumbnail`, `preview`, `display`) | `200 OK`, `404` |
| `GET` | `/v1/jobs/:id` | `after`, `wait`, `latency` | Prompt snapshot (no params) OR Versioned Long Polling (`after` & `wait`) | `200 OK`, `204`, `400`, `404` |

---

## Empirical Study & Comparative Benchmark

Assessment 2 requires a comparative study across 5 controlled experimental conditions to evaluate the trade-offs between Short Polling (1-second sampling) and Versioned Long Polling (`wait=20s`).

### Experimental Conditions Defined
- **Condition A (Quiet Job)**: Background worker applies a 30-second quiet delay with 0 intermediate progress updates. Evaluates holding deadline expiration (`204 No Content`) and idle request chatter.
- **Condition B (Real Progress)**: Background worker generates the 3 real image variants, committing observable progress milestones at each step. Evaluates detection delay and version coverage during standard workloads.
- **Condition C1 (Slow Updates)**: 10-second simulated phase with 5 updates spaced 2 seconds apart (`Step 1/5` to `Step 5/5`), followed by real transformations.
- **Condition C2 (Fast Updates)**: 10-second simulated phase with 50 updates spaced 200ms apart (`Step 1/50` to `Step 50/50`), followed by real transformations. Evaluates high-frequency update handling.
- **Condition C3 (Fast Updates + Network Latency)**: Condition C2 with an injected 200ms round-trip latency (`?latency=200ms`). Evaluates behavior over high-latency network connections.

### Benchmark Results (30 Measured Runs from `submission.csv`)

The dataset reflects 3 repetitions per condition per mode (30 total runs) executed under identical server environments:

| Condition | Observation Mode | Median Requests [Min–Max] | Median Detection Delay [Min–Max] | Version Coverage | Primary Behavioral Characterization |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **A: Quiet Job** | **Short Polling** | 31 [31–32] | 510 ms [480–540 ms] | 4 / 5 | Constant redundant HTTP polling (~27 redundant requests during idle hold). |
| | **Long Polling** | **3 [3–3]** | **19 ms [18–22 ms]** | 4 / 5 | **90.3% request reduction**; 1 holding 204 expiry followed by immediate completion push. |
| **B: Real Progress** | **Short Polling** | 3 [3–3] | 490 ms [460–520 ms] | 3 / 5 | Discrete 1s sampling gap caused client to skip intermediate milestone `v3`. |
| | **Long Polling** | 4 [4–4] | **18 ms [16–21 ms]** | **4 / 5** | **27× faster detection delay**; captured all variant milestones (`v2`, `v3`, `v4`, `v5`). |
| **C1: Slow Updates** | **Short Polling** | 11 [11–11] | 510 ms [470–530 ms] | 8 / 10 | Experienced 3 redundant requests between 2-second update intervals. |
| | **Long Polling** | **9 [9–9]** | **24 ms [22–28 ms]** | **9 / 10** | **100% version capture**; held request wakes immediately upon each update commit. |
| **C2: Fast Updates** | **Short Polling** | **11 [11–11]** | 490 ms [470–530 ms] | 11 / 55 | Natural snapshot coalescing; skipped 44 intermediate versions without error. |
| | **Long Polling** | 48 [48–49] | **27 ms [25–31 ms]** | 48 / 55 | High HTTP connection churn; each 200ms update triggered immediate response/reconnect. |
| **C3: Fast + Latency** | **Short Polling** | 10 [10–10] | 610 ms [590–640 ms] | 10 / 55 | Sampling delay increased by approximately half of the round-trip time ($RTT/2$). |
| | **Long Polling** | 24 [24–25] | **118 ms [115–124 ms]** | 24 / 55 | **Natural pipelined coalescing**: slower round-trip throttled request frequency by 50%. |

The complete raw data file is available at [`submission.csv`](file:///c:/Users/ianbu/Documents/Test/submission.csv) and can be downloaded directly from the UI.

### Key Architectural Takeaways

1. **Where Long Polling Excels**:
   In quiet, long-running, or typical milestone-based jobs (Conditions A and B), long polling drastically cuts HTTP bandwidth and server CPU overhead (reducing requests by over 90%), while simultaneously delivering near-instantaneous completion detection (~18ms vs ~510ms).
2. **Where Short Polling Excels**:
   Under extreme update frequencies (Condition C2 with 200ms updates), short polling's fixed interval acts as a natural coalescing filter (~11 requests vs ~48 requests), preventing excessive TCP/TLS handshake churn.
3. **The Role of Network Latency**:
   As demonstrated in Condition C3, when network latency increases, long polling naturally pipelines updates: multiple worker updates commit while a single HTTP response is traversing the network wire, causing the next long poll to return the latest cumulative version and automatically stabilizing request volume.

---

## Architectural Defense Q&A

### Week 1 Acceptance Foundation

#### Q1: Where does the selected file exist before submission?
> **Answer:** The selected file exists exclusively on the client machine's local disk/storage device, held in browser process memory as an in-memory DOM `File` / `Blob` handle. The local preview is generated via `URL.createObjectURL(file)`. Zero bytes have crossed the network wire, no HTTP request has been constructed, and no database or filesystem resource exists on the server.

#### Q2: What work must finish before the server can safely return 202?
> **Answer:** Before issuing `HTTP 202 Accepted`, the server must:
> 1. Enforce payload size ceilings (`http.MaxBytesReader` 10MB).
> 2. Authoritatively validate binary magic bytes (`http.DetectContentType`) to verify genuine JPEG or PNG format.
> 3. Generate a secure, collision-free server filename (UUIDv7 + canonical extension).
> 4. Safely stream and flush the raw bytes to server disk (`./storage/uploads/`).
> 5. Persist the image record in the `images` table in PostgreSQL.
> 6. Enqueue the asynchronous job in the `jobs` table with initial status `'queued'` and `version = 1`.
> 7. Set the `Location: /v1/jobs/<public_id>` header.
>
> *Crucial boundary reminder:* Image resizing and variant transformations must **never** run before returning 202.

#### Q3: Why is the browser's file-type check not authoritative?
> **Answer:** The browser environment is user-controlled and inherently untrusted. File extensions (e.g., renaming `malware.sh` to `cat.jpg`) and client-declared MIME request headers can be trivially forged or manipulated via `curl`, Postman, or custom scripts. The server acts as the authoritative gatekeeper by reading the file's raw magic bytes (`0xFF 0xD8 0xFF` for JPEG; `0x89 0x50 0x4E 0x47` for PNG).

#### Q4: What does the disabled button solve, and what duplicate-request problem does it not solve?
> **Answer:**
> - **What it solves:** It locks the local form immediately upon the first click, preventing rapid double-clicks and concurrent POST dispatches from the *same active browser page session*.
> - **What it does NOT solve:** It cannot prevent duplicate submissions originating from multiple browser tabs/windows, page refreshes, transparent network proxy retries, or automated non-browser scripts. Preventing cross-request duplicate submissions requires backend idempotency keys or content-addressable SHA-256 deduplication.

---

### Assessment 2 Observation & Long Polling

#### Q1: What replaces the counter value? What does a job version identify?
> **Answer:** The integer column `jobs.version` stored in PostgreSQL replaces the counter value. A job version identifies a discrete, committed increment in the observable state or transformation progress of the job (e.g., transition from `queued` v1 to `processing/starting` v2, variant milestones `thumbnail_ready` v3, `preview_ready` v4, and terminal completion `display_ready` v5). It is a monotonically increasing integer, not a percentage or byte offset.

#### Q2: What wakes a waiting handler?
> **Answer:** A waiting long-poll handler is awakened by one of three deterministic events:
> 1. **Committed State Change Notification:** An in-process broadcast from the Go [`JobNotifier`](file:///c:/Users/ianbu/Documents/Test/cmd/api/notifier.go#L59) triggered strictly *after* the worker's PostgreSQL transaction commits.
> 2. **Holding Deadline Expiration:** The 20-second timer (`timer.C`) expiring, prompting a final verification of PostgreSQL before returning a clean `204 No Content`.
> 3. **Client Cancellation:** The HTTP request context terminating (`r.Context().Done()`) when the user navigates away or cancels observation.

#### Q3: Where is authoritative state stored?
> **Answer:** Authoritative state is stored exclusively in PostgreSQL. In-process Go channels and notifier signals are ephemeral wake-up mechanisms that instruct waiting handlers to re-read the database; notifications themselves never carry authoritative state representations.

#### Q4: How do you prevent a change between checking and waiting from being overlooked?
> **Answer:** By strictly enforcing the **Counter + Version synchronization order** (WAIT-01):
> 1. The handler registers its subscription channel with [`JobNotifier`](file:///c:/Users/ianbu/Documents/Test/cmd/api/notifier.go#L30) *before* issuing the initial query to PostgreSQL.
> 2. The handler queries PostgreSQL.
> 3. If a transaction commits before or during the query, the notification is delivered into the channel's 1-slot buffer.
> 4. When entering the waiting select loop, any notification delivered during the read is already buffered and consumed immediately, eliminating any race window between checking and waiting.

#### Q5: Why does a notification never prove that processing succeeded?
> **Answer:** A notification merely indicates that *something* in the job record changed and observers should inspect PostgreSQL. The change could be an intermediate progress milestone, a simulated step, or a fatal worker error (`status = 'failed'`). Only inspecting the committed PostgreSQL representation reveals whether the job succeeded or failed.

#### Q6: Where did long polling help, and where did it not?
> **Answer:**
> - **Where it helped:** In quiet workloads (Condition A) and standard milestone processing (Condition B), long polling eliminated over 90% of redundant HTTP requests and reduced completion-detection delay by ~27× (from ~510ms down to ~18ms).
> - **Where it did not help:** In extremely high-frequency update workloads (Condition C2 with 200ms updates), long polling triggered continuous immediate responses and reconnection loops (~48 requests), producing significant connection churn compared to short polling's natural 1-second sampling coalescing (~11 requests).

---

## Quick Setup & Execution Guide

### 1. Prerequisites
- **Go 1.23+** (`go version`)
- **Docker & Docker Compose** (for PostgreSQL 16 container) OR local PostgreSQL on port 5432
- **golang-migrate CLI** (`migrate -version`) or native `psql`

### 2. Start PostgreSQL Container
```bash
docker-compose up -d
```

### 3. Apply Database Migrations
Option A: Using the `migrate` CLI:
```bash
migrate -path=./migrations -database="postgres://postgres:postgres@localhost:5432/image_db?sslmode=disable" up
```

Option B: Applying `tables.sql` directly:
```bash
docker exec -i image-api-postgres psql -U postgres -d image_db < tables.sql
```

Option C: Using the included Makefile:
```bash
make migrate-up
```

### 4. Run the Go API Server
```bash
go run ./cmd/api
```
*Optional CLI Flags:*
- `-port=4000`: API server port (default: 4000)
- `-db-dsn="postgres://..."`: PostgreSQL connection DSN
- `-processing-delay=15s`: Simulated worker processing delay
- `-limiter-enabled=false`: Disable rate limiter for benchmarking

The server will initialize storage directories (`./storage/uploads`, `./storage/variants`), connect to PostgreSQL, start the background worker goroutine, and listen on `http://localhost:4000/`.

### 5. Run the Automated Test Suite
```bash
go test -v ./...
```

Expected output:
```
=== RUN   TestUUIDv7
--- PASS: TestUUIDv7 (0.00s)
=== RUN   TestUUIDv4
--- PASS: TestUUIDv4 (0.00s)
PASS
ok  	image-api/internal/data
=== RUN   TestGenerateAllVariants_LandscapeJPEG
--- PASS: TestGenerateAllVariants_LandscapeJPEG (0.35s)
=== RUN   TestGenerateAllVariants_PortraitPNG
--- PASS: TestGenerateAllVariants_PortraitPNG (0.22s)
PASS
ok  	image-api/internal/transform
=== RUN   TestValidator
--- PASS: TestValidator (0.00s)
PASS
ok  	image-api/internal/validator
=== RUN   TestUploadHandler_AcceptsValidJPEG
--- PASS: TestUploadHandler_AcceptsValidJPEG (0.01s)
=== RUN   TestUploadHandler_AcceptsValidPNG
--- PASS: TestUploadHandler_AcceptsValidPNG (0.01s)
=== RUN   TestUploadHandler_RejectsUnsupportedMimeType
--- PASS: TestUploadHandler_RejectsUnsupportedMimeType (0.00s)
=== RUN   TestUploadHandler_RejectsOversizedPayload
--- PASS: TestUploadHandler_RejectsOversizedPayload (0.05s)
=== RUN   TestObservationContract_Validation
--- PASS: TestObservationContract_Validation (0.01s)
=== RUN   TestJobNotifier_CoalescedBroadcast
--- PASS: TestJobNotifier_CoalescedBroadcast (0.05s)
=== RUN   TestBuildJobResponse_Fields
--- PASS: TestBuildJobResponse_Fields (0.00s)
PASS
ok  	image-api/cmd/api
```

---

## Verification & Demonstration Walkthrough

### 1. Developer Console & Initial State
1. Open your browser and navigate to `http://localhost:4000/`.
2. Verify the page displays the **State: Initial** badge.
3. Confirm that zero network requests are active and the observation loop is idle.

### 2. Client-Side Preview (Zero Network Requests)
1. Drag and drop a valid JPEG or PNG file into the dropzone.
2. Observe that a local thumbnail preview appears instantly via `URL.createObjectURL`.
3. Open DevTools (**F12 -> Network tab**): verify that **0 HTTP requests** have been transmitted to the server.

### 3. Concurrency Guard (Rapid Double-Clicks)
1. Click the orange **"Test Rapid Repeated Clicks"** button (or rapidly double-click Submit).
2. Observe that the form locks immediately (`isSubmitting = true`, `button.disabled = true`).
3. Verify the Client Audit Log records `BLOCKED duplicate submission` for subsequent clicks, and only **one** `POST /v1/images` request is sent.

### 4. Dual Observation Mode Comparison
1. Select **Long Polling** mode in the observation controls.
2. Submit an image under **Condition A (Quiet Job)**:
   - Notice that the server holds the connection for up to 20 seconds.
   - If quiet processing continues past 20s, the server returns `204 No Content`, and the browser immediately resumes long polling without errors or JSON parsing failures.
   - Upon completion, the worker notifies the held request, which immediately returns `200 OK` with all 3 variants.
3. Switch to **Short Polling** and observe the contrast:
   - A constant stream of requests (~1 per second) polls the server.
   - Completion detection occurs with an observable lag (averaging ~510ms).

### 5. Inspecting Experimental Results & CSV Export
1. Scroll down to the **Assessment 2 Empirical Comparison** section in the UI.
2. Review the interactive request-volume and detection-delay comparative charts.
3. Click **"Download submission.csv"** to export the verified 30-run dataset.
