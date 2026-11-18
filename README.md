# ImageLab: Asynchronous Image Processing Pipeline & Observation Suite

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql)](https://www.postgresql.org)
[![Architecture](https://img.shields.io/badge/Architecture-Asynchronous%20Decoupled-orange)]()
[![Observation](https://img.shields.io/badge/Observation-SSE%20%7C%20Long%20Polling%20%7C%20Short%20Polling-green)]()

ImageLab is a high-throughput, asynchronous image processing platform and empirical observation laboratory built with Go, PostgreSQL 16, and vanilla web technologies.

The platform guarantees bounded, sub-25 ms HTTP acceptance times for image ingestion, defers CPU-intensive variant transformations to an isolated background worker goroutine, and provides a **three-mode observation suite** comparing **Server-Sent Events (SSE)**, **version-based long polling** (`wait=20s`), and **1-second short polling** — all backed by an in-process pub/sub event bus with the Counter + Version synchronization pattern.

> **Assessment 3** extends the Version 2 application by adding SSE as a third observation mode and running a controlled 18-run paired experiment (SSE vs. long polling).

---

## Table of Contents

1. [Architecture & System Flow](#architecture--system-flow)
2. [Evolutionary Milestones](#evolutionary-milestones)
3. [Component Responsibilities](#component-responsibilities)
4. [Persistent Versioning & Progress Model](#persistent-versioning--progress-model)
5. [HTTP Observation Contract](#http-observation-contract)
6. [SSE Endpoint & Event Contract](#sse-endpoint--event-contract)
7. [Synchronization & Notification Correctness](#synchronization--notification-correctness)
8. [Image Transformation & Variant Specifications](#image-transformation--variant-specifications)
9. [API Reference](#api-reference)
10. [Empirical Study — SSE vs. Long Polling](#empirical-study--sse-vs-long-polling)
11. [Architectural Defence Q&A](#architectural-defence-qa)
12. [Quick Setup & Execution Guide](#quick-setup--execution-guide)
13. [Running the Experiment](#running-the-experiment)
14. [Verification & Demonstration Walkthrough](#verification--demonstration-walkthrough)

---

## Architecture & System Flow

```
+----------------------------------------------------------------------------------------------------------+
|                                   IMAGELAB COMPLETE SYSTEM ARCHITECTURE                                  |
+----------------------------------------------------------------------------------------------------------+
|                                                                                                          |
|  [ Browser Client Tier ] (ui/index.html)                                                                 |
|       │                                                                                                  |
|       ├─> 1. User picks JPEG/PNG (<=10MB) -> URL.createObjectURL creates instantaneous local preview     |
|       │      (0 bytes transmitted, 0 database records created)                                           |
|       │                                                                                                  |
|       ├─> 2. Submit Clicked -> Concurrency Guard locks form (isSubmitting = true, button.disabled=true)  |
|       │      Rapid repeated clicks are blocked client-side; only 1 multipart POST is dispatched         |
|       │                                                                                                  |
|       │   3. Multipart POST /v1/images (optional condition / latency controls)                           |
|       ▼                                                                                                  |
|  [ Go API Ingestion Tier ] (cmd/api/images_handlers.go)                                                  |
|       │   4. http.MaxBytesReader(10MB) bounds upload payload memory                                      |
|       │   5. http.DetectContentType sniffs binary magic bytes (strictly image/jpeg or image/png)         |
|       │   6. Generates collision-free server filename (UUIDv7 + canonical extension)                     |
|       │   7. Streams original payload durably to disk: ./storage/uploads/<uuid>.<ext>                    |
|       │   8. Commits Image record (UUID, metadata) and initial Job record ('queued', version=1) to DB    |
|       ▼                                                                                                  |
|  [ HTTP 202 Accepted Response ] (<25ms acknowledgment SLA)                                               |
|       Location: /v1/jobs/<public_id>                                                                     |
|                                                                                                          |
|  ─────────────────────── Observation Split ────────────────────────────────────                          |
|                                                                                                          |
|  [ SSE Observer ]                      [ Long-Poll Observer ]  [ Short-Poll Observer ]                   |
|  GET /v1/jobs/:id/events               GET /v1/jobs/:id        GET /v1/jobs/:id                          |
|  EventSource (single stream)             ?after=v&wait=20s       (1-second loop)                         |
|  ─ Initial snapshot on connect         ─ Holds until version   ─ Polls every 1s                          |
|  ─ job-update events on commit           change or 20s timeout                                           |
|  ─ Heartbeat comments (:) on idle      ─ 204 on timeout        ─ 200 on change                           |
|  ─ Closes stream on terminal state     ─ 200 on change                                                   |
|       │                                                                                                  |
|       └────────────────── All modes read PostgreSQL as authority ──────────────────────                  |
|                                                                                                          |
|  [ In-Process Pub/Sub — JobNotifier ] (cmd/api/notifier.go)                                              |
|       Notify(jobID) published only after DB transaction commit (SYNC-01)                                 |
|       Broadcast wakes every subscriber channel; each handler re-reads PostgreSQL (SYNC-04)               |
|       TrackStreamOpen / TrackStreamClose / ActiveStreams / PeakActiveStreams (MEAS-05)                   |
|                                                                                                          |
|  [ Background Worker Goroutine ] (cmd/api/worker.go)                                                     |
|       Dequeues jobs, runs image transforms, persists variants, commits version increments                |
|       Worker termination is NOT linked to any observer lifecycle (SYNC-07)                               |
|                                                                                                          |
+----------------------------------------------------------------------------------------------------------+
```

### SSE Stream Lifecycle (single connection)

```
Client                                   Server (getJobEventsHandler)
  │                                            │
  │── GET /v1/jobs/:id/events ────────────────>│
  │                                            │── Subscribe(jobID)              [SYNC-03]
  │                                            │── fetchJob()  (DB read)
  │<── 200 text/event-stream ─────────────────│── Set SSE headers               [SSE-03]
  │                                            │
  │<── event: job-update                       │── Send initial snapshot         [SSE-04]
  │    data: { version:1, status:"queued" }    │── Flush                         [SSE-06]
  │                                            │
  │      ┄ (worker commits thumbnail) ┄        │
  │                                            │── Notify() fires                [SYNC-01]
  │                                            │── fetchJob() re-reads DB        [SYNC-04]
  │<── event: job-update                       │── Version advanced → send frame [SSE-05]
  │    data: { version:3, stage:"thumbnail_ready" }                              [SSE-06]
  │                                            │
  │       ┄ (15 s idle) ┄                      │── Heartbeat ticker fires
  │<── : heartbeat                             │── Flush                         [SSE-08]
  │                                            │
  │      ┄ (worker commits completed) ┄        │
  │<── event: job-update                       │── Terminal snapshot sent        [SSE-07]
  │    data: { version:5, status:"completed" } │── Handler returns, stream ends
  │                                            │── Unsubscribe()                 [SYNC-07]
```

---

## Evolutionary Milestones

| Phase | Version | Key addition |
|-------|---------|--------------|
| Phase 1 | Version 1 | POST /v1/images, durable job, local storage, one background worker, four job states, three variants, short polling |
| Phase 2 | Version 2 | Persistent versioning (`version`, `stage`, `variants_completed`), version-based long polling (`?after=v&wait=20s`), Counter + Version synchronization (WAIT-01–03), 204 on timeout, empirical short-polling vs. long-polling study |
| **Phase 3** | **Version 3** | **SSE endpoint `GET /v1/jobs/:id/events`, named `job-update` frames, heartbeat comments, terminal stream close, disconnect-safe worker, stream telemetry (MEAS-05), SSE vs. long-polling experiment (18 runs)** |

---

## Component Responsibilities

| File | Responsibility |
|------|----------------|
| `cmd/api/main.go` | Application wiring, server config, `WriteTimeout: 0` for persistent streams |
| `cmd/api/routes.go` | Route table: SSE route registered before job GET route |
| `cmd/api/images_handlers.go` | POST /v1/images — upload ingestion, file storage, job creation |
| `cmd/api/jobs_handlers.go` | GET /v1/jobs/:id (prompt + long poll) and GET /v1/jobs/:id/events (SSE) |
| `cmd/api/worker.go` | Background goroutine: dequeues, transforms, persists variants, notifies |
| `cmd/api/notifier.go` | In-process pub/sub; `JobNotifier` with stream telemetry tracking |
| `internal/data/jobs.go` | PostgreSQL data layer: Get, GetByPublicID, Update |
| `internal/data/variants.go` | Variant persistence and retrieval |
| `internal/transform/transform.go` | Image resizing / format conversion for thumbnail, preview, display |
| `ui/index.html` | Single-file SPA: 3-mode selector, SSE engine, Lab view, Charts, Dataset |
| `scripts/run_experiment.py` | Automated 18-run experiment runner (no external Python deps) |
| `data/submission.csv` | Live 18-run empirical dataset (MEAS-05 schema, 20 columns) |

---

## Persistent Versioning & Progress Model

Every client-visible state change increments `version` **inside the same database transaction** (JOB-03). The worker publishes an in-process notification only after the transaction commits (SYNC-01). This eliminates the read-your-writes race condition.

| version | Status | Stage | variants_completed | Event |
|---------|--------|-------|-------------------|-------|
| 1 | queued | — | 0 | Job created |
| 2 | processing | processing | 0 | Worker picks up job |
| 3 | processing | thumbnail_ready | 1 | Thumbnail stored & committed |
| 4 | processing | preview_ready | 2 | Preview stored & committed |
| 5 | completed | display_ready | 3 | Display stored; job terminal |

Simulated-progress conditions (Condition C) add intermediate versions between version 2 and the first real variant commit. These are labelled `simulated_progress` in the JSON snapshot and are never counted against `variants_completed` (JOB-05).

---

## HTTP Observation Contract

### Short Polling — GET /v1/jobs/:id
Issues one unconditional DB read per request. Browser repeats at 1-second interval. Simple but generates continuous traffic regardless of job activity.

### Long Polling — GET /v1/jobs/:id?after={v}&wait={s}
Holds the response open until:
- `job.Version > after` → 200 with current snapshot
- `job.Status ∈ {completed, failed}` → 200 with terminal snapshot
- Timeout expires with no change → 204 No Content (browser immediately retries)

The subscribe-before-read ordering (WAIT-01) prevents the race between reading version=N and waiting for N+1.

### SSE — GET /v1/jobs/:id/events
See [SSE Endpoint & Event Contract](#sse-endpoint--event-contract) below.

---

## SSE Endpoint & Event Contract

### Route

```
GET /v1/jobs/:id/events
```

The `:id` parameter accepts both the client-facing `public_id` (UUIDv4) and the internal `id` (UUIDv7). Unknown jobs return **404 before** a stream begins (SSE-02).

### Response Headers (SSE-03)

```
HTTP/1.1 200 OK
Content-Type: text/event-stream; charset=utf-8
Cache-Control: no-cache
Connection: keep-alive
X-Accel-Buffering: no
```

The server-level `WriteTimeout` is set to `0` so long-lived streams are not prematurely closed.

### Event Frame Shape (SSE-04, SSE-05, SSE-06)

```
event: job-update
data: {"id":"…","status":"processing","version":3,"stage":"thumbnail_ready","variants_completed":1,…}

```

Each frame is a **named event** (`event: job-update`) carrying a **complete current snapshot** in `data:`, terminated by a blank line, and immediately flushed. No `id:` field is emitted — this implementation does not support Last-Event-ID replay (SSE-09).

### Heartbeat Comments (SSE-08)

On idle streams (no job-update event in the last 15 seconds), the server sends:

```
: heartbeat

```

Heartbeat comments do not change job version and are not `job-update` events. The browser's `EventSource` ignores them. The experiment runner counts them separately (column `heartbeats`).

### Terminal Behaviour (SSE-07)

After sending a `completed` or `failed` snapshot, the server handler returns, ending the response. The browser's `EventSource` fires `onerror` → the UI checks the last-received status and calls `eventSource.close()` to prevent reconnection on a finished job.

### Reconnect Behaviour

If the network drops mid-stream, the browser's `EventSource` automatically reconnects. The new GET is handled identically to the initial connect: subscribe-before-read, then immediately send the **current snapshot** (which may be newer than what was last received). No event-history replay is needed because every frame carries a complete state snapshot (SSE-05).

### Stop Observing

The UI provides a **Stop Observation** button that calls `eventSource.close()`. The request context is cancelled server-side: the SSE handler goroutine returns (SYNC-07), `unsubscribe()` is deferred, and stream telemetry is updated. The background worker goroutine and PostgreSQL job are **completely unaffected** — they continue to completion.

---

## Synchronization & Notification Correctness

| ID | Requirement | Implementation |
|----|-------------|----------------|
| SYNC-01 | Notify only after DB commit | `a.notifier.Notify()` called in worker after `tx.Commit()` succeeds |
| SYNC-02 | Broadcast to all active observers | `Notify()` iterates all subscriber channels for the job ID |
| SYNC-03 | Subscribe before read | SSE handler calls `Subscribe()` before the first `fetchJob()` |
| SYNC-04 | Notification is a signal to re-read DB | On `<-ch`, handler calls `fetchJob()` and uses that result |
| SYNC-05 | Only send if newer | `job.Version > lastSentVersion` guards the frame write |
| SYNC-06 | No missed wakeup | Channel is buffered (size 1); a notification while handling a previous one is not lost |
| SYNC-07 | Worker independent of observer | `r.Context().Done()` only exits the SSE handler goroutine |

---

## Image Transformation & Variant Specifications

| Variant | Dimensions | Format | Use |
|---------|-----------|--------|-----|
| thumbnail | 150 × 150 | JPEG 85 | Grid / avatar |
| preview | 400 × 300 | JPEG 85 | Card / modal |
| display | 1200 × 900 | PNG lossless | Full-page viewer |

All-or-nothing availability: variants are exposed in the job snapshot only after `status = completed`. Partial variants are never served (JOB-04).

---

## API Reference

### POST /v1/images

Upload a JPEG or PNG image (≤ 10 MB) for asynchronous processing.

**Request:** `multipart/form-data` with field `image`.  
**Response:** `202 Accepted` — `Location: /v1/jobs/<public_id>`

```json
{
  "job": {
    "id": "b7e1c2d3-…",
    "status": "queued",
    "version": 1
  }
}
```

Optional query params: `condition=A|B|C` (experiment condition), `latency=<duration>`.

---

### GET /v1/jobs/:id

Prompt snapshot or versioned long poll.

| Query params | Behaviour |
|---|---|
| (none) | Immediate 200 with current job snapshot |
| `after=N` | Hold until version > N or timeout; `wait` defaults to 20 |
| `after=N&wait=S` | Hold for up to S seconds (1–20) |

**Responses:** `200 OK` with job envelope, or `204 No Content` on timeout.

---

### GET /v1/jobs/:id/events ⟵ **NEW in Version 3**

Open a Server-Sent Events stream for real-time job observation.

**Response:** `200 text/event-stream` — stream of `job-update` events.

```
event: job-update
data: { "id": "…", "status": "queued", "version": 1, … }

event: job-update
data: { "id": "…", "status": "processing", "version": 2, … }

: heartbeat

event: job-update
data: { "id": "…", "status": "completed", "version": 5, "variants": […] }

```

Stream ends immediately after the terminal snapshot.

**Error responses (before stream begins):**
- `404 Not Found` — unknown job ID
- `400 Bad Request` — invalid UUID

---

### GET /v1/images/:image_id/variants/:name

Serve a stored image variant (`thumbnail`, `preview`, `display`). Returns 404 until `status = completed`.

---

### GET /v1/healthcheck

```json
{ "status": "available", "system_info": { "environment": "development", "version": "1.0.0" } }
```

---

## Empirical Study — SSE vs. Long Polling

### Experiment Design

| Dimension | Value |
|-----------|-------|
| Methods compared | SSE vs. version-based long polling |
| Conditions | A (30 s quiet), B (real processing), C (200 ms simulated updates × 50 steps) |
| Repetitions | 3 per method per condition |
| Total runs | 18 |
| Input image | `sample_valid.jpg` (same file, same transformation settings) |
| Concurrency | 1 observer per run; empty worker queue at start |
| Method order | Alternating (long, sse, sse, long, long, sse per condition) |

### Predictions (stated before each condition)

**Condition A — Quiet interval:**  
Long polling will issue ~1–2 "empty" 204 responses per 20 s timeout before the job completes. SSE will hold one stream open and send heartbeat comments silently. Both methods should detect completion within tens of milliseconds of the commit.

**Condition B — Real processing:**  
Long polling will issue one held request per commit milestone (~4 total for version changes). SSE will push all milestones over one stream. Detection latency should be comparable because both re-read PostgreSQL immediately on notification.

**Condition C — Frequent updates (200 ms, 50 steps):**  
Long polling will generate one HTTP round-trip per version increment (~55 total). SSE will carry all 55 increments in one stream. Version gaps are possible for long polling if round-trip latency exceeds 200 ms, but are unlikely on localhost.

---

### Run-by-Run Measurement Table

| run_id | cond | mode | rep | baseline_v | terminal_v | obs_GETs | sse_streams | events | heartbeats | versions_rx | v_gaps | proc_ms | detect_ms | stream_s |
|--------|------|------|-----|-----------|-----------|---------|------------|--------|------------|------------|--------|---------|-----------|---------|
| RUN-01 | A | long | 1 | 1 | 5 | 6 | 0 | 0 | 0 | 5 | 0 | 30 495 | 11 | — |
| RUN-02 | A | sse  | 1 | 1 | 5 | 1 | 1 | 5 | 2 | 5 | 0 | 30 497 | 12 | 30.66 |
| RUN-03 | A | sse  | 2 | 1 | 5 | 1 | 1 | 5 | 2 | 5 | 0 | 30 498 |  9 | 30.68 |
| RUN-04 | A | long | 2 | 1 | 5 | 6 | 0 | 0 | 0 | 5 | 0 | 30 494 | 11 | — |
| RUN-05 | A | long | 3 | 1 | 5 | 6 | 0 | 0 | 0 | 5 | 0 | 30 488 |  8 | — |
| RUN-06 | A | sse  | 3 | 1 | 5 | 1 | 1 | 5 | 2 | 5 | 0 | 30 489 | 10 | 30.68 |
| RUN-07 | B | long | 1 | 1 | 5 | 5 | 0 | 0 | 0 | 5 | 0 |   500  | 11 | — |
| RUN-08 | B | sse  | 1 | 1 | 5 | 1 | 1 | 5 | 0 | 5 | 0 |   490  |  7 |  0.66 |
| RUN-09 | B | sse  | 2 | 1 | 5 | 1 | 1 | 5 | 0 | 5 | 0 |   491  | 11 |  0.69 |
| RUN-10 | B | long | 2 | 1 | 5 | 5 | 0 | 0 | 0 | 5 | 0 |   493  | 10 | — |
| RUN-11 | B | long | 3 | 1 | 5 | 5 | 0 | 0 | 0 | 5 | 0 |   490  | 17 | — |
| RUN-12 | B | sse  | 3 | 1 | 5 | 1 | 1 | 5 | 0 | 5 | 0 |   490  | 10 |  0.67 |
| RUN-13 | C | long | 1 | 1 | 55 | 55 | 0 | 0 | 0 | 55 | 0 | 10 743 |  9 | — |
| RUN-14 | C | sse  | 1 | 1 | 55 |  1 | 1 | 55 | 0 | 55 | 0 | 10 745 |  9 | 10.89 |
| RUN-15 | C | sse  | 2 | 1 | 55 |  1 | 1 | 55 | 0 | 55 | 0 | 10 741 |  8 | 10.86 |
| RUN-16 | C | long | 2 | 1 | 55 | 55 | 0 | 0 | 55 | 0 | 10 830 |  9 | — |
| RUN-17 | C | long | 3 | 1 | 55 | 55 | 0 | 0 | 55 | 0 | 10 774 | 12 | — |
| RUN-18 | C | sse  | 3 | 1 | 55 |  1 | 1 | 55 | 0 | 55 | 0 | 10 789 |  7 | 10.90 |

> **Measurement method:** `detection_ms` = `browser_receipt_time − server log time immediately after terminal transaction commit`. Both timestamps recorded in milliseconds; difference computed by the experiment runner (Python `urllib`, `time.time()`).

---

### Summary Table — Median [Min – Max]

| Condition | Mode | Obs GETs (median) | Detect ms (median) | Stream s (median) |
|-----------|------|-------------------|-------------------|-------------------|
| A – Quiet 30 s | Long polling | 6 [6–6] | 10 [8–11] | — |
| A – Quiet 30 s | SSE | 1 [1–1] | 10 [9–12] | 30.68 [30.66–30.68] |
| B – Real processing | Long polling | 5 [5–5] | 11 [10–17] | — |
| B – Real processing | SSE | 1 [1–1] | 10 [7–11] | 0.67 [0.66–0.69] |
| C – Frequent 200 ms | Long polling | 55 [55–55] | 10 [9–12] | — |
| C – Frequent 200 ms | SSE | 1 [1–1] | 8 [7–9] | 10.88 [10.86–10.90] |

---

### Graph 1 — Observation GET Count by Condition and Method

```
Obs GETs
60 │                                       ● 55  ● 55  ● 55
   │
   │
   │
30 │
   │
   │
 6 │  ● 6   ● 6   ● 6   ● 5   ● 5   ● 5
   │
 1 │             ○ 1 ○ 1 ○ 1       ○ 1 ○ 1 ○ 1       ○ 1 ○ 1 ○ 1
   └─────────────────────────────────────────────────────────────────
       Cond A                Cond B                Cond C
       ● Long Polling  ○ SSE
```

**Interpretation:** SSE consistently required exactly **1 observation GET** (the stream open) for every condition and repetition, while long polling required 6 GETs in Condition A (1 × 20 s timeout expiry + 5 version-change responses) and 55 GETs in Condition C (one per 200 ms update). The request count difference does **not** automatically imply lower total resource use: each SSE stream holds an open file descriptor and an active handler goroutine for its entire duration. In Condition A, the SSE handler was held open for ~30.7 s and sent 2 heartbeat frames; long polling held one connection for 20 s (the 204 timeout) and then immediately responded on completion. The long-polling 204 connection closed and freed its resources; the SSE handler kept its goroutine and subscriber slot throughout. Condition C shows the starkest contrast: 55 HTTP round-trips with connection teardown/setup overhead versus 1 open stream receiving 55 events over 10.9 s.

---

### Graph 2 — Completion-Detection Delay by Condition and Method

```
Detect ms
20 │                                               ● 17
   │
15 │
   │
10 │  ● 11  ● 11  ● 8    ● 11  ● 10  ● 17  ○ 9   ● 10  ● 12  ○ 8   ○ 9
   │  ○ 12  ○ 9   ○ 10   ○ 7   ○ 11  ○ 10
 5 │
   │
 0 │
   └─────────────────────────────────────────────────────────────────────
       Cond A                   Cond B                  Cond C
       Median A-long=10ms  A-sse=10ms  B-long=11ms  B-sse=10ms  C-long=10ms  C-sse=8ms
       ● Long Polling (individual reps)  ○ SSE (individual reps)
```

**Interpretation:** Detection latency was **comparable** between SSE and long polling across all three conditions (medians within 2 ms of each other). Both methods share the same fundamental pipeline: the worker commits a DB transaction → notifier fires → handler re-reads PostgreSQL → sends the frame. The re-read latency dominates both methods equally. SSE had no reconnect overhead (the stream was already open), while long polling had no additional connection setup overhead (keep-alive reuse on localhost). The outlier at 17 ms (RUN-11, B-long, rep 3) is within normal jitter for a single localhost DB round-trip. The key takeaway from this graph: **SSE's advantage in Condition C is in request count, not in detection latency** — both methods are fast enough for this workload.

---

### Version-Over-Time Trace — Condition C, RUN-14 (SSE)

```
Version
55 │                                               ●
   │                                          ●●●
   │                                     ●●●●
   │                                ●●●●
30 │                           ●●●●
   │                      ●●●●
   │                 ●●●●
   │            ●●●●
 1 │  ●●●●●●●●
   └───────────────────────────────────────────────
       0 s                5 s               10.9 s
                          ↑ 200 ms updates (simulated progress)
                          Versions 2–52 arrive at ~200 ms intervals
                          Versions 53/54/55 = real variants (thumbnail/preview/display)
```

**Interpretation:** Each SSE `job-update` event in Condition C carries the current version number, making the version progression directly observable. The trace is nearly linear (constant 200 ms cadence), confirming that the SSE handler processed every notification without coalescing on localhost. Long polling produced the same version coverage (55 distinct versions received, 0 gaps) because the 200 ms update interval was longer than the local HTTP round-trip (~1–2 ms), so each poll returned the next sequential version.

---

### Analysis & Recommendation

**Separating processing time from detection delay:**  
Processing duration (worker `started_at` → terminal commit) was 30.5 s for Condition A, ~0.5 s for Condition B, and ~10.8 s for Condition C. Detection delay (commit → browser receipt) was 7–17 ms across all 18 runs and all methods. Processing time and detection delay are orthogonal: a slow job does not cause slow detection.

**Version gaps:**  
Zero version gaps were observed for either method in any condition. This is expected on localhost: the SSE stream is always open, and the long-poll round-trip is fast enough to catch each sequential version. In a higher-latency environment (e.g., 100 ms RTT), Condition C long polling would begin to show gaps as the 200 ms update interval approaches the total round-trip time.

**Resource trade-offs:**  
- SSE holds one goroutine, one subscriber slot, and one HTTP connection per observer for the full job duration. It never performs redundant DB reads.  
- Long polling holds a goroutine only during the wait window and releases it on 204. It performs one DB read per notification (same as SSE) plus one authoritative re-read on timeout. It issues more HTTP requests, each of which involves TCP/TLS framing overhead (mitigated by connection reuse on HTTP/1.1 keep-alive).  
- Peak active SSE streams was always 1 in single-observer runs. In a production multi-client scenario, each concurrent observer adds one persistent goroutine for SSE but only brief goroutine hold times for long polling.

**Recommendation:**  
SSE is preferable when updates are **frequent** (Condition C pattern) or when **stream duration is short** (Condition B: ~0.7 s). Its request count advantage grows proportionally with update frequency. Long polling remains a viable alternative when updates are **infrequent** (Condition A: 1 × 204 expiry + 1 × completion = 6 GETs vs. SSE's 1 stream open + 2 heartbeats + 5 events), is more compatible with proxies and firewalls that buffer or time-out streaming responses, and requires no persistent connection management. For this classroom workload, **either method achieves sub-20 ms detection latency**; the choice should be based on update frequency, infrastructure constraints, and client-side complexity rather than latency alone.

**Limitations:**  
All 18 runs were conducted on localhost. Network RTT, proxy buffering, and connection limits were not factors. Results should not be extrapolated to production-scale or multi-server deployments. The experiment used one observer per run; concurrent-observer behaviour (resource contention, notification fan-out latency) was not measured.

---

## Architectural Defence Q&A

**Q: What does the server keep open, and what does Flush do?**  
A: For SSE, the server keeps the HTTP response body open (the write half of the TCP connection) and holds a handler goroutine in the stream loop. `Flush()` instructs Go's `bufio.Writer` to push buffered bytes to the OS network stack immediately, so the browser's EventSource parser receives the event frame without waiting for the buffer to fill or the response to end.

**Q: How does a committed job version become an SSE event for every connected tab?**  
A: The worker commits the new version in a PostgreSQL transaction, then calls `notifier.Notify(jobID)`. The notifier iterates every subscriber channel for that job ID and sends a signal. Each SSE handler goroutine is blocked on `<-ch`; when woken, it re-reads PostgreSQL to get the authoritative snapshot and writes an `event: job-update` frame to its own response writer, then flushes.

**Q: What happens when a connection drops while the worker is still running?**  
A: The request context (`r.Context()`) is cancelled when the client disconnects. The SSE handler's `select` wakes on `<-r.Context().Done()` and returns, triggering `defer unsubscribe()`. The worker goroutine and the PostgreSQL job record are unaffected (SYNC-07). When the browser's EventSource reconnects, the new GET handler subscribes, reads the current DB snapshot (which may now be at a later version), sends it immediately, and resumes waiting for future notifications.

**Q: Why can a reconnecting client receive the latest snapshot without replaying every intermediate version?**  
A: Every frame already carries a **complete current snapshot**, not a delta. The `data:` field contains the full job representation including the current version number. A reconnecting client that missed versions 3 and 4 receives version 5 and can fully reconstruct the display state from it. This is snapshot semantics: the receiver does not need a chain of deltas to reconstruct state.

**Q: What did the graphs show that was harder to see in the tables?**  
A: Graph 1 made the **magnitude** of the GET-count difference immediately visible — 55 vs. 1 in Condition C is a 55× ratio that is easy to miss when scanning a row of numbers. Graph 2 showed that despite the large request-count difference, detection latency distributions largely **overlap** between methods, reinforcing that the two dimensions are independent. The version-over-time trace showed the **cadence** of Condition C updates, confirming that SSE delivered each 200 ms increment without coalescing.

---

## Quick Setup & Execution Guide

### 1. Prerequisites

- **Go 1.23+** (`go version`)
- **Docker & Docker Compose** (for PostgreSQL 16) or local PostgreSQL on port 5432
- **Python 3.9+** (for the experiment runner; standard library only)

### 2. Start PostgreSQL

```powershell
docker-compose up -d
```

### 3. Apply Database Migrations

**Option A — migrate CLI:**
```bash
migrate -path=./migrations -database="postgres://postgres:postgres@localhost:5432/image_db?sslmode=disable" up
```

**Option B — tables.sql directly:**
```bash
docker exec -i image-api-postgres psql -U postgres -d image_db < tables.sql
```

**Option C — Makefile:**
```bash
make migrate-up
```

### 4. Build and Run the Server

```powershell
go build -ldflags="-s -w" -o ./bin/server.exe ./cmd/api
.\bin\server.exe
```

The server listens on **http://localhost:4000**.

Optional flags:
| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `4000` | API server port |
| `-db-dsn` | `postgres://postgres:postgres@localhost:5432/image_db?sslmode=disable` | PostgreSQL DSN |
| `-processing-delay` | `0` | Extra delay injected into worker per variant |
| `-limiter-enabled` | `true` | Set `false` for benchmarking |

### 5. Open the UI

Navigate to **http://localhost:4000/** in a browser.

- The default observation mode is **Server-Sent Events** (green badge).
- Switch modes using the segmented control: `SSE | Long Polling | Short Polling`.
- Navigate to **Lab** (top nav) to view experiment charts, summary table, and raw CSV.

### 6. Run the Automated Test Suite

```powershell
go build -o ./bin/test.exe -v ./cmd/api
.\bin\test.exe -test.v
```

Expected: all tests pass (some skip if PostgreSQL is unavailable). Tests in `internal/transform` require the compiled `transform.test.exe`.

---

## Running the Experiment

### Mode Selection

The UI mode selector governs which observation method is used for browser-initiated uploads. For the automated experiment runner, the mode is selected per run programmatically.

**Condition selection:**  
Pass `condition=A`, `condition=B`, or `condition=C` as a query parameter on the POST /v1/images request:

| Condition | Behaviour |
|-----------|-----------|
| A | 30-second quiet sleep before real processing begins |
| B | Immediate real image processing (thumbnail → preview → display) |
| C | 50 × 200 ms simulated-progress updates (labelled `simulated_progress`), then real processing |

> **Important:** Simulated-progress updates in Condition C are labelled `simulated_progress` in the JSON snapshot. They are never counted as `variants_completed`. A timer never makes a job `completed`; only successful real processing does (JOB-05).

### Automated Runner

```powershell
python scripts/run_experiment.py
```

The runner:
1. Checks server health (`GET /v1/healthcheck`)
2. Runs 18 alternating-order repetitions (3 conditions × 2 methods × 3 reps)
3. For each run: POSTs the image, starts the observer (SSE or long-poll), joins on terminal state, records all 20 MEAS-05 columns
4. Writes raw logs to `logs/client.log` and `logs/server.err.log`
5. Writes the full dataset to `data/submission.csv`

**Requirements:**
- Server must be running on port 4000
- `sample_valid.jpg` must exist in the workspace root
- No external Python packages needed (`urllib`, `time`, `csv`, `json` only)

### Reproducing a Single Run Manually

**SSE observation:**
```javascript
const es = new EventSource('/v1/jobs/<public_id>/events');
es.addEventListener('job-update', e => {
  const snap = JSON.parse(e.data);
  console.log(snap.version, snap.status);
  if (snap.status === 'completed' || snap.status === 'failed') es.close();
});
```

**Long-poll observation (curl loop):**
```bash
# Initial prompt
curl -s http://localhost:4000/v1/jobs/<public_id>
# Then loop with after=<v>&wait=20
while true; do
  R=$(curl -sw "%{http_code}" http://localhost:4000/v1/jobs/<id>?after=<v>&wait=20)
  # check status code 200 vs 204
done
```

---

## Verification & Demonstration Walkthrough

### 1. SSE Stream in DevTools

1. Open `http://localhost:4000/` and confirm the green **Server-Sent Events** badge.
2. Open DevTools → Network tab.
3. Upload `sample_valid.jpg`.
4. Observe a single `events` request in the Network tab (type: `eventsource`).
5. Click the request → **EventStream** tab: each `job-update` frame appears as it arrives.
6. Verify: initial frame arrives immediately; `thumbnail_ready`, `preview_ready`, `display_ready` frames follow; stream closes after the terminal frame.

### 2. Two-Tab Synchronisation

1. Open two browser tabs on `http://localhost:4000/`, both in SSE mode.
2. Upload in tab 1; copy the `public_id` from the URL and paste into tab 2's job ID field.
3. Start observation in tab 2 — both tabs receive every `job-update` event simultaneously.

### 3. Stop Observing — Worker Continues

1. Upload an image under **Condition A** (30-second quiet period).
2. Wait 5 seconds, then click **Stop Observation**.
3. Confirm the stream closes in DevTools (request ends).
4. Wait for the full 30-second quiet + processing period.
5. Reload the job snapshot: `GET /v1/jobs/<public_id>` — the job is `completed`.

### 4. Reconnect

1. Upload an image, start SSE observation.
2. In DevTools, block the `events` request (right-click → block request URL).
3. The EventSource fires `onerror`; the UI shows "Reconnecting…".
4. Unblock the URL — EventSource reconnects. The new GET returns the latest snapshot.

### 5. Experiment Lab Charts

Navigate to `http://localhost:4000/` → click **Lab** (top navigation).

- **Chart 1:** Observation GET count — SSE bar always = 1; Long Polling bars show condition-dependent counts.
- **Chart 2:** Detection delay — both methods show 7–17 ms range.
- **Chart 3:** Version progression trace for Condition C.
- **Summary table** and **Raw CSV table** show all 18 runs.
- **Download submission.csv** exports the MEAS-05 dataset.

---

*CMPS 4191 — Advanced Web Technologies — Assessment 3 — ImageLab Version 3*
