# Asynchronous Image Processing Pipeline
## Week 1 Check-In: Acceptance Foundation

This project establishes the **acceptance foundation** for an asynchronous, high-throughput image processing pipeline built with Go, PostgreSQL 16, and Vanilla Web technologies. 

The primary objective is to prove and enforce the boundary between selecting an image, uploading it, and accepting responsibility for work, guaranteeing bounded HTTP acknowledgment times (<25ms) while deferring CPU-heavy image transformations to Week 2 background worker execution.

---

## Architecture & Boundary Responsibilities

```
+-----------------------------------------------------------------------------------------------+
|                                    WEEK 1 ACCEPTANCE BOUNDARY                                 |
+-----------------------------------------------------------------------------------------------+
|                                                                                               |
|  [ Browser Client ]                                                                           |
|       │                                                                                       |
|       │ 1. User picks file (JPEG/PNG <= 10MB)                                                 |
|       ├─> Local Blob Preview via URL.createObjectURL (0 bytes sent, 0 DB rows created)        |
|       │                                                                                       |
|       │ 2. Submit Clicked                                                                     |
|       ├─> Concurrency Guard engaged: isSubmitting = true, button.disabled = true              |
|       │                                                                                       |
|       │ 3. Multipart POST /v1/images                                                          |
|       ▼                                                                                       |
|  [ Go API Server ]                                                                            |
|       │ 4. http.MaxBytesReader(10MB) protects against memory exhaustion                       |
|       │ 5. Sniff first 512 bytes with http.DetectContentType (strictly image/jpeg, image/png) |
|       │ 6. Generate collision-free server-controlled filename (UUIDv7 + canonical extension)   |
|       │                                                                                       |
|       ├─────────────────────────────────┬─────────────────────────────────┐                   |
|       │ 7. Write raw file to disk       │ 8. INSERT INTO images           │ 9. INSERT INTO    |
|       ▼                                 ▼    (metadata, server path)      ▼    jobs ('queued')|
|  [ Filesystem Storage ]          [ PostgreSQL Store ]              [ PostgreSQL Store ]       |
|  ./storage/uploads/<uuid>.jpg                                                                 |
|                                                                                               |
|       │                                                                                       |
|       │ 10. File durably on disk & Job committed to DB                                        |
|       ▼                                                                                       |
|  [ HTTP 202 Accepted Response ] ───────────────────────────────────────────────────────────┐  |
|       Location: /v1/jobs/<public_id>                                                       │  |
|       Acknowledgment Time: < 25 ms                                                         │  |
|                                                                                            ▼  |
+───────────────────────────────────────────────────────────────────────────────────────────────+
|                                  WEEK 2 DECOUPLED EXECUTION                                   |
|                                                                                               |
|  [ Background Worker Goroutine ]                                                              |
|       │ Claims queued job via FOR UPDATE SKIP LOCKED                                          |
|       │ Reads raw original from ./storage/uploads/                                            |
|       │ Executes transformations (resizing, thumbnailing, WebP encoding)                      |
|       │ Writes variants to ./storage/variants/ & updates DB status = 'completed'              |
+-----------------------------------------------------------------------------------------------+
```

---

## Quick Setup & Execution Procedure

### 1. Prerequisites
- **Go 1.23+** (`go version`)
- **Docker & Docker Compose** (for PostgreSQL 16 container) OR native PostgreSQL on port 5432
- **migrate CLI** (`migrate -version`)

### 2. Start PostgreSQL
```bash
# Start PostgreSQL 16 container in background
docker-compose up -d
```

### 3. Run Database Migrations
Option A: Using the `migrate` CLI:
```bash
migrate -path=./migrations -database="postgres://postgres:postgres@localhost:5432/image_db?sslmode=disable" up
```

Option B: Applying `tables.sql` directly via psql:
```bash
docker exec -i image-api-postgres psql -U postgres -d image_db < tables.sql
```

### 4. Run the Go API Server
```bash
go run ./cmd/api
```
The server will boot and bind to `http://localhost:4000/`.

### 5. Access the Web Developer Console
Open your browser and navigate to:
```
http://localhost:4000/
```

---

## API Endpoints

| Method | Endpoint | Description | Response Code |
|---|---|---|---|
| `GET` | `/` | Web Acceptance Dashboard & Inspector | `200 OK` (HTML) |
| `GET` | `/v1/healthcheck` | Server health check & environment info | `200 OK` (JSON) |
| `POST` | `/v1/images` | Accept image upload, store raw file, enqueue job | `202 Accepted` (JSON) |
| `GET` | `/v1/images/:id` | Get image metadata & variant records | `200 OK` (JSON) |
| `GET` | `/v1/jobs/:id` | Inspect job state by public_id or UUID | `200 OK` (JSON) |

---

## Verification & Demonstration Guide

### 1. Initial State
- Open `http://localhost:4000/`.
- Verify the page displays the **State: Initial** badge.
- No active image preview, no active jobs, no results, and zero background polling requests are active.

### 2. Image Selection & Local Preview (Zero Network Requests)
- Click or drag & drop a JPEG or PNG file into the dropzone.
- Open DevTools (**F12 -> Network tab**).
- **Observation:**
  - A local preview thumbnail immediately appears.
  - File metadata (filename, formatted size, client MIME) is rendered.
  - The banner explicitly confirms: *0 network requests sent. No database job created.*
  - The Network tab shows **0 requests** fired.

### 3. Concurrency Guard Demonstration (Rapid Double-Clicks)
- While an image is selected, click the orange **"Test Rapid Repeated Clicks"** button (or rapidly click Submit multiple times).
- **Observation:**
  - The `isSubmitting` boolean flag and `submitBtn.disabled = true` lock the form immediately upon the first click.
  - Subsequent clicks within the in-flight window are immediately blocked and logged in the Client Audit Log as `BLOCKED duplicate submission`.
  - Only **1 single HTTP POST request** is dispatched to the server.

### 4. Successful Ingestion & Server-Controlled Storage
- Click **Submit to Ingestion API**.
- **Observation:**
  - The server responds with `HTTP 202 Accepted` in < 25ms.
  - The response contains a `Location: /v1/jobs/<public_id>` header.
  - The original image is saved to disk at `./storage/uploads/<uuid>.jpg` or `./storage/uploads/<uuid>.png` (server-controlled filename; user filename is never used on the filesystem).
  - The database records the image in `images` and enqueues a job with status `'queued'`.
  - **Week 1 Gate Verification:** No image resizing or transformation takes place in the upload handler.

### 5. Unsupported Input Rejection (Server-Side Magic-Byte Gate)
- Attempt to upload a text file, PDF, or disguised script renamed to `.jpg`.
- **Observation:**
  - The server sniffs the first 512 bytes via `http.DetectContentType`.
  - The request is authoritatively rejected with `422 Unprocessable Entity`: `"unsupported file type; only image/jpeg and image/png are accepted"`.
  - Zero files are written to disk; zero rows are created in the database.

### 6. Oversized Input Rejection (> 10 MB)
- Attempt to upload a file exceeding 10 MB.
- **Observation:**
  - The server enforces `http.MaxBytesReader(10MB)` and returns `413 Request Entity Too Large`.

---

## Automated Unit & Integration Tests

Run the test suite:
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
PASS
ok  	image-api/cmd/api
```

---

## Questions You Must Be Able to Answer

### Q1: Where does the selected file exist before submission?
**Answer:** The file exists exclusively on the client machine's local disk/filesystem, held in browser process sandbox memory as a DOM `File`/`Blob` handle. The local preview is rendered via `URL.createObjectURL(file)`. Zero bytes have traveled across the network, no HTTP request has been sent, and no database or filesystem resource exists on the server.

### Q2: What work must finish before the server can safely return 202?
**Answer:** Before returning `202 Accepted`, the server must:
1. Parse the multipart payload within memory limits (`MaxBytesReader` 10MB).
2. Authoritatively validate the binary magic bytes (`http.DetectContentType`) to guarantee genuine JPEG or PNG.
3. Generate a secure, collision-free, server-controlled filename (UUIDv7 + canonical extension).
4. Stream and flush the original bytes safely to server disk (`./storage/uploads/`).
5. Persist the image record in the `images` table in PostgreSQL.
6. Enqueue the asynchronous job in the `jobs` table in PostgreSQL with initial status `'queued'`.
7. Set the `Location: /v1/jobs/<public_id>` header.
*Week 1 Gate reminder:* Image transformation must NOT run before returning 202.

### Q3: Why is the browser's file-type check not authoritative?
**Answer:** The client is untrusted and user-controlled. File extensions (e.g. `malware.sh` -> `malware.jpg`) and client-declared MIME headers can be trivially manipulated or forged using `curl`, Postman, or custom HTTP clients. The server acts as an authoritative gatekeeper by reading the file's raw binary magic bytes and enforcing size limits independently of client headers.

### Q4: What does the disabled button solve, and what duplicate-request problem does it not solve?
**Answer:**
- **What it solves:** It prevents user double-clicks and overlapping concurrent POST dispatches from the *same active page session* while an initial request is in flight.
- **What it does NOT solve:** It cannot protect against multiple browser tabs/windows opened by the same user, transparent network proxy retransmissions/retries, page reloads, or external non-browser scripts. To eliminate cross-request duplicate submissions, the backend requires Idempotency Keys or Content-Addressable Hashing (e.g., SHA-256 deduplication).
