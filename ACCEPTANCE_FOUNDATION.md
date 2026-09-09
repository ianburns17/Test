# CMPS4191: Advanced Web Development
## Week 1 Check-In: Acceptance Foundation & Architectural Defense

---

### Executive Overview
Week 1 establishes the **acceptance foundation** for the Asynchronous Image Processing Pipeline. The primary architectural objective is to establish and prove the exact boundary between selecting an image, uploading it, and accepting responsibility for work—guaranteeing bounded, sub-30ms HTTP acknowledgment latency while strictly deferring image transformation execution to Week 2.

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

### System Component Responsibilities

| Component | Responsibility in Week 1 | What it Must NOT Do in Week 1 |
|---|---|---|
| **Browser (Client)** | Renders initial, image-selected, and uploading UI states; creates local object preview via `URL.createObjectURL` without network I/O; guards against overlapping dispatches via `isSubmitting` flag and `button.disabled = true`. | Must NOT process or transform image pixels; must NOT assume client file-type check is authoritative; must NOT poll during initial state. |
| **API Server (Go)** | Ingests multipart stream; enforces 10MB ceiling; authoritatively validates MIME type via binary magic bytes; generates server-controlled filename; streams file to disk; writes `images` and `jobs` records; returns `202 Accepted` with `Location` header. | **WEEK 1 GATE**: Must NOT perform image decoding, resizing, compression, or variant transformations in the upload handler. |
| **Database (PostgreSQL)** | Persists relational metadata for `images`, lifecycle state for `jobs` (starting at `'queued'`), and provides schema structure for `image_variants`. | Does NOT store binary image BLOBs directly in table rows. |
| **Worker (Background)** | Dequeuing logic prepared for Week 2 (`FOR UPDATE SKIP LOCKED`). | Decoupled from the HTTP request handler. |
| **Filesystem Storage** | Provides durable, isolated local disk storage (`./storage/uploads/`) under server-controlled filenames. | Does NOT expose raw client filenames directly to directory paths. |

---

### Questions You Must Be Able to Answer

#### 1. Where does the selected file exist before submission?
> **Answer:**
> The selected file exists exclusively on the client machine's local storage device (hard drive/SSD) and is referenced in the browser sandbox process memory as an in-memory DOM `File` / `Blob` object (accessible via the HTML `<input type="file">` element). 
>
> When the browser renders the image preview, it creates an ephemeral client-side object URL (`URL.createObjectURL(file)`) pointing directly to the browser's local memory buffer. At this stage:
> - Zero bytes have crossed the network wire.
> - No HTTP request has been constructed or transmitted.
> - No file exists on the server's filesystem.
> - No database record or job has been created in PostgreSQL.

#### 2. What work must finish before the server can safely return 202?
> **Answer:**
> Before the server can issue `HTTP 202 Accepted`, it must finish the complete **contract of acceptance responsibility**:
> 1. **Size Enforcement**: Ingest the request body under an enforced memory and byte limit (`http.MaxBytesReader` set to 10 MB).
> 2. **Authoritative Magic-Byte Validation**: Read the file header bytes (first 512 bytes) and inspect magic numbers via `http.DetectContentType` to guarantee the payload is genuine `image/jpeg` or `image/png`.
> 3. **Server-Controlled Filename Generation**: Generate an unpredictable, collision-free identifier (UUIDv7) with a canonical extension.
> 4. **Durable Filesystem Persistence**: Safely stream the binary payload to server disk (`./storage/uploads/<uuid>.<ext>`) and verify that file closure and disk write succeeded without I/O error.
> 5. **Durable Database Persistence**: Insert the image record into the `images` table and insert an asynchronous tracking record into the `jobs` table with initial status `'queued'`.
> 6. **Location Header**: Prepare the `Location: /v1/jobs/<public_id>` response header.
>
> Only when the original bytes are durably on disk and the queued state is committed to the database has the server accepted responsibility. Crucially, **image transformation (resizing, filtering) must NOT run** before returning 202; running transformations synchronously violates the decoupled architecture.

#### 3. Why is the browser's file-type check not authoritative?
> **Answer:**
> The browser client is an untrusted, externally controlled execution environment. Any validation performed in the browser can be bypassed, manipulated, or forged:
> - **Extension Forgery**: A user or attacker can rename a malicious executable or shell script from `exploit.sh` to `exploit.jpg`.
> - **Header Spoofing**: An attacker using `curl`, Postman, Python scripts, or custom TCP sockets can emit arbitrary `Content-Type: image/jpeg` multipart headers while transmitting non-image payloads.
> - **Browser API Vulnerabilities**: Client-side MIME sniffing depends on operating system file associations which may be inaccurate or unverified.
>
> Therefore, the server must act as the sole authoritative gatekeeper. It reads the binary file header directly and validates magic numbers (`0xFF 0xD8 0xFF` for JPEG; `0x89 0x50 0x4E 0x47` for PNG). Unsupported or disguised payloads are rejected with `422 Unprocessable Entity` before any database job or persistent storage occurs.

#### 4. What does the disabled button solve, and what duplicate-request problem does it not solve?
> **Answer:**
> **What it solves:**
> The disabled button coupled with an in-memory `isSubmitting` boolean guard solves the **single-session concurrent dispatch race condition**. Specifically, it prevents accidental user double-clicks, impatient rapid clicking, or repeated keypresses from initiating overlapping, concurrent `POST /v1/images` HTTP requests from the *same active browser page* while the initial request is still in flight.
>
> **What it does NOT solve:**
> 1. **Cross-Tab/Cross-Window Duplication**: If the same user opens two tabs or windows and submits the same file in both, each tab maintains an independent DOM and memory state; both buttons are enabled, resulting in duplicate submissions.
> 2. **Network Retries and Retransmissions**: Transparent network proxies, reverse proxies, or client-side retry policies that retransmit dropped TCP packets upon transient timeout will dispatch duplicate POST requests.
> 3. **Page Reloads / Back Navigation**: If the user reloads the page or navigates away and returns, the DOM resets, re-enabling the button.
> 4. **Automated Non-Browser Clients**: Malicious actors or scripts bypassing the UI entirely can fire thousands of concurrent POST requests directly to the `/v1/images` endpoint.
>
> To solve cross-request duplicate submissions, the backend architecture requires **Idempotency Keys** (e.g., an `Idempotency-Key` header mapped to an atomic Redis/PostgreSQL lock) or **Content-Addressable Hashing** (e.g., computing a SHA-256 digest of the raw image stream and deduplicating identical blobs).

---

### Week 1 Gate Compliance
```
---------------------------------------------------------------------------------
CRITICAL GATE REQUIREMENT:
"Do not move image transformation into the upload handler simply to make the
demonstration appear complete. Week 1 establishes the acceptance boundary;
Week 2 adds background execution."
---------------------------------------------------------------------------------
```
In compliance with this requirement:
- `uploadImageHandler` in [`cmd/api/images_handlers.go`](file:///c:/Users/ianbu/Documents/Test/cmd/api/images_handlers.go) terminates immediately upon writing the original file to `./storage/uploads/` and recording the `'queued'` job row in PostgreSQL.
- Zero CPU cycles are spent resizing, cropping, converting, or filtering image pixels in the HTTP request lifetime.
- Bounded acknowledgment latency ($T_{\text{ack}} < 30\text{ms}$) is maintained.
