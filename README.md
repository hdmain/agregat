# agregat

Media processing API — thumbnails, perceptual hashing and video compression. Uploads are streamed to disk so large files never fill RAM; all CPU-heavy work runs in background worker threads.

## Endpoints

### Health

```
GET /health
```

### Thumbnail

Generate a JPEG thumbnail from any image/video file.

| Endpoint | Method | Description |
|---|---|---|
| `/thumbnail` | `POST` (multipart) | Upload file as `file` field |
| `/thumbnail/raw` | `POST` (raw body) | Raw bytes, suffix auto-detected |

Query params: `?width=512&jpeg_quality=50` (defaults: 256px, quality 12)

Returns job ID immediately:

```json
{"ok": true, "job_id": "...", "message": "thumbnail job queued"}
```

### Perceptual Hashing

Compute [pHash](https://www.hackerfactor.com/blog/index.php?/archives/432-Looks-Like-It.html) for image similarity detection.

| Endpoint | Method | Description |
|---|---|---|
| `/hash` | `POST` (multipart) | One image |
| `/hash/raw` | `POST` (raw body) | One image |
| `/hash/compare` | `POST` (multipart) | Two images (`file1`, `file2`) |
| `/hash/compare/raw` | `POST` (JSON) | `{ "image1": [...], "image2": [...] }` base64 |

Hamming distance ≤ 10 → images are likely duplicates.

### Video Compression

Compress videos using libx264 or NVENC (h264_nvenc).

| Endpoint | Method | Description |
|---|---|---|
| `/compress` | `POST` (multipart) | Single output |
| `/compress/raw` | `POST` (raw body) | Single output |
| `/compress/messenger` | `POST` (multipart) | ZIP with normal + low quality |
| `/compress/messenger/raw` | `POST` (raw body) | ZIP with normal + low quality |

Query params: `?profile=normal\|low` or individual `?bitrate=800k&crf=28&fps=24...`

### Job Management

All endpoints return a job ID. Track and manage background work:

| Endpoint | Method | Description |
|---|---|---|
| `GET /jobs` | `GET` | List recent jobs (default last 50) |
| `GET /jobs?status=running` | `GET` | Filter by status: `running`, `completed`, `cancelled`, `failed` |
| `GET /jobs?limit=100` | `GET` | Custom limit |
| `GET /jobs/active` | `GET` | Currently running jobs only |
| `GET /jobs/{id}` | `GET` | Inspect one job |
| `POST /jobs/{id}/cancel` | `POST` | Cancel a running job |

Job response fields:

| Field | Type | Description |
|---|---|---|
| `id` | string | UUID v4 |
| `type` | string | `thumbnail`, `hash`, `hash_compare`, `compress` |
| `status` | string | `running`, `completed`, `cancelled`, `failed` |
| `file_name` | string \| null | Original filename |
| `file_size` | number \| null | Size in bytes |
| `extra` | object \| null | Job-specific config (width, bitrate, etc.) |
| `created_at` | string | ISO 8601 timestamp |
| `started_at` | string \| null | When worker started |
| `finished_at` | string \| null | When worker finished |
| `elapsed_ms` | number \| null | Wall-clock duration |
| `error` | string \| null | Error message on failure |

## Architecture

```
Client request
    ↓
Stream to temp file (disk, not RAM)
    ↓
Return { ok: true, job_id: "..." }
    ↓
Background thread (tokio::spawn_blocking)
    ↓ Worker does the actual CPU work
    ↓
Result tracked in JobRegistry
```

- **File uploads**: streamed to `NamedTempFile` — never held in memory
- **CPU work**: dispatched via `tokio::spawn_blocking` — HTTP thread never blocks
- **Cancellation**: atomic flag checked by workers before/during work
- **Auto-pruning**: completed/cancelled/failed jobs older than 1 hour removed every 30 minutes

## Building

```bash
cargo build
cargo run --release
```

Requires FFmpeg libraries for rumpeg (video decode/encode). On Windows NVENC is available if an NVIDIA GPU is present.

## License

MIT
