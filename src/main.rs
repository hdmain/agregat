//! Media aggregator — thumbnails + bitrate-reduced video.
//!
//! Uploads are streamed to a temp file so large videos are not held in RAM.
//! All work is dispatched into background jobs (tokio::spawn_blocking) so the
//! HTTP thread never blocks on CPU-heavy media processing.

mod compress;
mod job;
mod thumbnail;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Multipart, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use compress::CompressOptions;
use futures_util::StreamExt;
use job::{JobMeta, JobRegistry, JobType};
use serde::Deserialize;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::{Builder, NamedTempFile};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

const PORT: u16 = 7528;
/// Allow large uploads; content is streamed to disk, not kept in a giant Vec.
const MAX_UPLOAD_BYTES: usize = 2 * 1024 * 1024 * 1024; // 2 GiB

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    rumpeg::init();

    let registry = JobRegistry::new();

    // Background cleanup: prune jobs older than 1 hour every 30 minutes.
    registry.spawn_pruner(Duration::from_secs(30 * 60), Duration::from_secs(60 * 60));

    let app = Router::new()
        .route("/", get(health))
        .route("/health", get(health))
        // Job management API — exact routes BEFORE {id} catch-alls
        .route("/jobs", get(job::handle_list_jobs))
        .route("/jobs/active", get(job::handle_list_active_jobs))
        .route("/jobs/{id}/cancel", post(job::handle_cancel_job))
        .route("/jobs/{id}", get(job::handle_get_job))
        // Thumbnail endpoints — fire-and-forget with job tracking
        .route("/thumbnail", post(thumbnail_multipart))
        .route("/thumbnail/raw", post(thumbnail_raw))
        // Hash endpoints
        .route("/hash", post(hash_multipart))
        .route("/hash/raw", post(hash_raw))
        .route("/hash/compare", post(hash_compare_multipart))
        .route("/hash/compare/raw", post(hash_compare_raw))
        // Compress endpoints
        .route("/compress", post(compress_multipart))
        .route("/compress/raw", post(compress_raw))
        .route("/compress/messenger", post(compress_messenger_multipart))
        .route("/compress/messenger/raw", post(compress_messenger_raw))
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(registry);

    let addr = SocketAddr::from(([0, 0, 0, 0], PORT));
    tracing::info!("agregat listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind port 7528");
    axum::serve(listener, app).await.expect("server");
}

async fn health(State(reg): State<JobRegistry>) -> Json<serde_json::Value> {
    let running = reg
        .list(Some(job::JobStatusFilter::Running), usize::MAX)
        .len();
    Json(serde_json::json!({
        "ok": true,
        "service": "agregat",
        "port": PORT,
        "jobs_running": running,
        "thumbnail": {
            "width": thumbnail::THUMB_WIDTH,
            "jpeg_quality": thumbnail::JPEG_QUALITY,
            "endpoints": {
                "multipart": "POST /thumbnail?width=512&jpeg_quality=50",
                "raw": "POST /thumbnail/raw?width=512&jpeg_quality=50",
            },
            "note": "width and jpeg_quality are overridable via query params.",
        },
        "hash": {
            "endpoints": {
                "single": "POST /hash → pHash of one image (multipart)",
                "compare": "POST /hash/compare → Hamming distance between two images (multipart)",
                "compare_raw": "POST /hash/compare/raw → JSON with base64 image1/image2",
            },
            "note": "Hamming distance ≤ 10 means images are likely duplicates.",
        },
        "compress": {
            "default_bitrate": compress::DEFAULT_BITRATE,
            "default_audio_bitrate": compress::DEFAULT_AUDIO_BITRATE,
            "default_crf": compress::DEFAULT_CRF,
            "default_gop": compress::DEFAULT_GOP,
            "default_preset": compress::DEFAULT_PRESET,
            "default_encoder": compress::DEFAULT_ENCODER,
            "encoders": ["h264", "h264_nvenc", "native"],
            "profiles": {
                "normal": { "max_width": 854, "bitrate": "1200k", "fps": 24 },
                "low": { "max_width": 480, "bitrate": "350k", "fps": 12 },
            },
            "endpoints": {
                "single": "POST /compress?profile=normal|low (or bitrate/scale/fps)",
                "dual": "POST /compress/messenger → zip with normal.mp4 + low.mp4 (one decode)",
            },
            "note": "Default encoder h264_nvenc; messenger dual path shares one decode for both qualities.",
        }
    }))
}

#[derive(Debug, Deserialize, Default)]
struct ThumbnailQuery {
    /// Target width (height keeps aspect). Default 256.
    width: Option<u32>,
    /// JPEG quality 1–100. Default 12.
    #[serde(alias = "quality")]
    jpeg_quality: Option<u8>,
}

impl ThumbnailQuery {
    fn into_options(self) -> thumbnail::ThumbnailOptions {
        let mut opts = thumbnail::ThumbnailOptions::default();
        if let Some(w) = self.width {
            opts.width = w.max(1);
        }
        if let Some(q) = self.jpeg_quality {
            opts.jpeg_quality = q.clamp(1, 100);
        }
        opts
    }
}

#[derive(Debug, Deserialize, Default)]
struct CompressQuery {
    /// e.g. `500k`, `1M`, or raw bits/s
    #[serde(alias = "b:v")]
    bitrate: Option<String>,
    /// e.g. `128k`
    #[serde(alias = "b:a")]
    audio_bitrate: Option<String>,
    crf: Option<i32>,
    qp: Option<i32>,
    gop: Option<u32>,
    /// Max output width (keeps aspect). Example: `1280`
    scale: Option<u32>,
    #[serde(alias = "max_width")]
    max_width: Option<u32>,
    /// Target fps (fps filter). Big speed win when lowered.
    fps: Option<u32>,
    /// Prefer speed: default max width 640 + 15fps when unset.
    #[serde(default)]
    fast: bool,
    /// `fast` / `medium` / `slow`
    preset: Option<String>,
    /// `h264`, `h264_nvenc` (default), `native`
    #[serde(alias = "c:v")]
    encoder: Option<String>,
    /// Messenger preset: `normal` | `low`
    profile: Option<String>,
}

impl CompressQuery {
    fn into_options(self) -> CompressOptions {
        let profile = self.profile.as_deref().and_then(compress::Profile::parse);
        CompressOptions {
            bitrate: self.bitrate,
            audio_bitrate: self.audio_bitrate,
            crf: self.crf,
            qp: self.qp,
            gop: self.gop,
            max_width: self.scale.or(self.max_width),
            fps: self.fps,
            fast: self.fast,
            preset: self.preset,
            encoder: self.encoder,
            profile,
        }
        .with_defaults()
    }
}

// ── helpers ─────────────────────────────────────────────────────

/// Compute Hamming distance between two `imagehash::Hash` values.
fn hamming_distance(a: &imagehash::Hash, b: &imagehash::Hash) -> u64 {
    a.bits
        .iter()
        .zip(b.bits.iter())
        .filter(|(x, y)| x != y)
        .count() as u64
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(serde_json::json!({"ok": false, "error": message})),
    )
        .into_response()
}

fn temp_file_name(file: &NamedTempFile) -> Option<String> {
    file.path()
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
}

/// Stream multipart form fields to a temp file. Returns it on success.
async fn stream_multipart_file(mut multipart: Multipart) -> Result<NamedTempFile, Response> {
    let mut temp: Option<NamedTempFile> = None;
    let mut written: u64 = 0;
    let mut parse_err: Option<String> = None;

    loop {
        match multipart.next_field().await {
            Ok(Some(mut field)) => {
                let name = field.name().unwrap_or("").to_string();
                let filename = field.file_name().map(|s| s.to_string());
                let is_file = name == "file"
                    || name == "media"
                    || name == "image"
                    || name == "video"
                    || filename.is_some();

                if !is_file && temp.is_some() {
                    continue;
                }

                let suffix = filename
                    .as_deref()
                    .and_then(|f| Path::new(f).extension())
                    .and_then(|e| e.to_str())
                    .map(|e| format!(".{e}"))
                    .unwrap_or_else(|| ".bin".to_string());

                let mut file = match Builder::new().suffix(&suffix).tempfile() {
                    Ok(f) => f,
                    Err(e) => {
                        parse_err = Some(format!("temp file create failed: {e}"));
                        break;
                    }
                };

                loop {
                    match field.chunk().await {
                        Ok(Some(chunk)) => {
                            if let Err(e) = file.as_file_mut().write_all(&chunk) {
                                parse_err = Some(format!("temp write failed: {e}"));
                                break;
                            }
                            written += chunk.len() as u64;
                        }
                        Ok(None) => break,
                        Err(e) => {
                            parse_err = Some(format!("multipart read error: {e}"));
                            break;
                        }
                    }
                }

                if parse_err.is_some() {
                    break;
                }
                if written == 0 {
                    continue;
                }
                if let Err(e) = file.as_file_mut().sync_all() {
                    parse_err = Some(format!("temp sync failed: {e}"));
                    break;
                }

                tracing::info!(
                    field = %name,
                    filename = ?filename,
                    bytes = written,
                    "multipart streamed to temp file"
                );
                temp = Some(file);
                if is_file {
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => {
                parse_err = Some(format!("multipart parse error: {e}"));
                break;
            }
        }
    }

    if let Some(msg) = parse_err {
        tracing::warn!("{msg}");
        return Err(error_response(StatusCode::BAD_REQUEST, &msg));
    }

    temp.ok_or_else(|| {
        error_response(
            StatusCode::BAD_REQUEST,
            "missing multipart file field (use `file`)",
        )
    })
}

/// Stream raw body to a temp file.
async fn stream_body_to_temp(body: Body, suffix: &str) -> Result<NamedTempFile, Response> {
    let mut file = Builder::new()
        .suffix(suffix)
        .tempfile()
        .map_err(|e| error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))?;

    let mut stream = body.into_data_stream();
    let mut written: u64 = 0;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                file.as_file_mut().write_all(&bytes).map_err(|e| {
                    error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
                })?;
                written += bytes.len() as u64;
            }
            Err(e) => {
                return Err(error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("body read error: {e}"),
                ));
            }
        }
    }

    if written == 0 {
        return Err(error_response(StatusCode::BAD_REQUEST, "empty body"));
    }

    file.as_file_mut()
        .sync_all()
        .map_err(|e| error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))?;

    tracing::info!(bytes = written, "raw body streamed to temp file");
    Ok(file)
}

/// Stream two fields from a multipart form.
async fn stream_multipart_files(
    multipart: Multipart,
) -> Result<(NamedTempFile, NamedTempFile), Response> {
    let mut temp1: Option<NamedTempFile> = None;
    let mut temp2: Option<NamedTempFile> = None;
    let mut parse_err: Option<String> = None;

    let mut mp = multipart;
    loop {
        match mp.next_field().await {
            Ok(Some(mut field)) => {
                let name = field.name().unwrap_or("");
                let filename = field.file_name().map(|s| s.to_string());

                let target = if name == "file1" || filename.as_deref() == Some("image1.jpg") {
                    &mut temp1
                } else if name == "file2" || filename.as_deref() == Some("image2.jpg") {
                    &mut temp2
                } else {
                    continue;
                };

                let suffix = filename
                    .as_deref()
                    .and_then(|f| Path::new(f).extension())
                    .and_then(|e| e.to_str())
                    .map(|e| format!(".{e}"))
                    .unwrap_or_else(|| ".bin".to_string());

                let mut file = match Builder::new().suffix(&suffix).tempfile() {
                    Ok(f) => f,
                    Err(e) => {
                        parse_err = Some(format!("temp file create failed: {e}"));
                        break;
                    }
                };

                loop {
                    match field.chunk().await {
                        Ok(Some(chunk)) => {
                            if let Err(e) = file.as_file_mut().write_all(&chunk) {
                                parse_err = Some(format!("temp write failed: {e}"));
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            parse_err = Some(format!("multipart read error: {e}"));
                            break;
                        }
                    }
                }

                if parse_err.is_some() {
                    break;
                }
                if let Err(e) = file.as_file_mut().sync_all() {
                    parse_err = Some(format!("temp sync failed: {e}"));
                    break;
                }

                *target = Some(file);
            }
            Ok(None) => break,
            Err(e) => {
                parse_err = Some(format!("multipart parse error: {e}"));
                break;
            }
        }
    }

    let path1 = match temp1 {
        Some(f) => f,
        None => {
            let msg = parse_err.unwrap_or_else(|| "missing file1".into());
            return Err(error_response(StatusCode::BAD_REQUEST, &msg));
        }
    };
    let path2 = match temp2 {
        Some(f) => f,
        None => {
            let msg = parse_err.unwrap_or_else(|| "missing file2".into());
            return Err(error_response(StatusCode::BAD_REQUEST, &msg));
        }
    };

    Ok((path1, path2))
}

// ── async job-aware endpoint handlers ───────────────────────────

/// `POST /thumbnail?width=512&jpeg_quality=50` — streams file, returns job ID immediately.
async fn thumbnail_multipart(
    State(reg): State<JobRegistry>,
    Query(q): Query<ThumbnailQuery>,
    multipart: Multipart,
) -> Response {
    let opts = q.into_options();
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_thumbnail_async(reg, file, opts).await,
        Err(resp) => resp,
    }
}

/// `POST /thumbnail/raw?width=512&jpeg_quality=50` — streams body, returns job ID.
async fn thumbnail_raw(
    State(reg): State<JobRegistry>,
    Query(q): Query<ThumbnailQuery>,
    body: Body,
) -> Response {
    let opts = q.into_options();
    match stream_body_to_temp(body, ".bin").await {
        Ok(file) => respond_thumbnail_async(reg, file, opts).await,
        Err(resp) => resp,
    }
}

/// Background thumbnail worker — returns job ID immediately.
async fn respond_thumbnail_async(
    reg: JobRegistry,
    file: NamedTempFile,
    opts: thumbnail::ThumbnailOptions,
) -> Response {
    let in_path: PathBuf = file.path().to_path_buf();
    let file_name = temp_file_name(&file);
    let file_size = std::fs::metadata(&in_path).ok().map(|m| m.len());

    let meta = JobMeta {
        file_name,
        file_size,
        extra: Some(serde_json::json!({
            "width": opts.width,
            "jpeg_quality": opts.jpeg_quality,
        })),
    };

    // Thumbnail work doesn't need a result payload beyond output_size,
    // which we get from metadata of the input (the file still exists on disk).
    let id = job::spawn_job(
        reg,
        JobType::Thumbnail,
        meta,
        move |cancelled| {
            cancelled.store(false, std::sync::atomic::Ordering::SeqCst); // already checked at spawn
            let _ = thumbnail::make_thumbnail_from_path(&in_path, &opts);
            drop(file);
        },
        || None,
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "thumbnail job queued",
    }))
    .into_response()
}

/// `POST /hash` — upload one image, get its pHash back (async).
async fn hash_multipart(State(reg): State<JobRegistry>, multipart: Multipart) -> Response {
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_hash_async(reg, file).await,
        Err(resp) => resp,
    }
}

/// `POST /hash/raw` — raw body → pHash (async).
async fn hash_raw(State(reg): State<JobRegistry>, body: Body) -> Response {
    match stream_body_to_temp(body, ".bin").await {
        Ok(file) => respond_hash_async(reg, file).await,
        Err(resp) => resp,
    }
}

async fn respond_hash_async(reg: JobRegistry, file: NamedTempFile) -> Response {
    let in_path: PathBuf = file.path().to_path_buf();
    let file_name = temp_file_name(&file);
    let file_size = std::fs::metadata(&in_path).ok().map(|m| m.len());

    let meta = JobMeta {
        file_name,
        file_size,
        extra: None,
    };

    // Compute result inside `work` since the temp file is moved/dropped.
    let computed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let cc = computed.clone();

    // Clone in_path for the work closure.
    let work_path = in_path.clone();

    let id = job::spawn_job(
        reg,
        JobType::Hash,
        meta,
        move |_cancelled| {
            if let Ok(img) = image::open(&work_path) {
                let hash = imagehash::perceptual_hash(&img);
                *cc.lock().unwrap() = Some(hash.to_string());
            }
            drop(file);
        },
        move || {
            computed
                .lock()
                .ok()
                .and_then(|g| g.as_ref().cloned())
                .map(|phash| serde_json::json!({ "phash": phash }))
        },
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "hash job queued",
    }))
    .into_response()
}

/// `POST /hash/compare` — upload two images, get Hamming distance (async).
async fn hash_compare_multipart(State(reg): State<JobRegistry>, multipart: Multipart) -> Response {
    match stream_multipart_files(multipart).await {
        Ok(files) => respond_hash_compare_async(reg, files).await,
        Err(resp) => resp,
    }
}

/// `POST /hash/compare/raw` — JSON with image bytes → compare (async).
async fn hash_compare_raw(
    State(reg): State<JobRegistry>,
    Json(payload): Json<HashComparePayload>,
) -> Response {
    let mut tmp1 = match Builder::new().suffix(".jpg").tempfile() {
        Ok(f) => f,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    let mut tmp2 = match Builder::new().suffix(".jpg").tempfile() {
        Ok(f) => f,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };

    let _ = tmp1.as_file_mut().write_all(&payload.image1);
    let _ = tmp2.as_file_mut().write_all(&payload.image2);

    // Compute hashes inside `work` since temp files are moved/dropped.
    let computed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let computed1 = computed.clone();

    let file_name = Some("compare".to_string());
    let file_size = Some((payload.image1.len() + payload.image2.len()) as u64);

    let _meta = JobMeta {
        file_name,
        file_size,
        extra: None,
    };

    let id = job::spawn_job(
        reg,
        JobType::HashCompare,
        _meta,
        move |_cancelled| {
            let h1 = image::open(tmp1.path())
                .ok()
                .map(|img| imagehash::perceptual_hash(&img));
            let h2 = image::open(tmp2.path())
                .ok()
                .map(|img| imagehash::perceptual_hash(&img));
            if let (Some(a), Some(b)) = (h1, h2) {
                *computed1.lock().unwrap() = Some(hamming_distance(&a, &b).to_string());
            }
            drop(tmp1);
            drop(tmp2);
        },
        move || {
            computed
                .lock()
                .ok()
                .and_then(|g| g.as_ref().cloned())
                .map(|dist| serde_json::json!({ "distance": dist.parse::<u64>().unwrap_or(0) }))
        },
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "hash compare job queued",
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct HashComparePayload {
    image1: Vec<u8>,
    image2: Vec<u8>,
}

async fn respond_hash_compare_async(
    reg: JobRegistry,
    files: (NamedTempFile, NamedTempFile),
) -> Response {
    let (file1, file2) = files;
    let path1: PathBuf = file1.path().to_path_buf();
    let path2: PathBuf = file2.path().to_path_buf();
    let file_name = Some("compare".to_string());
    let file_size = std::fs::metadata(&path1)
        .ok()
        .map(|m| m.len())
        .zip(std::fs::metadata(&path2).ok().map(|m| m.len()))
        .map(|(a, b)| a + b);

    let meta = JobMeta {
        file_name,
        file_size,
        extra: None,
    };

    // Compute hashes inside `work` since temp files are moved/dropped.
    let computed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let c1 = computed.clone();

    let id = job::spawn_job(
        reg,
        JobType::HashCompare,
        meta,
        move |_cancelled| {
            let h1 = image::open(&path1)
                .ok()
                .map(|img| imagehash::perceptual_hash(&img));
            let h2 = image::open(&path2)
                .ok()
                .map(|img| imagehash::perceptual_hash(&img));
            if let (Some(a), Some(b)) = (h1, h2) {
                *c1.lock().unwrap() = Some(hamming_distance(&a, &b).to_string());
            }
            drop(file1);
            drop(file2);
        },
        move || {
            computed
                .lock()
                .ok()
                .and_then(|g| g.as_ref().cloned())
                .map(|dist| serde_json::json!({ "distance": dist.parse::<u64>().unwrap_or(0) }))
        },
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "hash compare job queued",
    }))
    .into_response()
}

/// `POST /compress?bitrate=800k` — multipart upload → smaller H.264 MP4 (async).
async fn compress_multipart(
    State(reg): State<JobRegistry>,
    Query(q): Query<CompressQuery>,
    multipart: Multipart,
) -> Response {
    let opts = q.into_options();
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_compress_async(reg, file, opts).await,
        Err(resp) => resp,
    }
}

/// `POST /compress/raw?bitrate=800k` — raw body → smaller H.264 MP4 (async).
async fn compress_raw(
    State(reg): State<JobRegistry>,
    Query(q): Query<CompressQuery>,
    body: Body,
) -> Response {
    let opts = q.into_options();
    match stream_body_to_temp(body, ".mp4").await {
        Ok(file) => respond_compress_async(reg, file, opts).await,
        Err(resp) => resp,
    }
}

async fn respond_compress_async(
    reg: JobRegistry,
    input: NamedTempFile,
    opts: CompressOptions,
) -> Response {
    let out = match Builder::new().suffix(".mp4").tempfile() {
        Ok(f) => f,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };

    let in_path: PathBuf = input.path().to_path_buf();
    let out_path: PathBuf = out.path().to_path_buf();

    let file_name = temp_file_name(&input);
    let file_size = std::fs::metadata(&in_path).ok().map(|m| m.len());

    let meta = JobMeta {
        file_name,
        file_size,
        extra: Some(serde_json::json!({
            "bitrate": opts.bitrate,
            "audio_bitrate": opts.audio_bitrate,
            "crf": opts.crf,
            "qp": opts.qp,
            "gop": opts.gop,
            "max_width": opts.max_width,
            "fps": opts.fps,
            "fast": opts.fast,
            "preset": opts.preset,
            "encoder": opts.encoder,
            "profile": opts.profile.map(|p| match p {
                compress::Profile::Normal => "normal",
                compress::Profile::Low => "low",
            }),
        })),
    };

    // Clone `out_path` for `on_complete` — it's moved into `work`.
    let out_result = out_path.clone();

    let id = job::spawn_job(
        reg,
        JobType::Compress,
        meta,
        move |_cancelled| {
            let result = compress::compress_video(&in_path, &out_path, &opts);
            if let Err(e) = result {
                tracing::warn!("compress failed: {e}");
            }
            drop(input);
        },
        move || {
            if let Ok(meta) = std::fs::metadata(&out_result) {
                Some(serde_json::json!({ "output_size": meta.len() }))
            } else {
                None
            }
        },
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "compress job queued",
    }))
    .into_response()
}

/// `POST /compress/messenger` — one decode → normal.mp4 + low.mp4 in a ZIP (async).
async fn compress_messenger_multipart(
    State(reg): State<JobRegistry>,
    multipart: Multipart,
) -> Response {
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_messenger_dual_async(reg, file).await,
        Err(resp) => resp,
    }
}

/// `POST /compress/messenger/raw` — raw body → ZIP with both messenger qualities (async).
async fn compress_messenger_raw(State(reg): State<JobRegistry>, body: Body) -> Response {
    match stream_body_to_temp(body, ".mp4").await {
        Ok(file) => respond_messenger_dual_async(reg, file).await,
        Err(resp) => resp,
    }
}

async fn respond_messenger_dual_async(reg: JobRegistry, input: NamedTempFile) -> Response {
    let out = match Builder::new().suffix(".zip").tempfile() {
        Ok(f) => f,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };

    let in_path: PathBuf = input.path().to_path_buf();
    let out_path: PathBuf = out.path().to_path_buf();

    let file_name = temp_file_name(&input);
    let file_size = std::fs::metadata(&in_path).ok().map(|m| m.len());

    let meta = JobMeta {
        file_name,
        file_size,
        extra: Some(serde_json::json!({ "mode": "messenger_dual" })),
    };

    // Clone `out_path` for `on_complete` — it's moved into `work`.
    let out_result = out_path.clone();

    let id = job::spawn_job(
        reg,
        JobType::Compress,
        meta,
        move |_cancelled| {
            let result = compress::compress_messenger_dual(&in_path, &out_path);
            if let Err(e) = result {
                tracing::warn!("messenger compress failed: {e}");
            }
            drop(input);
        },
        move || {
            if let Ok(meta) = std::fs::metadata(&out_result) {
                Some(serde_json::json!({ "zip_size": meta.len() }))
            } else {
                None
            }
        },
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "messenger compress job queued",
    }))
    .into_response()
}
