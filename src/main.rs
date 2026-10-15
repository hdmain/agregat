#!\ Media aggregator — thumbnails + bitrate-reduced video.
//!
//! Uploads are streamed to a temp file so large videos are not held in RAM.
//! All work is dispatched into background jobs (tokio::spawn_blocking) so the
//! HTTP thread never blocks on CPU-heavy media processing.
//!
//! Workers: `--workers <N>` sets tokio blocking threads (default = num_cpus).
//! Prune interval: `--prune-secs <N>` removes old jobs (default 3600).

mod compress;
mod config;
mod ffmpeg;
mod job;
mod thumbnail;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Multipart, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use compress::CompressOptions;

use config::{load_config, AppState};
use job::{JobMeta, JobType};

use futures_util::StreamExt;
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

#[tokio::main]
async fn main() {
    let config = load_config();
    tracing::info!(?config, "configuration loaded");

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    rumpeg::init();

    std::fs::create_dir_all(&config.thumb_dir).ok();

    let worker_count = num_cpus::get() * config.workers_multiplier;
    job::init_worker_pool(worker_count);

    let registry = job::JobRegistry::new();
    let state = AppState { config, registry };

    state.registry.spawn_pruner(
        Duration::from_secs(state.config.prune_secs / 2),
        Duration::from_secs(state.config.prune_secs),
    );

    let app = Router::new()
        .route("/", get(health))
        .route("/health", get(health))
        .route("/jobs", get(job::handle_list_jobs))
        .route("/jobs/active", get(job::handle_list_active_jobs))
        .route("/jobs/{id}/cancel", post(job::handle_cancel_job))
        .route("/jobs/{id}", get(job::handle_get_job))
        .route("/thumb/{name}", get(download_thumbnail))
        .route("/thumbnail", post(thumbnail_multipart))
        .route("/thumbnail/raw", post(thumbnail_raw))
        .route("/hash", post(hash_multipart))
        .route("/hash/raw", post(hash_raw))
        .route("/hash/compare", post(hash_compare_multipart))
        .route("/hash/compare/raw", post(hash_compare_raw))
        .route("/compress", post(compress_multipart))
        .route("/compress/raw", post(compress_raw))
        .route("/compress/messenger", post(compress_messenger_multipart))
        .route("/compress/messenger/raw", post(compress_messenger_raw))
        .route("/ffmpeg/compress", post(ffmpeg_compress_multipart))
        .route("/ffmpeg/compress/raw", post(ffmpeg_compress_raw))
        .route("/ffmpeg/qualities", post(ffmpeg_qualities_multipart))
        .route("/ffmpeg/qualities/raw", post(ffmpeg_qualities_raw))
        .route("/ffmpeg/strip-exif", post(ffmpeg_strip_exif_multipart))
        .route("/ffmpeg/strip-exif/raw", post(ffmpeg_strip_exif_raw))
        .route("/ffmpeg/{name}", get(download_ffmpeg_output))
        .layer(DefaultBodyLimit::max(state.config.max_upload_bytes))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    tracing::info!("agregat listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind port");
    axum::serve(listener, app).await.expect("server");
}

async fn health(State(s): State<AppState>) -> Json<serde_json::Value> {
    let cfg = &s.config;
    let reg = &s.registry;
    let running = reg
        .list(Some(job::JobStatusFilter::Running), usize::MAX)
        .len();
    Json(serde_json::json!({
        "ok": true,
        "service": "agregat",
        "port": cfg.port,
        "jobs_running": running,
        "thumbnail": {
            "width": cfg.thumb_width,
            "jpeg_quality": cfg.thumb_jpeg_quality,
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
        },
        "ffmpeg": {
            "available": ffmpeg::ffmpeg_available(cfg.ffmpeg_path.as_deref()),
            "path": ffmpeg::resolve_ffmpeg(cfg.ffmpeg_path.as_deref())
                .map(|p| p.display().to_string()),
            "endpoints": {
                "compress": "POST /ffmpeg/compress?mode=lossy|lossless&strip_exif=true&profile=normal|low",
                "qualities": "POST /ffmpeg/qualities?strip_exif=true&include_lossless=false → zip normal+low",
                "strip_exif": "POST /ffmpeg/strip-exif → stream-copy without metadata",
                "download": "GET /ffmpeg/{name}",
            },
            "note": "Uses system FFmpeg with CUDA/NVENC when available; strips EXIF/metadata by default.",
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
        // Zero values mean "not set" — server will apply config defaults
        if opts.width == 0 {
            opts.width = thumbnail::THUMB_WIDTH;
        }
        if opts.jpeg_quality == 0 {
            opts.jpeg_quality = thumbnail::JPEG_QUALITY;
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

#[derive(Debug, Deserialize, Default)]
struct FfmpegQuery {
    /// `lossy` (default) | `lossless`
    mode: Option<String>,
    /// Messenger profile for lossy: `normal` | `low`
    profile: Option<String>,
    /// Strip EXIF / container metadata (default true).
    strip_exif: Option<bool>,
    bitrate: Option<String>,
    audio_bitrate: Option<String>,
    scale: Option<u32>,
    max_width: Option<u32>,
    fps: Option<u32>,
    crf: Option<i32>,
    preset: Option<String>,
    /// `auto` | `cuda` | `none`
    hwaccel: Option<String>,
    /// For /ffmpeg/qualities — also pack lossless.mp4
    include_lossless: Option<bool>,
}

impl FfmpegQuery {
    fn into_options(self) -> ffmpeg::FfmpegOptions {
        let mode = self
            .mode
            .as_deref()
            .and_then(ffmpeg::CompressMode::parse)
            .unwrap_or(ffmpeg::CompressMode::Lossy);
        ffmpeg::FfmpegOptions {
            mode,
            profile: self.profile.as_deref().and_then(ffmpeg::QualityProfile::parse),
            strip_exif: self.strip_exif.unwrap_or(true),
            bitrate: self.bitrate,
            audio_bitrate: self.audio_bitrate,
            max_width: self.scale.or(self.max_width),
            fps: self.fps,
            crf: self.crf,
            preset: self.preset,
            hwaccel: self.hwaccel.unwrap_or_else(|| "auto".into()),
            include_lossless: self.include_lossless.unwrap_or(false),
        }
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
    State(s): State<AppState>,
    Query(q): Query<ThumbnailQuery>,
    multipart: Multipart,
) -> Response {
    let opts = q.into_options();
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_thumbnail_async(s, file, opts).await,
        Err(resp) => resp,
    }
}

async fn thumbnail_raw(
    State(s): State<AppState>,
    Query(q): Query<ThumbnailQuery>,
    body: Body,
) -> Response {
    let opts = q.into_options();
    match stream_body_to_temp(body, ".bin").await {
        Ok(file) => respond_thumbnail_async(s, file, opts).await,
        Err(resp) => resp,
    }
}

/// Background thumbnail worker — returns job ID immediately.
async fn respond_thumbnail_async(
    s: AppState,
    file: NamedTempFile,
    mut opts: thumbnail::ThumbnailOptions,
) -> Response {
    let in_path: PathBuf = file.path().to_path_buf();
    let file_name = temp_file_name(&file);
    let file_size = std::fs::metadata(&in_path).ok().map(|m| m.len());

    // Apply config defaults for any unset values
    if opts.width == 0 {
        opts.width = s.config.thumb_width;
    }
    if opts.jpeg_quality == 0 {
        opts.jpeg_quality = s.config.thumb_jpeg_quality;
    }

    let meta = JobMeta {
        file_name,
        file_size,
        extra: Some(serde_json::json!({
            "width": opts.width,
            "jpeg_quality": opts.jpeg_quality,
        })),
    };

    let out_name = format!("thumb_{}.jpg", uuid::Uuid::new_v4());
    let out_path: PathBuf = Path::new(&s.config.thumb_dir).join(&out_name);
    let out_check = out_path.clone();

    let id = job::spawn_job(
        s.registry,
        JobType::Thumbnail,
        meta.clone(),
        move |cancelled| {
            if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("cancelled by user".into());
            }
            let data = thumbnail::make_thumbnail_from_path(&in_path, &opts)
                .map_err(|e| e.to_string())?;
            if data.is_empty() {
                return Err("thumbnail produced empty output".into());
            }
            std::fs::write(&out_path, &data).map_err(|e| e.to_string())?;
            drop(file);
            Ok(())
        },
        move || {
            if let Ok(meta) = std::fs::metadata(&out_check) {
                Some(serde_json::json!({ "output_size": meta.len(), "output_path": out_name }))
            } else {
                None
            }
        },
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "thumbnail job queued",
    }))
    .into_response()
}

/// `POST /hash` — upload one image, get its pHash back (async).
async fn hash_multipart(State(s): State<AppState>, multipart: Multipart) -> Response {
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_hash_async(s, file).await,
        Err(resp) => resp,
    }
}

/// `POST /hash/raw` — raw body → pHash (async).
async fn hash_raw(State(s): State<AppState>, body: Body) -> Response {
    match stream_body_to_temp(body, ".bin").await {
        Ok(file) => respond_hash_async(s, file).await,
        Err(resp) => resp,
    }
}

/// Extract first video frame — downsampled to configured size for fast pHash.
pub fn extract_first_frame_64(path: &Path, target_w: u32) -> Option<image::DynamicImage> {
    use rumpeg::codec::DecoderContext;
    use rumpeg::filter;
    use rumpeg::format::FormatContext;
    use rumpeg::scale::{self, FilterMode};
    use rumpeg::{Frame, MediaType, PixelFormat};

    let mut demuxer = FormatContext::open(path).ok()?;
    let stream_idx = demuxer.best_stream(MediaType::Video)?.index;
    let codec_params = demuxer
        .streams()
        .iter()
        .find(|s| s.index == stream_idx)?
        .codec_params
        .clone();
    let mut decoder = DecoderContext::open(&codec_params).ok()?;

    // pHash only needs a small frame; scale while decoding to avoid full-res work.
    let target_w = target_w.max(8);
    loop {
        let packet = match demuxer.read_packet() {
            Ok(p) => p,
            Err(rumpeg::Error::Eof) => break,
            Err(_) => return None,
        };
        if packet.stream_index != stream_idx {
            continue;
        }
        for frame in decoder.decode(&packet).ok()? {
            if let Frame::Video(v) = frame {
                let scale_filter = format!("scale={}:-1", target_w);
                let filtered = filter::process(Frame::Video(v), &scale_filter).ok()?;
                let Frame::Video(scaled) = filtered else {
                    return None;
                };
                let rgb = scale::transform(
                    &scaled,
                    PixelFormat::Rgb24,
                    scaled.width,
                    scaled.height,
                    FilterMode::Bilinear,
                )
                .ok()?;
                let data = rgb.plane(0)?;
                return Some(image::DynamicImage::ImageRgb8(
                    image::ImageBuffer::from_raw(scaled.width, scaled.height, data.to_vec())?,
                ));
            }
        }
    }
    None
}

async fn respond_hash_async(s: AppState, file: NamedTempFile) -> Response {
    let in_path: PathBuf = file.path().to_path_buf();
    let file_name = temp_file_name(&file);
    let file_size = std::fs::metadata(&in_path).ok().map(|m| m.len());

    let meta = JobMeta {
        file_name,
        file_size,
        extra: None,
    };

    let hash_video_frame_size = s.config.hash_video_frame_size;
    let computed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let cc = computed.clone();
    let work_path = in_path.clone();

    let id = job::spawn_job(
        s.registry,
        JobType::Hash,
        meta,
        move |cancelled| {
            if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("cancelled by user".into());
            }
            // Try as image first, then fall back to video first-frame.
            let img = image::open(&work_path)
                .map(Some)
                .unwrap_or_else(|_| extract_first_frame_64(&work_path, hash_video_frame_size));
            let Some(img) = img else {
                drop(file);
                return Err("could not decode image or video frame".into());
            };
            let hash = imagehash::perceptual_hash(&img);
            *cc.lock().unwrap() = Some(hash.to_string());
            drop(file);
            Ok(())
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
async fn hash_compare_multipart(State(s): State<AppState>, multipart: Multipart) -> Response {
    match stream_multipart_files(multipart).await {
        Ok(files) => respond_hash_compare_async(s, files).await,
        Err(resp) => resp,
    }
}

/// `POST /hash/compare/raw` — JSON with image bytes → compare (async).
async fn hash_compare_raw(
    State(s): State<AppState>,
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
        s.registry,
        JobType::HashCompare,
        _meta,
        move |cancelled| {
            if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("cancelled by user".into());
            }
            let h1 = image::open(tmp1.path())
                .ok()
                .map(|img| imagehash::perceptual_hash(&img));
            let h2 = image::open(tmp2.path())
                .ok()
                .map(|img| imagehash::perceptual_hash(&img));
            let (Some(a), Some(b)) = (h1, h2) else {
                drop(tmp1);
                drop(tmp2);
                return Err("could not decode one or both images".into());
            };
            *computed1.lock().unwrap() = Some(hamming_distance(&a, &b).to_string());
            drop(tmp1);
            drop(tmp2);
            Ok(())
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

pub async fn respond_hash_compare_async(
    s: AppState,
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
        s.registry,
        JobType::HashCompare,
        meta,
        move |cancelled| {
            if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("cancelled by user".into());
            }
            let h1 = image::open(&path1)
                .ok()
                .map(|img| imagehash::perceptual_hash(&img));
            let h2 = image::open(&path2)
                .ok()
                .map(|img| imagehash::perceptual_hash(&img));
            let (Some(a), Some(b)) = (h1, h2) else {
                drop(file1);
                drop(file2);
                return Err("could not decode one or both images".into());
            };
            *c1.lock().unwrap() = Some(hamming_distance(&a, &b).to_string());
            drop(file1);
            drop(file2);
            Ok(())
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
    State(s): State<AppState>,
    Query(q): Query<CompressQuery>,
    multipart: Multipart,
) -> Response {
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_compress_async(s, file, q.into_options()).await,
        Err(resp) => resp,
    }
}

/// `POST /compress/raw` — raw body → H.264 MP4 (async).
async fn compress_raw(
    State(s): State<AppState>,
    Query(q): Query<CompressQuery>,
    body: Body,
) -> Response {
    match stream_body_to_temp(body, ".mp4").await {
        Ok(file) => respond_compress_async(s, file, q.into_options()).await,
        Err(resp) => resp,
    }
}

async fn respond_compress_async(
    s: AppState,
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
        s.registry,
        JobType::Compress,
        meta,
        move |cancelled| {
            if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("cancelled by user".into());
            }
            let result = compress::compress_video(&in_path, &out_path, &opts);
            drop(input);
            result.map_err(|e| {
                tracing::warn!("compress failed: {e}");
                e.to_string()
            })
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

/// Download a generated thumbnail by name.
async fn download_thumbnail(
    axum::extract::Path(name): axum::extract::Path<String>,
    State(s): State<AppState>,
) -> Response {
    let file_path = PathBuf::from(&s.config.thumb_dir).join(&name);
    match std::fs::read(&file_path) {
        Ok(data) => (StatusCode::OK, data).into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("thumbnail not found: {name}") })),
        )
            .into_response(),
    }
}

/// `POST /compress/messenger` — one decode → normal.mp4 + low.mp4 in a ZIP (async).
async fn compress_messenger_multipart(State(s): State<AppState>, multipart: Multipart) -> Response {
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_messenger_dual_async(s, file).await,
        Err(resp) => resp,
    }
}

/// `POST /compress/messenger/raw` — raw body → ZIP with both messenger qualities (async).
async fn compress_messenger_raw(State(s): State<AppState>, body: Body) -> Response {
    match stream_body_to_temp(body, ".mp4").await {
        Ok(file) => respond_messenger_dual_async(s, file).await,
        Err(resp) => resp,
    }
}

async fn respond_messenger_dual_async(s: AppState, input: NamedTempFile) -> Response {
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

    let out_result = out_path.clone();

    let id = job::spawn_job(
        s.registry,
        JobType::Compress,
        meta,
        move |cancelled| {
            if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("cancelled by user".into());
            }
            let result = compress::compress_messenger_dual(&in_path, &out_path);
            drop(input);
            result.map_err(|e| {
                tracing::warn!("messenger compress failed: {e}");
                e.to_string()
            })
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

// ── FFmpeg endpoints ────────────────────────────────────────────

fn require_ffmpeg(s: &AppState) -> Result<PathBuf, Response> {
    ffmpeg::resolve_ffmpeg(s.config.ffmpeg_path.as_deref()).ok_or_else(|| {
        error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "ffmpeg not found — set config ffmpeg_path, FFMPEG_PATH, or install ffmpeg on PATH",
        )
    })
}

/// `POST /ffmpeg/compress?mode=lossy|lossless&strip_exif=true&profile=normal|low`
async fn ffmpeg_compress_multipart(
    State(s): State<AppState>,
    Query(q): Query<FfmpegQuery>,
    multipart: Multipart,
) -> Response {
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_ffmpeg_compress(s, file, q.into_options()).await,
        Err(resp) => resp,
    }
}

async fn ffmpeg_compress_raw(
    State(s): State<AppState>,
    Query(q): Query<FfmpegQuery>,
    body: Body,
) -> Response {
    match stream_body_to_temp(body, ".mp4").await {
        Ok(file) => respond_ffmpeg_compress(s, file, q.into_options()).await,
        Err(resp) => resp,
    }
}

async fn respond_ffmpeg_compress(
    s: AppState,
    input: NamedTempFile,
    opts: ffmpeg::FfmpegOptions,
) -> Response {
    let ffmpeg_bin = match require_ffmpeg(&s) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let out_name = format!("ff_{}.mp4", uuid::Uuid::new_v4());
    let out_path = PathBuf::from(&s.config.thumb_dir).join(&out_name);
    let in_path = input.path().to_path_buf();
    let file_name = temp_file_name(&input);
    let file_size = std::fs::metadata(&in_path).ok().map(|m| m.len());

    let meta = JobMeta {
        file_name,
        file_size,
        extra: Some(serde_json::json!({
            "engine": "ffmpeg",
            "mode": opts.mode.as_str(),
            "strip_exif": opts.strip_exif,
            "profile": opts.profile.map(|p| match p {
                ffmpeg::QualityProfile::Normal => "normal",
                ffmpeg::QualityProfile::Low => "low",
            }),
            "hwaccel": opts.hwaccel,
        })),
    };

    let out_check = out_path.clone();
    let download_name = out_name.clone();

    let id = job::spawn_job(
        s.registry,
        JobType::Compress,
        meta,
        move |cancelled| {
            if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("cancelled by user".into());
            }
            let result = ffmpeg::compress_one(&ffmpeg_bin, &in_path, &out_path, opts);
            drop(input);
            result
        },
        move || {
            std::fs::metadata(&out_check).ok().map(|m| {
                serde_json::json!({
                    "output_size": m.len(),
                    "output_path": download_name,
                    "download": format!("/ffmpeg/{download_name}"),
                })
            })
        },
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "ffmpeg compress job queued",
        "output_path": out_name,
    }))
    .into_response()
}

/// `POST /ffmpeg/qualities` → ZIP with normal.mp4 + low.mp4 (+ optional lossless.mp4)
async fn ffmpeg_qualities_multipart(
    State(s): State<AppState>,
    Query(q): Query<FfmpegQuery>,
    multipart: Multipart,
) -> Response {
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_ffmpeg_qualities(s, file, q.into_options()).await,
        Err(resp) => resp,
    }
}

async fn ffmpeg_qualities_raw(
    State(s): State<AppState>,
    Query(q): Query<FfmpegQuery>,
    body: Body,
) -> Response {
    match stream_body_to_temp(body, ".mp4").await {
        Ok(file) => respond_ffmpeg_qualities(s, file, q.into_options()).await,
        Err(resp) => resp,
    }
}

async fn respond_ffmpeg_qualities(
    s: AppState,
    input: NamedTempFile,
    opts: ffmpeg::FfmpegOptions,
) -> Response {
    let ffmpeg_bin = match require_ffmpeg(&s) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let out_name = format!("ff_qualities_{}.zip", uuid::Uuid::new_v4());
    let out_path = PathBuf::from(&s.config.thumb_dir).join(&out_name);
    let in_path = input.path().to_path_buf();
    let file_name = temp_file_name(&input);
    let file_size = std::fs::metadata(&in_path).ok().map(|m| m.len());

    let meta = JobMeta {
        file_name,
        file_size,
        extra: Some(serde_json::json!({
            "engine": "ffmpeg",
            "mode": "qualities",
            "strip_exif": opts.strip_exif,
            "include_lossless": opts.include_lossless,
            "hwaccel": opts.hwaccel,
        })),
    };

    let out_check = out_path.clone();
    let download_name = out_name.clone();

    let id = job::spawn_job(
        s.registry,
        JobType::Compress,
        meta,
        move |cancelled| {
            if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("cancelled by user".into());
            }
            let result = ffmpeg::compress_qualities_zip(&ffmpeg_bin, &in_path, &out_path, &opts);
            drop(input);
            result
        },
        move || {
            std::fs::metadata(&out_check).ok().map(|m| {
                serde_json::json!({
                    "zip_size": m.len(),
                    "output_path": download_name,
                    "download": format!("/ffmpeg/{download_name}"),
                })
            })
        },
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "ffmpeg multi-quality job queued",
        "output_path": out_name,
    }))
    .into_response()
}

/// `POST /ffmpeg/strip-exif` — remove metadata with stream copy (no re-encode).
async fn ffmpeg_strip_exif_multipart(State(s): State<AppState>, multipart: Multipart) -> Response {
    match stream_multipart_file(multipart).await {
        Ok(file) => respond_ffmpeg_strip_exif(s, file).await,
        Err(resp) => resp,
    }
}

async fn ffmpeg_strip_exif_raw(State(s): State<AppState>, body: Body) -> Response {
    match stream_body_to_temp(body, ".bin").await {
        Ok(file) => respond_ffmpeg_strip_exif(s, file).await,
        Err(resp) => resp,
    }
}

async fn respond_ffmpeg_strip_exif(s: AppState, input: NamedTempFile) -> Response {
    let ffmpeg_bin = match require_ffmpeg(&s) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let out_name = format!("ff_strip_{}.mp4", uuid::Uuid::new_v4());
    let out_path = PathBuf::from(&s.config.thumb_dir).join(&out_name);
    let in_path = input.path().to_path_buf();
    let file_name = temp_file_name(&input);
    let file_size = std::fs::metadata(&in_path).ok().map(|m| m.len());

    let meta = JobMeta {
        file_name,
        file_size,
        extra: Some(serde_json::json!({
            "engine": "ffmpeg",
            "mode": "strip_exif",
        })),
    };

    let out_check = out_path.clone();
    let download_name = out_name.clone();

    let id = job::spawn_job(
        s.registry,
        JobType::Compress,
        meta,
        move |cancelled| {
            if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("cancelled by user".into());
            }
            let result = ffmpeg::strip_exif(&ffmpeg_bin, &in_path, &out_path);
            drop(input);
            result
        },
        move || {
            std::fs::metadata(&out_check).ok().map(|m| {
                serde_json::json!({
                    "output_size": m.len(),
                    "output_path": download_name,
                    "download": format!("/ffmpeg/{download_name}"),
                })
            })
        },
    );

    Json(serde_json::json!({
        "ok": true,
        "job_id": id,
        "message": "ffmpeg strip-exif job queued",
        "output_path": out_name,
    }))
    .into_response()
}

async fn download_ffmpeg_output(
    axum::extract::Path(name): axum::extract::Path<String>,
    State(s): State<AppState>,
) -> Response {
    // Prevent path traversal
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return error_response(StatusCode::BAD_REQUEST, "invalid file name");
    }
    let file_path = PathBuf::from(&s.config.thumb_dir).join(&name);
    match std::fs::read(&file_path) {
        Ok(data) => {
            let ctype = if name.ends_with(".zip") {
                "application/zip"
            } else {
                "video/mp4"
            };
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, ctype)],
                data,
            )
                .into_response()
        }
        Err(_) => error_response(StatusCode::NOT_FOUND, &format!("file not found: {name}")),
    }
}
