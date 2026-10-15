//! Configuration — loaded from config.json on disk, then overridden by CLI args.

use std::path::Path;

/// Shared state — config + job registry.
#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub registry: crate::job::JobRegistry,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub max_upload_bytes: usize,
    pub thumb_dir: String,
    pub workers_multiplier: usize,
    pub prune_secs: u64,
    /// Thumbnail defaults (overridable via query params).
    pub thumb_width: u32,
    pub thumb_jpeg_quality: u8,
    /// pHash video frame extraction size.
    pub hash_video_frame_size: u32,
    /// Optional absolute path to ffmpeg.exe (else env / auto-detect).
    pub ffmpeg_path: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 7528,
            max_upload_bytes: 2 * 1024 * 1024 * 1024, // 2 GiB
            thumb_dir: "output".to_string(),
            workers_multiplier: 2,
            prune_secs: 3600,
            thumb_width: 256,
            thumb_jpeg_quality: 12,
            hash_video_frame_size: 64,
            ffmpeg_path: None,
        }
    }
}

/// Load config from a JSON file if it exists, falling back to defaults for missing keys.
fn load_config_from_file(path: &Path) -> Option<Config> {
    let data = std::fs::read_to_string(path).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&data).ok()?;
    let mut cfg = Config::default();

    if let Some(v) = parsed.get("port").and_then(|v| v.as_u64()) {
        if v > 0 && v <= u16::MAX as u64 {
            cfg.port = v as u16;
        }
    }
    if let Some(v) = parsed.get("max_upload_bytes").and_then(|v| v.as_u64()) {
        if v > 0 {
            cfg.max_upload_bytes = v as usize;
        }
    }
    if let Some(v) = parsed
        .get("thumb_dir")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
    {
        if !v.is_empty() {
            cfg.thumb_dir = v;
        }
    }
    if let Some(v) = parsed.get("workers_multiplier").and_then(|v| v.as_u64()) {
        if v > 0 {
            cfg.workers_multiplier = v as usize;
        }
    }
    if let Some(v) = parsed.get("prune_secs").and_then(|v| v.as_u64()) {
        if v > 0 {
            cfg.prune_secs = v;
        }
    }

    if let Some(thumb) = parsed.get("thumbnail") {
        if let Some(v) = thumb.get("width").and_then(|v| v.as_u64()) {
            if v > 0 {
                cfg.thumb_width = v as u32;
            }
        }
        if let Some(v) = thumb.get("jpeg_quality").and_then(|v| v.as_u64()) {
            if v > 0 && v <= 100 {
                cfg.thumb_jpeg_quality = v as u8;
            }
        }
    }

    if let Some(hash) = parsed.get("hash") {
        if let Some(v) = hash.get("video_frame_size").and_then(|v| v.as_u64()) {
            if v > 0 {
                cfg.hash_video_frame_size = v as u32;
            }
        }
    }

    if let Some(v) = parsed
        .get("ffmpeg_path")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
    {
        if !v.is_empty() {
            cfg.ffmpeg_path = Some(v);
        }
    }

    Some(cfg)
}

pub fn load_config() -> Config {
    let mut cfg = load_config_from_file(Path::new("config.json")).unwrap_or_default();

    // CLI args override file config
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                if let Some(v) = args.get(i + 1) {
                    cfg.port = v.parse::<u16>().unwrap_or(cfg.port);
                    i += 2;
                    continue;
                }
            }
            "--max-upload" => {
                if let Some(v) = args.get(i + 1) {
                    cfg.max_upload_bytes = v.parse::<usize>().unwrap_or(cfg.max_upload_bytes);
                    i += 2;
                    continue;
                }
            }
            "--thumb-dir" => {
                if let Some(v) = args.get(i + 1) {
                    cfg.thumb_dir = v.clone();
                    i += 2;
                    continue;
                }
            }
            "--workers" | "-j" => {
                if let Some(v) = args.get(i + 1) {
                    if let Ok(n) = v.parse::<usize>() {
                        cfg.workers_multiplier = n.max(1);
                    }
                    i += 2;
                    continue;
                }
            }
            "--prune-secs" => {
                if let Some(v) = args.get(i + 1) {
                    cfg.prune_secs = v.parse::<u64>().unwrap_or(cfg.prune_secs);
                    i += 2;
                    continue;
                }
            }
            "--ffmpeg" => {
                if let Some(v) = args.get(i + 1) {
                    cfg.ffmpeg_path = Some(v.clone());
                    i += 2;
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }
    cfg
}
