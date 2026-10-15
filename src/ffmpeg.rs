//! FFmpeg-backed media processing: lossy / lossless compress, multi-quality, EXIF strip.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use zip::write::SimpleFileOptions;
use zip::CompressionMethod;
use zip::ZipWriter;

use std::fs::File;
use std::io::BufWriter;

static DETECTED_FFMPEG: OnceLock<Option<PathBuf>> = OnceLock::new();

/// Prefer configured path, then env, then known local installs, then PATH.
pub fn resolve_ffmpeg(configured: Option<&str>) -> Option<PathBuf> {
    if let Some(p) = configured.filter(|s| !s.is_empty()) {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Some(path);
        }
    }
    if let Ok(p) = std::env::var("FFMPEG_PATH") {
        if !p.is_empty() {
            let path = PathBuf::from(&p);
            if path.is_file() {
                return Some(path);
            }
            if which_ffmpeg(&p).is_some() {
                return Some(PathBuf::from(p));
            }
        }
    }
    DETECTED_FFMPEG
        .get_or_init(|| {
            let mut candidates: Vec<&str> = Vec::new();
            #[cfg(windows)]
            {
                candidates.extend([
                    r"C:\Users\makss\.scopeast\ffmpeg.exe",
                    r"C:\ffmpeg\bin\ffmpeg.exe",
                    "ffmpeg.exe",
                ]);
            }
            #[cfg(not(windows))]
            {
                candidates.extend([
                    "/usr/bin/ffmpeg",
                    "/usr/local/bin/ffmpeg",
                    "/bin/ffmpeg",
                ]);
            }
            candidates.push("ffmpeg");

            for c in candidates {
                let path = PathBuf::from(c);
                if path.is_file() {
                    return Some(path);
                }
                if which_ffmpeg(c).is_some() {
                    return Some(PathBuf::from(c));
                }
            }
            None
        })
        .clone()
}

/// Default hwaccel: env `FFMPEG_HWACCEL`, else CUDA when an NVIDIA device is present.
pub fn default_hwaccel() -> String {
    if let Ok(v) = std::env::var("FFMPEG_HWACCEL") {
        if !v.is_empty() {
            return v;
        }
    }
    if nvidia_available() {
        "cuda".into()
    } else {
        "none".into()
    }
}

fn nvidia_available() -> bool {
    #[cfg(not(windows))]
    {
        if Path::new("/dev/nvidia0").exists() {
            return true;
        }
    }
    let mut cmd = Command::new("nvidia-smi");
    cmd.arg("-L")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    cmd.status().map(|s| s.success()).unwrap_or(false)
}

fn which_ffmpeg(name: &str) -> Option<()> {
    let mut cmd = Command::new(name);
    cmd.arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    cmd.status().ok().filter(|s| s.success()).map(|_| ())
}

pub fn ffmpeg_available(configured: Option<&str>) -> bool {
    resolve_ffmpeg(configured).is_some()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressMode {
    Lossy,
    Lossless,
}

impl CompressMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "lossy" | "loss" | "normal" => Some(Self::Lossy),
            "lossless" | "raw" | "ffv1" => Some(Self::Lossless),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lossy => "lossy",
            Self::Lossless => "lossless",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityProfile {
    Normal,
    Low,
}

impl QualityProfile {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "normal" | "high" | "hq" => Some(Self::Normal),
            "low" | "weak" | "lq" | "small" => Some(Self::Low),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FfmpegOptions {
    pub mode: CompressMode,
    pub profile: Option<QualityProfile>,
    pub strip_exif: bool,
    pub bitrate: Option<String>,
    pub audio_bitrate: Option<String>,
    pub max_width: Option<u32>,
    pub fps: Option<u32>,
    pub crf: Option<i32>,
    pub preset: Option<String>,
    /// `auto` | `cuda` | `none`
    pub hwaccel: String,
    pub include_lossless: bool,
}

impl Default for FfmpegOptions {
    fn default() -> Self {
        Self {
            mode: CompressMode::Lossy,
            profile: None,
            strip_exif: true,
            bitrate: None,
            audio_bitrate: None,
            max_width: None,
            fps: None,
            crf: None,
            preset: None,
            hwaccel: default_hwaccel(),
            include_lossless: false,
        }
    }
}

impl FfmpegOptions {
    fn apply_profile(&mut self) {
        match self.profile {
            Some(QualityProfile::Normal) => {
                if self.max_width.is_none() {
                    self.max_width = Some(854);
                }
                if self.bitrate.is_none() {
                    self.bitrate = Some("1200k".into());
                }
                if self.audio_bitrate.is_none() {
                    self.audio_bitrate = Some("96k".into());
                }
                if self.fps.is_none() {
                    self.fps = Some(24);
                }
            }
            Some(QualityProfile::Low) => {
                if self.max_width.is_none() {
                    self.max_width = Some(480);
                }
                if self.bitrate.is_none() {
                    self.bitrate = Some("350k".into());
                }
                if self.audio_bitrate.is_none() {
                    self.audio_bitrate = Some("48k".into());
                }
                if self.fps.is_none() {
                    self.fps = Some(12);
                }
            }
            None => {
                if self.bitrate.is_none() && self.crf.is_none() && self.mode == CompressMode::Lossy
                {
                    self.bitrate = Some("800k".into());
                }
                if self.audio_bitrate.is_none() {
                    self.audio_bitrate = Some("96k".into());
                }
            }
        }
    }
}

fn run_ffmpeg(ffmpeg: &Path, args: &[String]) -> Result<(), String> {
    tracing::info!(bin = %ffmpeg.display(), ?args, "ffmpeg invoke");
    let mut cmd = Command::new(ffmpeg);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    let output = cmd
        .output()
        .map_err(|e| format!("failed to spawn ffmpeg: {e}"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let tail: String = stderr
        .lines()
        .rev()
        .take(20)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n");
    Err(format!(
        "ffmpeg failed ({}): {}",
        output.status,
        if tail.is_empty() {
            "no stderr".into()
        } else {
            tail
        }
    ))
}

fn hwaccel_args(hwaccel: &str) -> Vec<String> {
    match hwaccel.to_ascii_lowercase().as_str() {
        "none" | "off" | "cpu" => Vec::new(),
        "cuda" => vec!["-hwaccel".into(), "cuda".into()],
        // auto: CUDA only when an NVIDIA device is visible (safe for Docker/CPU)
        "auto" => {
            if nvidia_available() {
                vec!["-hwaccel".into(), "cuda".into()]
            } else {
                Vec::new()
            }
        }
        _ => {
            if nvidia_available() {
                vec!["-hwaccel".into(), "cuda".into()]
            } else {
                Vec::new()
            }
        }
    }
}

fn strip_metadata_args(strip: bool) -> Vec<String> {
    if strip {
        vec![
            "-map_metadata".into(),
            "-1".into(),
            "-map_chapters".into(),
            "-1".into(),
        ]
    } else {
        Vec::new()
    }
}

fn vf_scale_fps(max_width: Option<u32>, fps: Option<u32>) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(w) = max_width.filter(|w| *w > 0) {
        // even dimensions for H.264
        parts.push(format!("scale='min({w},iw)':-2"));
    }
    if let Some(f) = fps.filter(|f| *f > 0) {
        parts.push(format!("fps={f}"));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(","))
    }
}

/// Strip container/stream metadata (EXIF / tags) with stream copy when possible.
pub fn strip_exif(ffmpeg: &Path, input: &Path, output: &Path) -> Result<(), String> {
    let mut args = vec![
        "-y".into(),
        "-hide_banner".into(),
        "-loglevel".into(),
        "error".into(),
        "-i".into(),
        input.display().to_string(),
    ];
    args.extend(strip_metadata_args(true));
    args.extend([
        "-c".into(),
        "copy".into(),
        "-movflags".into(),
        "+faststart".into(),
        output.display().to_string(),
    ]);
    run_ffmpeg(ffmpeg, &args)?;
    ensure_output(output)
}

/// Single-file compress: lossy (H.264 NVENC/x264) or lossless (H.264 CRF 0).
pub fn compress_one(
    ffmpeg: &Path,
    input: &Path,
    output: &Path,
    mut opts: FfmpegOptions,
) -> Result<(), String> {
    opts.apply_profile();
    match opts.mode {
        CompressMode::Lossy => compress_lossy(ffmpeg, input, output, &opts),
        CompressMode::Lossless => compress_lossless(ffmpeg, input, output, &opts),
    }
}

fn prefer_nvenc(opts: &FfmpegOptions) -> bool {
    let hw = opts.hwaccel.to_ascii_lowercase();
    if matches!(hw.as_str(), "none" | "off" | "cpu") {
        return false;
    }
    nvidia_available()
}

fn compress_lossy(
    ffmpeg: &Path,
    input: &Path,
    output: &Path,
    opts: &FfmpegOptions,
) -> Result<(), String> {
    if !prefer_nvenc(opts) {
        return compress_lossy_x264(ffmpeg, input, output, opts);
    }

    let mut args = Vec::new();
    args.push("-y".into());
    args.push("-hide_banner".into());
    args.push("-loglevel".into());
    args.push("error".into());
    args.extend(hwaccel_args(&opts.hwaccel));
    args.push("-i".into());
    args.push(input.display().to_string());
    args.extend(strip_metadata_args(opts.strip_exif));

    if let Some(vf) = vf_scale_fps(opts.max_width, opts.fps) {
        args.push("-vf".into());
        args.push(vf);
    }

    let preset = opts.preset.as_deref().unwrap_or("p1");
    args.extend([
        "-c:v".into(),
        "h264_nvenc".into(),
        "-preset".into(),
        preset.into(),
        "-tune".into(),
        "ull".into(),
        "-rc".into(),
        "vbr".into(),
    ]);
    if let Some(ref br) = opts.bitrate {
        args.push("-b:v".into());
        args.push(br.clone());
        args.push("-maxrate".into());
        args.push(br.clone());
        args.push("-bufsize".into());
        args.push(double_rate(br));
    } else if let Some(crf) = opts.crf {
        args.push("-cq".into());
        args.push(crf.clamp(0, 51).to_string());
    }

    args.extend(audio_encode_args(opts.audio_bitrate.as_deref().unwrap_or("96k")));
    args.extend([
        "-movflags".into(),
        "+faststart".into(),
        output.display().to_string(),
    ]);

    match run_ffmpeg(ffmpeg, &args) {
        Ok(()) => ensure_output(output),
        Err(nvenc_err) => {
            tracing::warn!(error = %nvenc_err, "NVENC failed, falling back to libx264");
            compress_lossy_x264(ffmpeg, input, output, opts)
        }
    }
}

fn compress_lossy_x264(
    ffmpeg: &Path,
    input: &Path,
    output: &Path,
    opts: &FfmpegOptions,
) -> Result<(), String> {
    let mut args = vec![
        "-y".into(),
        "-hide_banner".into(),
        "-loglevel".into(),
        "error".into(),
        "-i".into(),
        input.display().to_string(),
    ];
    args.extend(strip_metadata_args(opts.strip_exif));
    if let Some(vf) = vf_scale_fps(opts.max_width, opts.fps) {
        args.push("-vf".into());
        args.push(vf);
    }
    let preset = opts.preset.as_deref().unwrap_or("veryfast");
    args.extend([
        "-c:v".into(),
        "libx264".into(),
        "-preset".into(),
        preset.into(),
        "-pix_fmt".into(),
        "yuv420p".into(),
    ]);
    if let Some(ref br) = opts.bitrate {
        args.push("-b:v".into());
        args.push(br.clone());
    } else {
        let crf = opts.crf.unwrap_or(28).clamp(0, 51);
        args.push("-crf".into());
        args.push(crf.to_string());
    }
    args.extend(audio_encode_args(opts.audio_bitrate.as_deref().unwrap_or("96k")));
    args.extend([
        "-movflags".into(),
        "+faststart".into(),
        output.display().to_string(),
    ]);
    run_ffmpeg(ffmpeg, &args)?;
    ensure_output(output)
}

fn compress_lossless(
    ffmpeg: &Path,
    input: &Path,
    output: &Path,
    opts: &FfmpegOptions,
) -> Result<(), String> {
    let mut args = Vec::new();
    args.push("-y".into());
    args.push("-hide_banner".into());
    args.push("-loglevel".into());
    args.push("error".into());
    args.extend(hwaccel_args(&opts.hwaccel));
    args.push("-i".into());
    args.push(input.display().to_string());
    args.extend(strip_metadata_args(opts.strip_exif));

    if let Some(vf) = vf_scale_fps(opts.max_width, opts.fps) {
        args.push("-vf".into());
        args.push(vf);
    }

    let encode_x264 = |base: Vec<String>| -> Result<(), String> {
        let mut xargs = base;
        xargs.extend([
            "-c:v".into(),
            "libx264".into(),
            "-preset".into(),
            "ultrafast".into(),
            "-crf".into(),
            "0".into(),
            "-pix_fmt".into(),
            "yuv420p".into(),
        ]);
        xargs.extend(audio_encode_args(
            opts.audio_bitrate.as_deref().unwrap_or("192k"),
        ));
        xargs.extend([
            "-movflags".into(),
            "+faststart".into(),
            output.display().to_string(),
        ]);
        run_ffmpeg(ffmpeg, &xargs)?;
        ensure_output(output)
    };

    if !prefer_nvenc(opts) {
        return encode_x264(args);
    }

    let mut nvenc_args = args.clone();
    nvenc_args.extend([
        "-c:v".into(),
        "h264_nvenc".into(),
        "-preset".into(),
        "p1".into(),
        "-tune".into(),
        "lossless".into(),
        "-pix_fmt".into(),
        "yuv420p".into(),
    ]);
    nvenc_args.extend(audio_encode_args(
        opts.audio_bitrate.as_deref().unwrap_or("192k"),
    ));
    nvenc_args.extend([
        "-movflags".into(),
        "+faststart".into(),
        output.display().to_string(),
    ]);

    match run_ffmpeg(ffmpeg, &nvenc_args) {
        Ok(()) => ensure_output(output),
        Err(e) => {
            tracing::warn!(error = %e, "NVENC lossless failed, falling back to libx264 CRF 0");
            encode_x264(args)
        }
    }
}

fn audio_encode_args(bitrate: &str) -> Vec<String> {
    vec![
        "-c:a".into(),
        "aac".into(),
        "-b:a".into(),
        bitrate.into(),
        "-ac".into(),
        "2".into(),
    ]
}

fn double_rate(br: &str) -> String {
    let s = br.trim();
    if let Some(num) = s.strip_suffix('k').or_else(|| s.strip_suffix('K')) {
        if let Ok(v) = num.parse::<f64>() {
            return format!("{}k", (v * 2.0).round());
        }
    }
    if let Some(num) = s.strip_suffix('M').or_else(|| s.strip_suffix('m')) {
        if let Ok(v) = num.parse::<f64>() {
            return format!("{}M", v * 2.0);
        }
    }
    format!("{s}")
}

fn ensure_output(path: &Path) -> Result<(), String> {
    let meta = std::fs::metadata(path).map_err(|e| format!("missing output: {e}"))?;
    if meta.len() == 0 {
        return Err("ffmpeg produced empty output".into());
    }
    Ok(())
}

/// One decode → normal + low (+ optional lossless) packed into a ZIP.
pub fn compress_qualities_zip(
    ffmpeg: &Path,
    input: &Path,
    zip_out: &Path,
    opts: &FfmpegOptions,
) -> Result<(), String> {
    let dir = zip_out
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let id = std::process::id();
    let normal_path = dir.join(format!("agregat-ff-normal-{id}.mp4"));
    let low_path = dir.join(format!("agregat-ff-low-{id}.mp4"));
    let lossless_path = dir.join(format!("agregat-ff-lossless-{id}.mp4"));

    let result = (|| {
        compress_dual_lossy(ffmpeg, input, &normal_path, &low_path, opts)?;

        if opts.include_lossless {
            let mut ll = opts.clone();
            ll.mode = CompressMode::Lossless;
            ll.profile = None;
            ll.bitrate = None;
            ll.fps = None;
            ll.max_width = None;
            compress_one(ffmpeg, input, &lossless_path, ll)?;
        }

        write_qualities_zip(
            zip_out,
            &normal_path,
            &low_path,
            if opts.include_lossless {
                Some(lossless_path.as_path())
            } else {
                None
            },
        )
    })();

    let _ = std::fs::remove_file(&normal_path);
    let _ = std::fs::remove_file(&low_path);
    let _ = std::fs::remove_file(&lossless_path);
    result
}

/// Single-decode dual-encode (filter_complex split) for messenger qualities.
fn compress_dual_lossy(
    ffmpeg: &Path,
    input: &Path,
    normal_out: &Path,
    low_out: &Path,
    opts: &FfmpegOptions,
) -> Result<(), String> {
    let filter = "\
[0:v]split=2[v0][v1];\
[v0]scale='min(854,iw)':-2,fps=24[n];\
[v1]scale='min(480,iw)':-2,fps=12[l]";

    let use_nvenc = prefer_nvenc(opts);
    let (vcodec, preset_n, tune_n, preset_l, tune_l) = if use_nvenc {
        ("h264_nvenc", "p1", Some("ull"), "p1", Some("ull"))
    } else {
        ("libx264", "veryfast", None, "veryfast", None)
    };

    let mut args = Vec::new();
    args.push("-y".into());
    args.push("-hide_banner".into());
    args.push("-loglevel".into());
    args.push("error".into());
    args.extend(hwaccel_args(&opts.hwaccel));
    args.push("-i".into());
    args.push(input.display().to_string());
    args.push("-filter_complex".into());
    args.push(filter.into());
    args.extend(strip_metadata_args(opts.strip_exif));

    args.extend([
        "-map".into(),
        "[n]".into(),
        "-map".into(),
        "0:a?".into(),
        "-c:v".into(),
        vcodec.into(),
        "-preset".into(),
        preset_n.into(),
    ]);
    if let Some(t) = tune_n {
        args.extend(["-tune".into(), t.into()]);
    }
    args.extend([
        "-b:v".into(),
        "1200k".into(),
        "-maxrate".into(),
        "1200k".into(),
        "-bufsize".into(),
        "2400k".into(),
        "-pix_fmt".into(),
        "yuv420p".into(),
        "-c:a".into(),
        "aac".into(),
        "-b:a".into(),
        "96k".into(),
        "-ac".into(),
        "2".into(),
        "-movflags".into(),
        "+faststart".into(),
        normal_out.display().to_string(),
    ]);

    args.extend([
        "-map".into(),
        "[l]".into(),
        "-map".into(),
        "0:a?".into(),
        "-c:v".into(),
        vcodec.into(),
        "-preset".into(),
        preset_l.into(),
    ]);
    if let Some(t) = tune_l {
        args.extend(["-tune".into(), t.into()]);
    }
    args.extend([
        "-b:v".into(),
        "350k".into(),
        "-maxrate".into(),
        "350k".into(),
        "-bufsize".into(),
        "700k".into(),
        "-pix_fmt".into(),
        "yuv420p".into(),
        "-c:a".into(),
        "aac".into(),
        "-b:a".into(),
        "48k".into(),
        "-ac".into(),
        "2".into(),
        "-movflags".into(),
        "+faststart".into(),
        low_out.display().to_string(),
    ]);

    match run_ffmpeg(ffmpeg, &args) {
        Ok(()) => {
            ensure_output(normal_out)?;
            ensure_output(low_out)?;
            Ok(())
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "dual encode failed, encoding qualities sequentially"
            );
            let mut normal_opts = opts.clone();
            normal_opts.mode = CompressMode::Lossy;
            normal_opts.profile = Some(QualityProfile::Normal);
            compress_one(ffmpeg, input, normal_out, normal_opts)?;

            let mut low_opts = opts.clone();
            low_opts.mode = CompressMode::Lossy;
            low_opts.profile = Some(QualityProfile::Low);
            compress_one(ffmpeg, input, low_out, low_opts)?;
            Ok(())
        }
    }
}

fn write_qualities_zip(
    zip_out: &Path,
    normal: &Path,
    low: &Path,
    lossless: Option<&Path>,
) -> Result<(), String> {
    let file = File::create(zip_out).map_err(|e| e.to_string())?;
    let mut zip = ZipWriter::new(BufWriter::new(file));
    let opts = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);

    zip.start_file("normal.mp4", opts)
        .map_err(|e| e.to_string())?;
    std::io::copy(&mut File::open(normal).map_err(|e| e.to_string())?, &mut zip)
        .map_err(|e| e.to_string())?;

    zip.start_file("low.mp4", opts)
        .map_err(|e| e.to_string())?;
    std::io::copy(&mut File::open(low).map_err(|e| e.to_string())?, &mut zip)
        .map_err(|e| e.to_string())?;

    if let Some(ll) = lossless {
        zip.start_file("lossless.mp4", opts)
            .map_err(|e| e.to_string())?;
        std::io::copy(&mut File::open(ll).map_err(|e| e.to_string())?, &mut zip)
            .map_err(|e| e.to_string())?;
    }

    zip.finish().map_err(|e| e.to_string())?;
    ensure_output(zip_out)
}
