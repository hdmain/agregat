//! Re-encode video for messenger: normal + low quality variants.

use rumpeg::codec::aac::AacEncoderCodec;
use rumpeg::codec::{self, DecoderContext, EncoderContext};
use rumpeg::filter::Graph;
use rumpeg::format::{self, FormatContext, OutputContext, Stream};
use rumpeg::{
    CodecId, CodecParams, Error, Frame, MediaType, PixelFormat, Rational, Result, SampleFormat,
    VideoParams,
};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::{self, JoinHandle};
use zip::write::SimpleFileOptions;
use zip::CompressionMethod;
use zip::ZipWriter;

/// Default target video bitrate when none is specified.
pub const DEFAULT_BITRATE: &str = "800k";
/// Default AAC bitrate when source has audio.
pub const DEFAULT_AUDIO_BITRATE: &str = "96k";
/// Default CRF when neither bitrate nor qp is given and caller asks for crf-style.
pub const DEFAULT_CRF: i32 = 28;
/// Default GOP / IDR interval.
pub const DEFAULT_GOP: u32 = 60;
/// Default max width in fast mode (matches rumpeg `--fast`).
pub const FAST_MAX_WIDTH: u32 = 640;
/// Default encode preset for rusty_h264 / NVENC hints.
pub const DEFAULT_PRESET: &str = "fast";
/// Prefer NVENC on Windows; pure-Rust H.264 elsewhere (Docker/Linux).
#[cfg(windows)]
pub const DEFAULT_ENCODER: &str = "h264_nvenc";
#[cfg(not(windows))]
pub const DEFAULT_ENCODER: &str = "h264";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Chat “good enough” — ~720p-class, keeps source fps.
    Normal,
    /// Weak network — small + fewer fps.
    Low,
}

impl Profile {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "normal" | "high" | "hq" => Some(Self::Normal),
            "low" | "weak" | "lq" | "small" => Some(Self::Low),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompressOptions {
    pub bitrate: Option<String>,
    pub audio_bitrate: Option<String>,
    pub crf: Option<i32>,
    pub qp: Option<i32>,
    pub gop: Option<u32>,
    pub max_width: Option<u32>,
    /// Drop/duplicate to this fps when set (big speed win).
    pub fps: Option<u32>,
    pub fast: bool,
    pub preset: Option<String>,
    pub encoder: Option<String>,
    pub profile: Option<Profile>,
}

impl Default for CompressOptions {
    fn default() -> Self {
        Self {
            bitrate: None,
            audio_bitrate: None,
            crf: None,
            qp: None,
            gop: None,
            max_width: None,
            fps: None,
            fast: false,
            preset: None,
            encoder: None,
            profile: None,
        }
    }
}

impl CompressOptions {
    pub fn messenger_normal() -> Self {
        Self {
            profile: Some(Profile::Normal),
            ..Self::default()
        }
        .with_defaults()
    }

    pub fn messenger_low() -> Self {
        Self {
            profile: Some(Profile::Low),
            ..Self::default()
        }
        .with_defaults()
    }

    pub fn with_defaults(mut self) -> Self {
        if let Some(profile) = self.profile {
            match profile {
                Profile::Normal => {
                    if self.max_width.is_none() {
                        self.max_width = Some(854);
                    }
                    if self.bitrate.is_none() && self.crf.is_none() && self.qp.is_none() {
                        self.bitrate = Some("1200k".into());
                    }
                    if self.audio_bitrate.is_none() {
                        self.audio_bitrate = Some("96k".into());
                    }
                    if self.fps.is_none() {
                        self.fps = Some(24);
                    }
                }
                Profile::Low => {
                    if self.max_width.is_none() {
                        self.max_width = Some(480);
                    }
                    if self.bitrate.is_none() && self.crf.is_none() && self.qp.is_none() {
                        self.bitrate = Some("350k".into());
                    }
                    if self.audio_bitrate.is_none() {
                        self.audio_bitrate = Some("48k".into());
                    }
                    if self.fps.is_none() {
                        self.fps = Some(12);
                    }
                }
            }
            if self.preset.is_none() {
                self.preset = Some("fast".into());
            }
            if self.encoder.is_none() {
                self.encoder = Some(DEFAULT_ENCODER.into());
            }
            if self.gop.is_none() {
                self.gop = Some(DEFAULT_GOP);
            }
            return self;
        }

        if self.qp.is_none() && self.crf.is_none() && self.bitrate.is_none() {
            self.bitrate = Some(DEFAULT_BITRATE.to_string());
        }
        if self.audio_bitrate.is_none() {
            self.audio_bitrate = Some(DEFAULT_AUDIO_BITRATE.to_string());
        }
        if self.gop.is_none() {
            self.gop = Some(DEFAULT_GOP);
        }
        if self.fast && self.max_width.is_none() {
            self.max_width = Some(FAST_MAX_WIDTH);
        }
        if self.fast && self.fps.is_none() {
            self.fps = Some(15);
        }
        if self.preset.is_none() {
            self.preset = Some(DEFAULT_PRESET.to_string());
        }
        if self.encoder.is_none() {
            self.encoder = Some(DEFAULT_ENCODER.to_string());
        }
        self
    }
}

/// Re-encode `input` → one H.264(+AAC) MP4.
pub fn compress_video(input: &Path, output: &Path, opts: &CompressOptions) -> Result<()> {
    rumpeg::init();
    let opts = opts.clone().with_defaults();
    let mut pipeline = Pipeline::open(input, &[&opts], &[output])?;
    pipeline.run()?;
    Ok(())
}

/// One decode → two encodes (normal + low). Writes a ZIP with `normal.mp4` + `low.mp4`.
pub fn compress_messenger_dual(input: &Path, zip_out: &Path) -> Result<()> {
    rumpeg::init();
    let normal = CompressOptions::messenger_normal();
    let low = CompressOptions::messenger_low();

    let dir = zip_out
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let normal_path = dir.join(format!(
        "agregat-normal-{}.mp4",
        std::process::id()
    ));
    let low_path = dir.join(format!("agregat-low-{}.mp4", std::process::id()));

    let result = (|| {
        let mut pipeline = Pipeline::open(input, &[&normal, &low], &[&normal_path, &low_path])?;
        pipeline.run()?;
        write_zip(zip_out, &normal_path, &low_path)?;
        Ok(())
    })();

    let _ = std::fs::remove_file(&normal_path);
    let _ = std::fs::remove_file(&low_path);
    result
}

fn write_zip(zip_out: &Path, normal: &Path, low: &Path) -> Result<()> {
    let file = File::create(zip_out).map_err(Error::Io)?;
    let mut zip = ZipWriter::new(BufWriter::new(file));
    let opts = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    zip.start_file("normal.mp4", opts)
        .map_err(|e| Error::Other(format!("zip: {e}")))?;
    std::io::copy(&mut File::open(normal).map_err(Error::Io)?, &mut zip)
        .map_err(Error::Io)?;
    zip.start_file("low.mp4", opts)
        .map_err(|e| Error::Other(format!("zip: {e}")))?;
    std::io::copy(&mut File::open(low).map_err(Error::Io)?, &mut zip).map_err(Error::Io)?;
    zip.finish()
        .map_err(|e| Error::Other(format!("zip finish: {e}")))?;
    Ok(())
}

struct Branch {
    graph: Graph,
    encoder: EncoderContext,
    muxer: OutputContext,
    out_v: usize,
    audio_encoder: Option<EncoderContext>,
    out_a: Option<usize>,
    frames: u64,
}

struct Pipeline {
    demuxer: FormatContext,
    video_index: usize,
    audio_index: Option<usize>,
    decoder: DecoderContext,
    audio_decoder: Option<DecoderContext>,
    branches: Vec<Branch>,
    source_fps: u32,
}

impl Pipeline {
    fn open(input: &Path, opts_list: &[&CompressOptions], outputs: &[&Path]) -> Result<Self> {
        assert_eq!(opts_list.len(), outputs.len());
        let demuxer = FormatContext::open(input)?;
        let in_stream = demuxer
            .best_stream(MediaType::Video)
            .ok_or_else(|| Error::not_found("no video stream"))?
            .clone();
        let audio_in = demuxer.best_stream(MediaType::Audio).cloned();

        let decoder = DecoderContext::open(&in_stream.codec_params)?;
        let audio_decoder = if let Some(ref a) = audio_in {
            Some(DecoderContext::open(&a.codec_params)?)
        } else {
            None
        };

        let source_fps = in_stream
            .codec_params
            .video()
            .map(|v| {
                let f = v.frame_rate.as_f64();
                if f > 0.0 {
                    f.round().max(1.0) as u32
                } else {
                    25
                }
            })
            .unwrap_or(25);

        let src_w = in_stream
            .codec_params
            .video()
            .map(|v| v.width.max(1))
            .unwrap_or(1);
        let src_h = in_stream
            .codec_params
            .video()
            .map(|v| v.height.max(1))
            .unwrap_or(1);

        let mut branches = Vec::with_capacity(opts_list.len());
        for (opts, output) in opts_list.iter().zip(outputs.iter()) {
            branches.push(open_branch(
                opts,
                output,
                &in_stream,
                audio_in.as_ref(),
                source_fps,
                src_w,
                src_h,
            )?);
        }

        Ok(Self {
            demuxer,
            video_index: in_stream.index,
            audio_index: audio_in.as_ref().map(|s| s.index),
            decoder,
            audio_decoder,
            branches,
            source_fps,
        })
    }

    fn run(&mut self) -> Result<()> {
        let _ = self.source_fps;
        let branches = std::mem::take(&mut self.branches);
        if branches.is_empty() {
            return Err(Error::not_found("no encode branches"));
        }

        let cpus = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        // Prefetch so soft-decode overlaps with scale + YUV→ARGB + NVENC.
        let queue_depth = (cpus * 2).clamp(8, 64);
        tracing::info!(
            workers = branches.len(),
            cpus,
            queue_depth,
            "compress pipeline: decode thread + parallel encode workers"
        );

        let mut senders: Vec<SyncSender<WorkItem>> = Vec::with_capacity(branches.len());
        let mut joins: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(branches.len());

        for (i, branch) in branches.into_iter().enumerate() {
            let (tx, rx) = sync_channel::<WorkItem>(queue_depth);
            senders.push(tx);
            joins.push(
                thread::Builder::new()
                    .name(format!("agregat-enc-{i}"))
                    .spawn(move || encode_worker(i, branch, rx))
                    .map_err(|e| Error::Other(format!("spawn encode worker: {e}")))?,
            );
        }

        let distribute = |frame: Frame, video: bool| -> Result<()> {
            let n = senders.len();
            if n == 0 {
                return Ok(());
            }
            if n == 1 {
                let item = if video {
                    WorkItem::Video(frame)
                } else {
                    WorkItem::Audio(frame)
                };
                return senders[0]
                    .send(item)
                    .map_err(|_| Error::Other("encode worker stopped".into()));
            }
            for tx in &senders[..n - 1] {
                let item = if video {
                    WorkItem::Video(frame.clone())
                } else {
                    WorkItem::Audio(frame.clone())
                };
                tx.send(item)
                    .map_err(|_| Error::Other("encode worker stopped".into()))?;
            }
            let item = if video {
                WorkItem::Video(frame)
            } else {
                WorkItem::Audio(frame)
            };
            senders[n - 1]
                .send(item)
                .map_err(|_| Error::Other("encode worker stopped".into()))
        };

        loop {
            let packet = match self.demuxer.read_packet() {
                Ok(p) => p,
                Err(Error::Eof) => break,
                Err(e) => {
                    stop_workers(&senders);
                    let _ = join_workers(joins);
                    return Err(e);
                }
            };
            if packet.stream_index == self.video_index {
                for frame in self.decoder.decode(&packet)? {
                    if let Err(e) = distribute(frame, true) {
                        stop_workers(&senders);
                        let _ = join_workers(joins);
                        return Err(e);
                    }
                }
            } else if Some(packet.stream_index) == self.audio_index {
                if let Some(adec) = self.audio_decoder.as_mut() {
                    for frame in adec.decode(&packet)? {
                        if let Err(e) = distribute(frame, false) {
                            stop_workers(&senders);
                            let _ = join_workers(joins);
                            return Err(e);
                        }
                    }
                }
            }
        }

        self.decoder.send_eof()?;
        loop {
            match self.decoder.receive_frame() {
                Ok(frame) => {
                    if let Err(e) = distribute(frame, true) {
                        stop_workers(&senders);
                        let _ = join_workers(joins);
                        return Err(e);
                    }
                }
                Err(Error::Eof) | Err(Error::NeedMoreData) => break,
                Err(e) => {
                    stop_workers(&senders);
                    let _ = join_workers(joins);
                    return Err(e);
                }
            }
        }

        if let Some(adec) = self.audio_decoder.as_mut() {
            adec.send_eof()?;
            loop {
                match adec.receive_frame() {
                    Ok(frame) => {
                        if let Err(e) = distribute(frame, false) {
                            stop_workers(&senders);
                            let _ = join_workers(joins);
                            return Err(e);
                        }
                    }
                    Err(Error::Eof) | Err(Error::NeedMoreData) => break,
                    Err(e) => {
                        stop_workers(&senders);
                        let _ = join_workers(joins);
                        return Err(e);
                    }
                }
            }
        }

        stop_workers(&senders);
        join_workers(joins)
    }
}

enum WorkItem {
    Video(Frame),
    Audio(Frame),
    Eof,
}

fn stop_workers(senders: &[SyncSender<WorkItem>]) {
    for tx in senders {
        let _ = tx.send(WorkItem::Eof);
    }
}

fn join_workers(joins: Vec<JoinHandle<Result<()>>>) -> Result<()> {
    let mut first_err = None;
    for (i, join) in joins.into_iter().enumerate() {
        match join.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(branch = i, error = %e, "encode worker failed");
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(_) => {
                let e = Error::Other(format!("encode worker {i} panicked"));
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn encode_worker(branch_i: usize, mut branch: Branch, rx: Receiver<WorkItem>) -> Result<()> {
    while let Ok(item) = rx.recv() {
        match item {
            WorkItem::Video(frame) => push_video(&mut branch, branch_i, frame)?,
            WorkItem::Audio(frame) => push_audio(&mut branch, &frame)?,
            WorkItem::Eof => break,
        }
    }
    finalize_branch(branch_i, &mut branch)
}

fn push_video(branch: &mut Branch, branch_i: usize, frame: Frame) -> Result<()> {
    for frame in branch.graph.run_all(frame)? {
        for mut pkt in branch.encoder.encode(&frame)? {
            pkt.stream_index = branch.out_v;
            branch.muxer.write_packet(&pkt)?;
        }
        branch.frames += 1;
        if branch.frames % 120 == 0 {
            tracing::info!(branch = branch_i, frames = branch.frames, "compress progress");
        }
    }
    Ok(())
}

fn push_audio(branch: &mut Branch, frame: &Frame) -> Result<()> {
    if let (Some(aenc), Some(ai)) = (branch.audio_encoder.as_mut(), branch.out_a) {
        for mut pkt in aenc.encode(frame)? {
            pkt.stream_index = ai;
            branch.muxer.write_packet(&pkt)?;
        }
    }
    Ok(())
}

fn finalize_branch(branch_i: usize, branch: &mut Branch) -> Result<()> {
    branch.encoder.send_eof()?;
    loop {
        match branch.encoder.receive_packet() {
            Ok(mut pkt) => {
                pkt.stream_index = branch.out_v;
                branch.muxer.write_packet(&pkt)?;
            }
            Err(Error::Eof) | Err(Error::NeedMoreData) => break,
            Err(e) => return Err(e),
        }
    }
    if let (Some(aenc), Some(ai)) = (branch.audio_encoder.as_mut(), branch.out_a) {
        aenc.send_eof()?;
        loop {
            match aenc.receive_packet() {
                Ok(mut pkt) => {
                    pkt.stream_index = ai;
                    branch.muxer.write_packet(&pkt)?;
                }
                Err(Error::Eof) | Err(Error::NeedMoreData) => break,
                Err(e) => return Err(e),
            }
        }
    }
    branch.muxer.write_trailer()?;
    if branch.frames == 0 {
        return Err(Error::not_found("no video frames encoded"));
    }
    tracing::info!(
        branch = branch_i,
        frames = branch.frames,
        audio = branch.out_a.is_some(),
        "branch finished"
    );
    Ok(())
}

fn open_branch(
    opts: &CompressOptions,
    output: &Path,
    in_stream: &Stream,
    audio_in: Option<&Stream>,
    source_fps: u32,
    src_w: u32,
    src_h: u32,
) -> Result<Branch> {
    let mut video = in_stream.codec_params.video().cloned().unwrap_or_else(|| VideoParams {
        pix_fmt: PixelFormat::Yuv420p,
        width: src_w,
        height: src_h,
        frame_rate: Rational::new(source_fps as i32, 1),
        sample_aspect_ratio: Rational::one(),
    });
    video.pix_fmt = PixelFormat::Yuv420p;
    if video.frame_rate.as_f64() <= 0.0 {
        video.frame_rate = Rational::new(source_fps as i32, 1);
    }

    let mut filter_parts = Vec::new();
    if let Some(w) = opts.max_width.filter(|w| *w > 0) {
        if src_w > w {
            let h = ((src_h as u64 * w as u64) / src_w as u64).max(1) as u32;
            let w = (w & !1).max(2);
            let h = (h & !1).max(2);
            video.width = w;
            video.height = h;
            filter_parts.push(format!("scale={w}:{h}"));
        }
    }
    if let Some(fps) = opts.fps.filter(|f| *f > 0) {
        video.frame_rate = Rational::new(fps as i32, 1);
        filter_parts.push(format!("fps={fps}"));
    }

    let mut out_params = CodecParams::video_codec(CodecId::H264, video);
    if let Some(ref name) = opts.encoder {
        out_params.encoder_name = name.clone();
    }
    if let Some(ref preset) = opts.preset {
        out_params.encode_preset = preset.clone();
    }
    if let Some(ref br) = opts.bitrate {
        out_params.bit_rate = parse_bit_rate(br)?;
    }
    if let Some(q) = opts.qp {
        out_params.quality = q.clamp(0, 51);
    } else if let Some(c) = opts.crf {
        out_params.quality = c.clamp(0, 51);
    }
    out_params.gop_size = opts.gop.unwrap_or(DEFAULT_GOP).max(1);

    let mut graph = if filter_parts.is_empty() {
        Graph::new()
    } else {
        Graph::parse(&filter_parts.join(","))?
    };
    graph.set_fps_source(source_fps);

    let encoder = codec::open_encoder(&out_params)?;
    out_params.extradata = encoder.params().extradata.clone();

    tracing::info!(
        out = %output.display(),
        width = out_params.video().map(|v| v.width).unwrap_or(0),
        height = out_params.video().map(|v| v.height).unwrap_or(0),
        bitrate = ?opts.bitrate,
        fps = ?opts.fps,
        preset = ?opts.preset,
        encoder = %out_params.encoder_name,
        "compress branch started"
    );

    let mut muxer = format::open_output(output, Some("mp4"))?;
    let out_v = muxer.add_stream(out_params)?;

    let mut audio_encoder = None;
    let mut out_a = None;
    if let Some(ain) = audio_in {
        let mut audio = ain.codec_params.audio().cloned().unwrap_or_default();
        audio.sample_fmt = SampleFormat::S16;
        let mut aparams = CodecParams::audio_codec(CodecId::Aac, audio);
        aparams.bit_rate = match opts.audio_bitrate.as_deref() {
            Some(br) => parse_bit_rate(br)?,
            None => 96_000,
        };
        let sr = aparams.audio().map(|a| a.sample_rate).unwrap_or(48_000);
        let ch = aparams.audio().map(|a| a.layout.channels).unwrap_or(2);
        aparams.extradata = AacEncoderCodec::audio_specific_config(sr, ch);
        audio_encoder = Some(codec::open_encoder(&aparams)?);
        out_a = Some(muxer.add_stream(aparams)?);
    }
    muxer.write_header()?;

    Ok(Branch {
        graph,
        encoder,
        muxer,
        out_v,
        audio_encoder,
        out_a,
        frames: 0,
    })
}

fn parse_bit_rate(s: &str) -> Result<u64> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix('k').or_else(|| s.strip_suffix('K')) {
        let v: f64 = num
            .parse()
            .map_err(|_| Error::invalid_data(format!("bad bitrate '{s}'")))?;
        Ok((v * 1000.0) as u64)
    } else if let Some(num) = s.strip_suffix('M').or_else(|| s.strip_suffix('m')) {
        let v: f64 = num
            .parse()
            .map_err(|_| Error::invalid_data(format!("bad bitrate '{s}'")))?;
        Ok((v * 1_000_000.0) as u64)
    } else {
        s.parse()
            .map_err(|_| Error::invalid_data(format!("bad bitrate '{s}'")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bitrate_units() {
        assert_eq!(parse_bit_rate("800k").unwrap(), 800_000);
        assert_eq!(parse_bit_rate("1.5M").unwrap(), 1_500_000);
    }

    #[test]
    fn messenger_profiles() {
        let n = CompressOptions::messenger_normal();
        assert_eq!(n.max_width, Some(854));
        assert_eq!(n.fps, Some(24));
        let l = CompressOptions::messenger_low();
        assert_eq!(l.max_width, Some(480));
        assert_eq!(l.fps, Some(12));
    }
}
