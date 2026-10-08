//! First-frame / still-image → tiny low-quality JPEG via Rumpeg.

use image::codecs::jpeg::JpegEncoder;
use image::ColorType;
use rumpeg::codec::DecoderContext;
use rumpeg::filter;
use rumpeg::format::FormatContext;
use rumpeg::scale::{self, FilterMode};
use rumpeg::{Error, Frame, MediaType, PixelFormat, Result, VideoFrame};
use std::io::Cursor;
use std::path::Path;

/// Target width of the thumbnail (height keeps aspect ratio).
pub const THUMB_WIDTH: u32 = 256;

/// JPEG quality 1–100 — intentionally very low (client will blur).
pub const JPEG_QUALITY: u8 = 12;

/// Tunable thumbnail parameters (overridable via query params).
#[derive(Debug, Clone, Copy)]
pub struct ThumbnailOptions {
    /// Target width of the thumbnail (height keeps aspect ratio).
    pub width: u32,
    /// JPEG quality 1–100.
    pub jpeg_quality: u8,
}

impl Default for ThumbnailOptions {
    fn default() -> Self {
        Self {
            width: THUMB_WIDTH,
            jpeg_quality: JPEG_QUALITY,
        }
    }
}

/// Decode the first video/image frame from a file on disk (does not load the whole file into RAM).
pub fn make_thumbnail_from_path(path: &Path, opts: &ThumbnailOptions) -> Result<Vec<u8>> {
    rumpeg::init();

    let mut demuxer = FormatContext::open(path)?;
    let stream = demuxer
        .best_stream(MediaType::Video)
        .ok_or_else(|| Error::not_found("no video/image stream"))?
        .clone();

    let mut decoder = DecoderContext::open(&stream.codec_params)?;
    let mut video_frame = None;

    // Stop as soon as we have one decoded video frame — do not scan the whole file.
    loop {
        let packet = match demuxer.read_packet() {
            Ok(p) => p,
            Err(Error::Eof) => break,
            Err(e) => return Err(e),
        };
        if packet.stream_index != stream.index {
            continue;
        }
        for frame in decoder.decode(&packet)? {
            if let Frame::Video(v) = frame {
                video_frame = Some(v);
                break;
            }
        }
        if video_frame.is_some() {
            break;
        }
    }

    if video_frame.is_none() {
        decoder.send_eof()?;
        loop {
            match decoder.receive_frame() {
                Ok(Frame::Video(v)) => {
                    video_frame = Some(v);
                    break;
                }
                Ok(_) => continue,
                Err(Error::Eof) | Err(Error::NeedMoreData) => break,
                Err(e) => return Err(e),
            }
        }
    }

    let frame = video_frame.ok_or_else(|| Error::not_found("no video frame decoded"))?;
    let (w, h) = thumb_size(frame.width, frame.height, opts.width);

    let filtered = filter::process(Frame::Video(frame), &format!("scale={w}:{h}"))?;
    let Frame::Video(scaled) = filtered else {
        return Err(Error::invalid_data("scale filter produced no video frame"));
    };
    let rgb = scale::transform(
        &scaled,
        PixelFormat::Rgb24,
        scaled.width,
        scaled.height,
        FilterMode::Bilinear,
    )?;
    encode_jpeg_low(&rgb, opts.jpeg_quality)
}

fn thumb_size(src_w: u32, src_h: u32, target_w: u32) -> (u32, u32) {
    let src_w = src_w.max(1);
    let src_h = src_h.max(1);
    let target_w = target_w.max(1);
    let height = ((src_h as u64 * target_w as u64) / src_w as u64).max(1) as u32;
    (target_w, height)
}

fn encode_jpeg_low(frame: &VideoFrame, quality: u8) -> Result<Vec<u8>> {
    let data = frame
        .plane(0)
        .ok_or_else(|| Error::invalid_data("missing RGB plane"))?;
    let mut out = Cursor::new(Vec::new());
    let mut enc = JpegEncoder::new_with_quality(&mut out, quality);
    enc.encode(data, frame.width, frame.height, ColorType::Rgb8)
        .map_err(|e| Error::Other(format!("JPEG encode failed: {e}")))?;
    Ok(out.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thumb_size_keeps_aspect() {
        assert_eq!(thumb_size(1920, 1080, 256), (256, 144));
        assert_eq!(thumb_size(1080, 1920, 256), (256, 455));
        assert_eq!(thumb_size(256, 256, 256), (256, 256));
    }

    #[test]
    fn thumb_size_respects_custom_width() {
        assert_eq!(thumb_size(1920, 1080, 512), (512, 288));
        assert_eq!(thumb_size(1920, 1080, 1), (1, 1));
    }
}
