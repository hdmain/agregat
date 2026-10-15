#!/usr/bin/env python3
"""Test script for agregat API — thumbnails, pHash, and compression."""

import argparse
import base64
import json
import sys
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "http://localhost:7528"


def request(method, path, data=None, files=None, query_params=None):
    """Send an HTTP request to the agregat API."""
    url = BASE_URL + path
    if query_params:
        url += "?" + urlencode(query_params)

    if files is not None:
        import email.mime.multipart
        import email.mime.base
        import email.encoders

        boundary = "----bound123"
        body = b""
        for field_name, file_path in files.items():
            with open(file_path, "rb") as f:
                content = f.read()
            body += f'--{boundary}\r\n'.encode()
            body += f'Content-Disposition: form-data; name="{field_name}"; filename="{Path(file_path).name}"\r\n'.encode()
            body += b"Content-Type: application/octet-stream\r\n\r\n"
            body += content + b"\r\n"
        body += f"--{boundary}--\r\n".encode()

        headers = {"Content-Type": f"multipart/form-data; boundary={boundary}"}
        req = Request(url, data=body, headers=headers, method=method)
    elif isinstance(data, dict):
        body = json.dumps(data).encode()
        headers = {"Content-Type": "application/json"}
        req = Request(url, data=body, headers=headers, method=method)
    elif data is not None:
        # raw bytes
        req = Request(url, data=data, method=method)
    else:
        req = Request(url, method=method)

    try:
        with urlopen(req, timeout=120) as resp:
            raw = resp.read()
            if raw:
                return json.loads(raw)
            return {}
    except HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"  ERROR {e.code}: {body[:500]}", file=sys.stderr)
        sys.exit(1)


def wait_for_job(job_id, job_type, poll_interval=0.5, timeout=120):
    """Poll /jobs/{id} until the job completes or times out."""
    start = time.monotonic()
    while True:
        resp = request("GET", f"/jobs/{job_id}")
        # Server wraps response as {"ok": true, "job": {...}}
        job = resp.get("job", resp)
        status = job.get("status")
        elapsed = time.monotonic() - start
        print(f"  [{elapsed:.1f}s] status={status}", end="\r", file=sys.stderr)
        if status == "completed":
            print(f"\n  [done] ({elapsed:.1f}s)", file=sys.stderr)
            return job
        if status == "failed":
            print(f"\n  [FAILED] {job.get('error')}", file=sys.stderr)
            return job
        if status == "cancelled":
            print(f"\n  [cancelled]", file=sys.stderr)
            return job
        if elapsed > timeout:
            print(f"\n  [timeout after {timeout}s]", file=sys.stderr)
            return job
        time.sleep(poll_interval)


def download_job_output(job, output_dir="output"):
    """Display job metadata and download generated files."""
    extra = job.get("extra") if isinstance(job, dict) else None
    result = job.get("result") if isinstance(job, dict) else None

    # Hash results may be in extra or result depending on endpoint
    hash_val = None
    distance = None
    if extra and "hash" in extra:
        hash_val = extra["hash"]
        distance = extra.get("distance")
    if result and "phash" in result:
        hash_val = result["phash"]
    if result and "distance" in result:
        distance = result["distance"]

    if hash_val is not None:
        print(f"  phash: {hash_val}")
        if distance is not None:
            print(f"  hamming distance: {distance}")
        return

    if extra:
        if "width" in extra:
            print(f"  requested width: {extra['width']}px")
        if "jpeg_quality" in extra:
            print(f"  jpeg quality: {extra['jpeg_quality']}")

    if result:
        if "output_size" in result:
            print(f"  output size: {result['output_size']} bytes")
        if "output_path" in result:
            name = result["output_path"]
            out_file = Path(output_dir) / name
            # Download thumbnail from server
            url = BASE_URL + f"/thumb/{name}"
            try:
                with urlopen(url, timeout=30) as resp:
                    data = resp.read()
                    Path(output_dir).mkdir(exist_ok=True)
                    out_file.write_bytes(data)
                    print(f"  saved: {out_file} ({len(data)} bytes)")
            except HTTPError as e:
                print(f"  download failed: {e.code}")
            except Exception as e:
                print(f"  download error: {e}")
        if "output_width" in result:
            print(f"  output width: {result['output_width']}px")
        if "output_height" in result:
            print(f"  output height: {result['output_height']}px")
        if "encoder" in result:
            print(f"  encoder: {result['encoder']}")


def test_health():
    """Check that the server is running."""
    print("[health]")
    result = request("GET", "/health")
    print(f"  service: {result.get('service')}")
    print(f"  port: {result.get('port')}")
    print(f"  jobs_running: {result.get('jobs_running')}")
    print()


def test_thumbnail_image(args):
    """Generate thumbnail from an image."""
    print(f"[thumbnail image] {args.file}")
    params = {}
    if args.width:
        params["width"] = args.width
    if args.jpeg_quality:
        params["jpeg_quality"] = args.jpeg_quality
    result = request("POST", "/thumbnail", files={"file": args.file}, query_params=params)
    job_id = result["job_id"]
    print(f"  queued job_id={job_id}")
    job = wait_for_job(job_id, "thumbnail")
    download_job_output(job)
    print()


def test_thumbnail_video(args):
    """Generate thumbnail from a video."""
    print(f"[thumbnail video] {args.file}")
    params = {}
    if args.width:
        params["width"] = args.width
    if args.jpeg_quality:
        params["jpeg_quality"] = args.jpeg_quality
    result = request("POST", "/thumbnail", files={"file": args.file}, query_params=params)
    job_id = result["job_id"]
    print(f"  queued job_id={job_id}")
    job = wait_for_job(job_id, "thumbnail")
    download_job_output(job)
    print()


def test_hash_image(args):
    """Compute perceptual hash from an image."""
    print(f"[hash image] {args.file}")
    result = request("POST", "/hash", files={"file": args.file})
    job_id = result["job_id"]
    print(f"  queued job_id={job_id}")
    job = wait_for_job(job_id, "hash")
    download_job_output(job)
    print()


def test_hash_compare(args):
    """Compare two images using perceptual hash."""
    print(f"[hash compare] {args.image1} vs {args.image2}")
    result = request("POST", "/hash/compare", files={"file1": args.image1, "file2": args.image2})
    job_id = result["job_id"]
    print(f"  queued job_id={job_id}")
    job = wait_for_job(job_id, "hash_compare")
    download_job_output(job)
    print()


def test_compress(args):
    """Compress a video."""
    print(f"[compress] {args.file} profile={args.profile}")
    params = {"profile": args.profile}
    result = request("POST", "/compress", files={"file": args.file}, query_params=params)
    job_id = result["job_id"]
    print(f"  queued job_id={job_id}")
    job = wait_for_job(job_id, "compress")
    download_job_output(job)
    print()


def main():
    parser = argparse.ArgumentParser(description="Test agregat API")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # health
    subparsers.add_parser("health", help="Check server health")

    # thumbnail-image
    p_thumb_img = subparsers.add_parser("thumbnail-image", help="Thumbnail from image")
    p_thumb_img.add_argument("file", type=str, help="Path to image file")
    p_thumb_img.add_argument("--width", type=int, default=None)
    p_thumb_img.add_argument("--jpeg-quality", type=int, default=None)

    # thumbnail-video
    p_thumb_vid = subparsers.add_parser("thumbnail-video", help="Thumbnail from video")
    p_thumb_vid.add_argument("file", type=str, help="Path to video file")
    p_thumb_vid.add_argument("--width", type=int, default=None)
    p_thumb_vid.add_argument("--jpeg-quality", type=int, default=None)

    # hash
    p_hash = subparsers.add_parser("hash", help="pHash of single image")
    p_hash.add_argument("file", type=str, help="Path to image file")

    # hash-compare
    p_hash_cmp = subparsers.add_parser("hash-compare", help="Compare two images")
    p_hash_cmp.add_argument("image1", type=str, help="Path to first image")
    p_hash_cmp.add_argument("image2", type=str, help="Path to second image")

    # compress
    p_comp = subparsers.add_parser("compress", help="Compress video")
    p_comp.add_argument("file", type=str, help="Path to video file")
    p_comp.add_argument("--profile", choices=["normal", "low"], default="normal")

    args = parser.parse_args()

    if args.command == "health":
        test_health()
    elif args.command == "thumbnail-image":
        test_thumbnail_image(args)
    elif args.command == "thumbnail-video":
        test_thumbnail_video(args)
    elif args.command == "hash":
        test_hash_image(args)
    elif args.command == "hash-compare":
        test_hash_compare(args)
    elif args.command == "compress":
        test_compress(args)


if __name__ == "__main__":
    main()
