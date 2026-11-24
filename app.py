"""
Shan YT Downloader — standalone desktop app
Downloads YouTube channels directly to S3.
Double-click to run → opens in browser automatically.
"""
import gc
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import webbrowser
from pathlib import Path
from queue import Queue, Empty

from flask import Flask, Response, jsonify, render_template, request

# ── S3 Config (baked in at build time) ─────────────────────────────
S3_BUCKET  = os.environ.get("S3_BUCKET",  "")
S3_REGION  = os.environ.get("S3_REGION",  "ap-south-2")
S3_KEY_ID  = os.environ.get("AWS_ACCESS_KEY_ID",     "")
S3_SECRET  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
PORT       = int(os.environ.get("PORT", 7799))

app = Flask(__name__)

# ── State ────────────────────────────────────────────────────────────
download_queue: list   = []
progress_store: dict   = {}
queue_lock             = threading.Lock()
worker_running         = False
current_video_id       = ""
sse_clients: list      = []
sse_lock               = threading.Lock()

# ── Helpers ──────────────────────────────────────────────────────────
def find_ytdlp() -> str | None:
    """Find yt-dlp binary."""
    for candidate in ["yt-dlp", "yt-dlp.exe"]:
        try:
            subprocess.run([candidate, "--version"], capture_output=True, check=True)
            return candidate
        except Exception:
            pass
    # Check alongside exe (bundled)
    base = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent
    for name in ["yt-dlp.exe", "yt-dlp"]:
        p = base / name
        if p.exists():
            return str(p)
    return None

def sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", name)

def upload_to_s3(local_path: str, channel_name: str, video_id: str) -> str:
    import boto3
    s3 = boto3.client(
        "s3",
        region_name=S3_REGION,
        aws_access_key_id=S3_KEY_ID,
        aws_secret_access_key=S3_SECRET,
    )
    key = f"youtube/{sanitize(channel_name)}/{video_id}.mp4"
    file_size = os.path.getsize(local_path)
    with open(local_path, "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET, Key=key, Body=f,
            ContentLength=file_size, ContentType="video/mp4",
            CacheControl="public, max-age=31536000",
        )
    return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}"

def set_progress(video_id: str, **kwargs):
    progress_store[video_id] = {**progress_store.get(video_id, {}), **kwargs}
    broadcast_sse()

def broadcast_sse():
    data = json.dumps({
        "videos": list(progress_store.values()),
        "queue_length": len(download_queue),
        "current": current_video_id,
    })
    with sse_lock:
        for q in list(sse_clients):
            try:
                q.put_nowait(data)
            except Exception:
                pass

# ── Background download worker ────────────────────────────────────────
def process_queue():
    global worker_running, current_video_id
    worker_running = True
    ytdlp = find_ytdlp()
    if not ytdlp:
        worker_running = False
        return

    with tempfile.TemporaryDirectory(prefix="shan_ytdl_") as tmpdir:
        while True:
            with queue_lock:
                if not download_queue:
                    break
                item = download_queue.pop(0)

            vid      = item["video_id"]
            channel  = item["channel_name"]
            channel_url = item.get("channel_url", "")
            current_video_id = vid

            # Skip if already in S3
            try:
                import boto3
                s3c = boto3.client("s3", region_name=S3_REGION,
                                   aws_access_key_id=S3_KEY_ID,
                                   aws_secret_access_key=S3_SECRET)
                s3c.head_object(Bucket=S3_BUCKET,
                                Key=f"youtube/{sanitize(channel)}/{vid}.mp4")
                set_progress(vid, status="completed", progress=100,
                             s3_key=f"youtube/{sanitize(channel)}/{vid}.mp4",
                             note="already in S3")
                continue
            except Exception:
                pass

            # Download
            out_tmpl = os.path.join(tmpdir, f"{vid}.%(ext)s")
            yt_url   = f"https://www.youtube.com/watch?v={vid}"
            cookies_file = os.path.join(os.path.dirname(__file__), "cookies.txt")
            args = [
                ytdlp, "-f", "bestvideo[ext=mp4]/bestvideo",
                "--remux-video", "mp4",
                "--no-playlist", "--no-write-subs", "--no-write-auto-subs",
                "--no-embed-thumbnail", "--no-embed-metadata",
                "--socket-timeout", "60",
                "--retries", "5",
                "--fragment-retries", "5",
                "--extractor-retries", "3",
                "--progress", "--newline",
                "--progress-template",
                "%(progress._percent_str)s|%(progress._speed_str)s|%(progress._eta_str)s",
            ]
            if os.path.exists(cookies_file):
                args += ["--cookies", cookies_file]
            args += ["-o", out_tmpl, yt_url]

            set_progress(vid, status="downloading", progress=0,
                         speed="", eta="calculating...", channel=channel)

            proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True)
            # Kill if stuck for more than 10 minutes with no progress
            last_progress = [0, time.time()]
            for line in proc.stdout:
                m = re.match(r"([\d.]+)%\s*\|\s*([\S ]+?)\s*\|\s*([\S]+)", line)
                if m:
                    pct, spd, eta = m.groups()
                    pct_f = float(pct)
                    if pct_f > last_progress[0]:
                        last_progress[0] = pct_f
                        last_progress[1] = time.time()
                    set_progress(vid, progress=pct_f,
                                 speed=spd.strip(), eta=eta.strip())
                # Kill if no progress for 8 minutes
                if time.time() - last_progress[1] > 480:
                    proc.kill()
                    set_progress(vid, status="error", error="Stuck — YouTube blocked this server IP. Upload cookies.txt to fix.")
                    break
            proc.wait()

            if proc.returncode != 0:
                set_progress(vid, status="error", error=f"yt-dlp exit {proc.returncode}")
                continue

            # Find downloaded file
            mp4 = None
            for f in Path(tmpdir).iterdir():
                if f.stem == vid and f.suffix == ".mp4" and f.stat().st_size > 100_000:
                    mp4 = str(f)
                    break

            if not mp4:
                set_progress(vid, status="error", error="file not found after download")
                continue

            # Upload to S3
            set_progress(vid, status="uploading", progress=100, speed="", eta="")
            try:
                s3_url = upload_to_s3(mp4, channel, vid)
                set_progress(vid, status="completed", progress=100,
                             s3_url=s3_url,
                             s3_key=f"youtube/{sanitize(channel)}/{vid}.mp4")
                os.remove(mp4)
            except Exception as e:
                set_progress(vid, status="error", error=str(e))

            gc.collect()

    current_video_id = ""
    worker_running = False

# ── Routes ────────────────────────────────────────────────────────────
@app.route("/api/cookies", methods=["POST"])
def upload_cookies():
    f = request.files.get("cookies")
    if not f:
        return jsonify({"error": "No file"}), 400
    dest = os.path.join(os.path.dirname(__file__), "cookies.txt")
    f.save(dest)
    return jsonify({"ok": True, "message": "Cookies saved — downloads will now bypass YouTube restrictions"})

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/channel", methods=["POST"])
def scan_channel():
    data        = request.get_json()
    channel_url = data.get("channel_url", "").strip()
    if not channel_url:
        return jsonify({"error": "No URL provided"}), 400

    ytdlp = find_ytdlp()
    if not ytdlp:
        return jsonify({"error": "yt-dlp not found. Install it: pip install yt-dlp"}), 500

    args = [ytdlp, "--flat-playlist", "--dump-single-json",
            "--no-warnings", channel_url]
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return jsonify({"error": result.stderr[:300]}), 500
        info   = json.loads(result.stdout)
        videos = []
        for entry in info.get("entries", []):
            if not entry:
                continue
            vid_id = entry.get("id", "")
            if not vid_id:
                continue
            videos.append({
                "id":        vid_id,
                "title":     entry.get("title", vid_id),
                "thumbnail": entry.get("thumbnail", f"https://img.youtube.com/vi/{vid_id}/mqdefault.jpg"),
                "duration":  str(entry.get("duration_string", "")),
            })
        return jsonify({"videos": videos, "count": len(videos)})
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Scan timed out (>2 min)"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/download", methods=["POST"])
def enqueue_download():
    global worker_running
    data         = request.get_json()
    video_ids    = data.get("video_ids", [])
    channel_name = sanitize(data.get("channel_name", "unknown"))
    channel_url  = data.get("channel_url", "")

    added = 0
    with queue_lock:
        queued_ids = {i["video_id"] for i in download_queue}
        for vid in video_ids:
            if vid in queued_ids:
                continue
            st = progress_store.get(vid, {}).get("status", "")
            if st in ("downloading", "uploading", "completed"):
                continue
            download_queue.append({"video_id": vid, "channel_name": channel_name,
                                   "channel_url": channel_url})
            set_progress(vid, status="queued", progress=0,
                         eta="in queue", channel=channel_name)
            added += 1

    if added > 0 and not worker_running:
        t = threading.Thread(target=process_queue, daemon=True)
        t.start()

    return jsonify({"queued": added, "total_queue": len(download_queue)})

@app.route("/api/progress")
def progress_sse():
    q: Queue = Queue(maxsize=20)
    with sse_lock:
        sse_clients.append(q)

    def stream():
        try:
            # Send current state immediately
            data = json.dumps({
                "videos": list(progress_store.values()),
                "queue_length": len(download_queue),
                "current": current_video_id,
            })
            yield f"data: {data}\n\n"
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield f"data: {msg}\n\n"
                except Empty:
                    yield "data: {\"ping\":1}\n\n"
        finally:
            with sse_lock:
                try:
                    sse_clients.remove(q)
                except ValueError:
                    pass

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})

@app.route("/api/status")
def status():
    return jsonify({
        "ytdlp": find_ytdlp() is not None,
        "queue": len(download_queue),
        "current": current_video_id,
        "done": sum(1 for v in progress_store.values() if v.get("status") == "completed"),
        "s3_bucket": S3_BUCKET,
    })

# ── Entry point ───────────────────────────────────────────────────────
if __name__ == "__main__":
    is_local = not os.environ.get("RAILWAY_ENVIRONMENT") and not os.environ.get("RENDER")
    print(f"Shan YT Downloader starting on http://localhost:{PORT}")
    if is_local:
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{PORT}")).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
