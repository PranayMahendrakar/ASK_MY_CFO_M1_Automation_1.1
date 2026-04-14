#!/usr/bin/env python3
"""
ASK MY CFO — M1 Automation
===========================
Properly chains: page_detector → extract_tables → bs_pl_mapper
using their top-level functions exactly as they work from CLI.

Flow:
  1. extract_pages(pdf)        → *_BS_PL.pdf      (only BS/PL pages)
  2. extract_tables(bs_pl_pdf) → *_extracted.xlsx  (structured rows)
  3. process_file(xlsx, ...)   → *_Report.xlsx     (mapped template)
"""

import os
import sys
import json
import uuid
import io
import threading
import traceback
from pathlib import Path
from datetime import datetime
from queue import Queue, Empty

# Add modules to path
MODULES_DIR = os.path.join(os.path.dirname(__file__), "modules")
sys.path.insert(0, MODULES_DIR)

from flask import (
    Flask, render_template, request, jsonify,
    send_file, Response, stream_with_context
)
from werkzeug.utils import secure_filename

app = Flask(__name__, template_folder=".")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024

BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "output"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# JOB TRACKER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

jobs = {}


class PipelineJob:
    def __init__(self, job_id, pdf_paths, api_key, skip_stage1, skip_stage3):
        self.job_id = job_id
        self.pdf_paths = pdf_paths
        self.api_key = api_key
        self.skip_stage1 = skip_stage1
        self.skip_stage3 = skip_stage3
        self.status = "queued"
        self.stage = 0
        self.stage_label = ""
        self.progress = 0
        self.logs = []
        self.output_files = []
        self.error = None
        self.queue = Queue()

    def log(self, msg, level="info"):
        entry = {"time": datetime.now().strftime("%H:%M:%S"), "level": level, "message": msg}
        self.logs.append(entry)
        self._push({"type": "log", "data": entry, "stage": self.stage,
                     "stage_label": self.stage_label, "progress": self.progress,
                     "status": self.status})

    def set_stage(self, stage, label, progress=None):
        self.stage = stage
        self.stage_label = label
        if progress is not None:
            self.progress = progress
        self._push({"type": "stage", "stage": stage, "stage_label": label,
                     "progress": self.progress, "status": self.status})

    def finish(self):
        self.status = "completed"
        self.progress = 100
        self._push({"type": "complete", "status": "completed", "progress": 100,
                     "files": [os.path.basename(f) for f in self.output_files]})

    def fail(self, error):
        self.status = "failed"
        self.error = str(error)
        self._push({"type": "error", "error": str(error), "status": "failed"})

    def _push(self, data):
        self.queue.put(json.dumps(data))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STDOUT CAPTURE — route module prints → job log
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class LogCapture(io.TextIOBase):
    def __init__(self, job, level="info"):
        self.job = job
        self.level = level
        self.buffer = ""

    def write(self, s):
        if not s:
            return 0
        self.buffer += s
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            line = line.rstrip()
            if line:
                lvl = self.level
                if any(c in line for c in ["✓", "✅", "succeeded", "balanced"]):
                    lvl = "success"
                elif any(c in line for c in ["✗", "ERROR", "FAILED", "failed!"]):
                    lvl = "error"
                elif any(c in line for c in ["⚠", "WARNING", "retrying", "FLAGGED"]):
                    lvl = "warning"
                elif "━" in line or "──" in line or "==" in line:
                    lvl = "header"
                self.job.log(line, lvl)
        return len(s)

    def flush(self):
        if self.buffer.strip():
            self.job.log(self.buffer.strip(), self.level)
            self.buffer = ""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PIPELINE — uses EXACT same top-level functions as CLI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_pipeline(job: PipelineJob):
    out_dir = OUTPUT_DIR / job.job_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # Capture all print() from the modules
    old_stdout, old_stderr = sys.stdout, sys.stderr
    cap_out = LogCapture(job, "info")
    cap_err = LogCapture(job, "error")

    try:
        job.status = "running"
        total = len(job.pdf_paths)

        for fi, pdf_path in enumerate(job.pdf_paths):
            stem = Path(pdf_path).stem
            base_pct = int(fi / total * 100)
            span = int(100 / total)

            job.log(f"━━━ File {fi+1}/{total}: {Path(pdf_path).name} ━━━", "header")

            # ═══════════════════════════════════════
            # STAGE 1: extract_pages() → clean PDF
            # ═══════════════════════════════════════
            working_pdf = pdf_path

            if job.skip_stage1:
                job.set_stage(1, "Skipped — using PDF as-is", base_pct + 2)
                job.log("Stage 1 skipped (PDF assumed to already contain only BS/PL)")
            else:
                job.set_stage(1, f"Detecting pages — {stem}", base_pct + 2)
                job.log("Running page_detector.extract_pages()...")

                try:
                    from page_detector import extract_pages

                    sys.stdout, sys.stderr = cap_out, cap_err
                    bs_pl_pdf = extract_pages(pdf_path, str(out_dir))
                    sys.stdout, sys.stderr = old_stdout, old_stderr
                    cap_out.flush()
                    cap_err.flush()

                    if bs_pl_pdf and os.path.exists(bs_pl_pdf):
                        working_pdf = bs_pl_pdf
                        job.log(f"✓ Created {os.path.basename(bs_pl_pdf)}", "success")
                        job.output_files.append(bs_pl_pdf)
                    else:
                        job.log("⚠ No sections found — falling back to full PDF", "warning")

                except Exception as e:
                    sys.stdout, sys.stderr = old_stdout, old_stderr
                    job.log(f"⚠ Stage 1 error: {e}", "warning")
                    job.log("Falling back to full PDF for extraction", "warning")

            # ═══════════════════════════════════════
            # STAGE 2: extract_tables() → Excel
            # ═══════════════════════════════════════
            job.set_stage(2, f"Extracting tables — {stem}", base_pct + int(span * 0.25))
            job.log(f"Running extract_tables.extract_tables({os.path.basename(working_pdf)})...")

            extracted_xlsx = None
            try:
                from extract_tables import extract_tables as run_extract

                sys.stdout, sys.stderr = cap_out, cap_err
                run_extract(working_pdf, str(out_dir))
                sys.stdout, sys.stderr = old_stdout, old_stderr
                cap_out.flush()
                cap_err.flush()

                # Find output — extract_tables saves as {stem}_extracted.xlsx
                expected = out_dir / f"{Path(working_pdf).stem}_extracted.xlsx"
                if expected.exists():
                    extracted_xlsx = str(expected)
                else:
                    # Fallback: find any new _extracted.xlsx
                    for f in sorted(out_dir.glob("*_extracted.xlsx"), key=os.path.getmtime, reverse=True):
                        extracted_xlsx = str(f)
                        break

                if extracted_xlsx:
                    job.log(f"✓ Created {os.path.basename(extracted_xlsx)}", "success")
                    job.output_files.append(extracted_xlsx)
                else:
                    job.log("✗ No extracted Excel was generated!", "error")
                    continue

            except Exception as e:
                sys.stdout, sys.stderr = old_stdout, old_stderr
                job.log(f"✗ Stage 2 failed: {e}", "error")
                job.log(traceback.format_exc(), "error")
                continue

            # ═══════════════════════════════════════
            # STAGE 3: process_file() → Report
            # ═══════════════════════════════════════
            if job.skip_stage3:
                job.set_stage(3, "Skipped — no API key", base_pct + int(span * 0.95))
                job.log("Stage 3 skipped (provide OpenAI API key to enable LLM mapping)")
                continue

            job.set_stage(3, f"AI mapping — {stem}", base_pct + int(span * 0.50))
            job.log(f"Running bs_pl_mapper.process_file({os.path.basename(extracted_xlsx)})...")

            try:
                from bs_pl_mapper import process_file

                sys.stdout, sys.stderr = cap_out, cap_err
                report_path = process_file(
                    input_file=extracted_xlsx,
                    template_file=extracted_xlsx,  # not used internally
                    api_key=job.api_key,
                    output_dir=str(out_dir),
                )
                sys.stdout, sys.stderr = old_stdout, old_stderr
                cap_out.flush()
                cap_err.flush()

                if report_path and os.path.exists(report_path):
                    job.log(f"✓ Created {os.path.basename(report_path)}", "success")
                    job.output_files.append(report_path)
                else:
                    job.log("⚠ process_file returned no output", "warning")

            except Exception as e:
                sys.stdout, sys.stderr = old_stdout, old_stderr
                job.log(f"✗ Stage 3 failed: {e}", "error")
                job.log(traceback.format_exc(), "error")

        # All files done
        if job.output_files:
            job.log(f"━━━ Done — {len(job.output_files)} file(s) generated ━━━", "header")
            job.finish()
        else:
            job.fail("No output files were generated.")

    except Exception as e:
        sys.stdout, sys.stderr = old_stdout, old_stderr
        job.log(f"Fatal: {e}", "error")
        job.log(traceback.format_exc(), "error")
        job.fail(str(e))
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/upload", methods=["POST"])
def upload():
    if "files" not in request.files:
        return jsonify({"error": "No files uploaded"}), 400

    files = request.files.getlist("files")
    if not files or not any(f.filename for f in files):
        return jsonify({"error": "No files selected"}), 400

    api_key = request.form.get("api_key", "").strip()
    skip_stage1 = request.form.get("skip_stage1") == "true"
    skip_stage3 = not bool(api_key)

    job_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:6]
    job_dir = UPLOAD_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    pdf_paths = []
    for f in files:
        if f.filename:
            fname = secure_filename(f.filename)
            fpath = str(job_dir / fname)
            f.save(fpath)
            pdf_paths.append(fpath)

    if not pdf_paths:
        return jsonify({"error": "No valid files uploaded"}), 400

    job = PipelineJob(job_id, pdf_paths, api_key, skip_stage1, skip_stage3)
    jobs[job_id] = job

    thread = threading.Thread(target=run_pipeline, args=(job,), daemon=True)
    thread.start()

    return jsonify({
        "job_id": job_id,
        "files": [os.path.basename(p) for p in pdf_paths],
        "skip_stage1": skip_stage1,
        "skip_stage3": skip_stage3,
    })


@app.route("/api/stream/<job_id>")
def stream(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    def generate():
        while True:
            try:
                msg = job.queue.get(timeout=60)
                yield f"data: {msg}\n\n"
                data = json.loads(msg)
                if data.get("type") in ("complete", "error"):
                    break
            except Empty:
                yield f"data: {json.dumps({'type': 'ping'})}\n\n"
                if job.status in ("completed", "failed"):
                    break

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/download/<job_id>/<path:filename>")
def download(job_id, filename):
    fpath = OUTPUT_DIR / job_id / filename
    if fpath.exists():
        return send_file(str(fpath), as_attachment=True)
    return jsonify({"error": "File not found"}), 404


@app.route("/api/status/<job_id>")
def status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "status": job.status, "stage": job.stage, "progress": job.progress,
        "files": [os.path.basename(f) for f in job.output_files],
        "error": job.error,
    })


if __name__ == "__main__":
    UPLOAD_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
    print()
    print("  ╔═══════════════════════════════════════════╗")
    print("  ║  ASK MY CFO — M1 Automation               ║")
    print("  ║  http://localhost:5000                     ║")
    print("  ╚═══════════════════════════════════════════╝")
    print()
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
