# tools/dry_run_tool.py
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict

# local imports via relative path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from core.util import atomic_write_json, ensure_dir, now_iso, jsonl_append


def emit_stdout(event: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--doc-id", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--tool-id", required=True)

    ap.add_argument("--duration-sec", type=int, default=6)
    ap.add_argument("--tick-ms", type=int, default=250)
    ap.add_argument("--fail-prob", type=float, default=0.35)

    args = ap.parse_args()

    step_dir = Path(args.workdir).resolve()
    ensure_dir(step_dir)
    ensure_dir(step_dir / "artifacts")

    log_path = step_dir / "log.jsonl"
    progress_path = step_dir / "progress.json"
    meta_path = step_dir / "step_meta.json"
    artifact_out = step_dir / "artifacts" / "dry_run_result.json"

    started = now_iso()

    def log(level: str, message: str, **payload: Any) -> None:
        evt = {
            "t": now_iso(),
            "type": "log",
            "level": level,
            "tool_id": args.tool_id,
            "doc_id": args.doc_id,
            "run_id": args.run_id,
            "message": message,
            "payload": payload,
        }
        emit_stdout(evt)
        jsonl_append(log_path, evt)

    def progress(p: float, message: str) -> None:
        obj = {
            "t": now_iso(),
            "tool_id": args.tool_id,
            "doc_id": args.doc_id,
            "run_id": args.run_id,
            "progress": max(0.0, min(1.0, float(p))),
            "message": message,
        }
        emit_stdout({"t": obj["t"], "type": "progress", **obj})
        atomic_write_json(progress_path, obj)

    log("info", "Dry run started", duration_sec=args.duration_sec, tick_ms=args.tick_ms, fail_prob=args.fail_prob)

    ok = True
    err: Dict[str, Any] | None = None
    exit_code = 0

    try:
        total = max(1, int(args.duration_sec * 1000 / args.tick_ms))
        for i in range(total):
            time.sleep(args.tick_ms / 1000.0)
            p = (i + 1) / total
            progress(p, f"Simulating work... {int(p*100)}%")

        # decide outcome
        if random.random() < args.fail_prob:
            raise RuntimeError("Simulated failure (random)")

        # produce a tiny artifact
        atomic_write_json(artifact_out, {
            "doc_id": args.doc_id,
            "run_id": args.run_id,
            "tool_id": args.tool_id,
            "result": "ok",
            "ended_at": now_iso()
        })

        log("info", "Dry run completed OK", artifact=str(artifact_out))
        progress(1.0, "Done")

    except Exception as e:
        ok = False
        exit_code = 1
        tb = traceback.format_exc()
        err = {"message": str(e), "traceback": tb}
        log("error", "Dry run failed", error=str(e))
        progress(1.0, "Failed")

    ended = now_iso()

    meta = {
        "doc_id": args.doc_id,
        "run_id": args.run_id,
        "tool_id": args.tool_id,
        "started_at": started,
        "ended_at": ended,
        "status": "ok" if ok else "failed",
        "exit_code": exit_code,
        "outputs": [
            str(progress_path.name),
            str(meta_path.name),
            str(artifact_out.relative_to(step_dir)).replace("\\", "/"),
            str(log_path.name),
        ],
        "error": err,
    }
    atomic_write_json(meta_path, meta)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())