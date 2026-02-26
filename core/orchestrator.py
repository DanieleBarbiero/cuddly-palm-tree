# core/orchestrator.py
from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .util import (
    atomic_write_json,
    ensure_dir,
    item_type_from_path,
    now_iso,
    read_json,
    sha256_file,
    short_id,
    jsonl_append,
)


@dataclass(frozen=True)
class WorkItem:
    doc_id: str
    item_type: str
    work_dir: Path
    input_path: Path
    manifest_path: Path

    @property
    def display_name(self) -> str:
        return f"{short_id(self.doc_id)}  ({self.input_path.name})"


class Orchestrator:
    def __init__(self, work_root: Path) -> None:
        self.work_root = work_root
        ensure_dir(self.work_root)

        self.app_log = self.work_root / "app.log.jsonl"

    def log_app(self, event: str, **payload: Any) -> None:
        jsonl_append(self.app_log, {"t": now_iso(), "event": event, **payload})

    def list_items(self) -> List[WorkItem]:
        items: List[WorkItem] = []
        for d in sorted(self.work_root.iterdir()):
            if not d.is_dir():
                continue
            manifest = d / "manifest.json"
            if not manifest.exists():
                continue
            try:
                m = read_json(manifest)
                input_rel = m["input"]["path"]
                input_path = d / input_rel
                items.append(
                    WorkItem(
                        doc_id=m["doc_id"],
                        item_type=m.get("item_type", "unknown"),
                        work_dir=d,
                        input_path=input_path,
                        manifest_path=manifest,
                    )
                )
            except Exception:
                continue
        return items

    def import_file(self, src: Path) -> WorkItem:
        src = src.resolve()
        if not src.exists() or not src.is_file():
            raise FileNotFoundError(str(src))

        doc_id = sha256_file(src)
        item_type = item_type_from_path(src)

        doc_dir = self.work_root / doc_id
        input_dir = doc_dir / "input"
        ensure_dir(input_dir)

        dest = input_dir / src.name
        if not dest.exists():
            shutil.copy2(src, dest)

        manifest = {
            "doc_id": doc_id,
            "item_type": item_type,
            "created_at": now_iso(),
            "input": {
                "path": str(dest.relative_to(doc_dir)).replace("\\", "/"),
                "original_abs": str(src),
                "filename": src.name,
            },
            "last_run_id": None,
        }

        atomic_write_json(doc_dir / "manifest.json", manifest)
        ensure_dir(doc_dir / "runs")
        ensure_dir(doc_dir / "logs")

        self.log_app("import_file", doc_id=doc_id, src=str(src), dest=str(dest))
        return WorkItem(
            doc_id=doc_id,
            item_type=item_type,
            work_dir=doc_dir,
            input_path=dest,
            manifest_path=doc_dir / "manifest.json",
        )

    def new_run(self, item: WorkItem) -> str:
        run_id = now_iso().replace(":", "").replace("-", "").replace("T", "_") + "_" + uuid.uuid4().hex[:8]
        run_dir = item.work_dir / "runs" / run_id
        ensure_dir(run_dir)
        atomic_write_json(run_dir / "run_manifest.json", {
            "doc_id": item.doc_id,
            "run_id": run_id,
            "created_at": now_iso(),
            "steps": {}
        })

        # update doc manifest pointer
        m = read_json(item.manifest_path)
        m["last_run_id"] = run_id
        atomic_write_json(item.manifest_path, m)

        self.log_app("new_run", doc_id=item.doc_id, run_id=run_id)
        return run_id

    def step_dir(self, item: WorkItem, run_id: str, tool_id: str) -> Path:
        d = item.work_dir / "runs" / run_id / "steps" / tool_id
        ensure_dir(d)
        ensure_dir(d / "artifacts")
        return d

    def record_step_result(self, item: WorkItem, run_id: str, tool_id: str, step_meta: Dict[str, Any]) -> None:
        run_manifest_path = item.work_dir / "runs" / run_id / "run_manifest.json"
        rm = read_json(run_manifest_path)
        rm["steps"][tool_id] = step_meta
        atomic_write_json(run_manifest_path, rm)
        self.log_app("step_recorded", doc_id=item.doc_id, run_id=run_id, tool_id=tool_id, status=step_meta.get("status"))

    def get_last_run_manifest(self, item: WorkItem) -> Optional[Dict[str, Any]]:
        m = read_json(item.manifest_path)
        run_id = m.get("last_run_id")
        if not run_id:
            return None
        path = item.work_dir / "runs" / run_id / "run_manifest.json"
        if not path.exists():
            return None
        return read_json(path)

    def has_required_artifacts(self, step_dir: Path, requires: List[str]) -> bool:
        # "requires" here is artifact names; in questo demo controlliamo file nella dir step o nei parent.
        # Semplice: consideriamo che se richiede "docling.json" esista in workdir (non implementato qui).
        # Per ora: nessun tool reale richiede artefatti, quindi sempre True.
        return True