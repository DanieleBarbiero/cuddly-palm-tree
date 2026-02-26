# app.py
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, QProcess, QTimer
from PySide6.QtGui import QAction, QFont
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFileDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QToolBar,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.orchestrator import Orchestrator, WorkItem
from core.tool_registry import (
    PipelineProfile,
    ToolDef,
    load_pipeline_profiles,
    load_tools_yaml,
    tool_accepts_item,
)
from core.util import now_iso, read_json, ensure_dir, jsonl_append


@dataclass
class ToolRunRequest:
    tool: ToolDef
    params: Dict[str, Any]
    skip: bool = False
    skip_reason: str = ""


class ToolRunner:
    """
    Runs one tool at a time via QProcess.
    Communication:
      - stdout JSONL events (log/progress)
      - step_dir/progress.json (polled)
      - step_dir/step_meta.json (final)
    """
    def __init__(self, parent: QWidget) -> None:
        self.parent = parent
        self.proc: Optional[QProcess] = None
        self.stdout_buf = b""

        self.current_item: Optional[WorkItem] = None
        self.current_run_id: Optional[str] = None
        self.current_tool: Optional[ToolDef] = None
        self.current_step_dir: Optional[Path] = None
        self.queue: List[ToolRunRequest] = []

        self.on_event = None   # callable(event_dict)
        self.on_finished = None  # callable(exit_code, step_meta_or_none)
        self.on_step_finished = None  # callable(tool_id, step_meta)

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.state() != QProcess.NotRunning

    def start_queue(self, item: WorkItem, run_id: str, step_dirs: Dict[str, Path], requests: List[ToolRunRequest]) -> None:
        if self.is_running():
            return
        self.current_item = item
        self.current_run_id = run_id
        self.queue = list(requests)
        self._start_next(step_dirs)

    def _start_next(self, step_dirs: Dict[str, Path]) -> None:
        if not self.queue:
            if self.on_finished:
                self.on_finished(0, None)
            return

        req = self.queue.pop(0)
        tool = req.tool
        self.current_tool = tool
        self.current_step_dir = step_dirs[tool.id]

        if req.skip:
            self._emit({
                "t": now_iso(),
                "type": "log",
                "level": "info",
                "tool_id": tool.id,
                "message": req.skip_reason or "Step skipped",
            })
            step_meta = {
                "doc_id": self.current_item.doc_id,
                "run_id": self.current_run_id,
                "tool_id": tool.id,
                "started_at": now_iso(),
                "ended_at": now_iso(),
                "status": "skipped",
                "exit_code": 0,
                "outputs": [],
                "error": None,
                "skip_reason": req.skip_reason or "outputs already available",
            }
            json_path = self.current_step_dir / "step_meta.json"
            json_path.write_text(json.dumps(step_meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            if self.on_step_finished:
                self.on_step_finished(tool.id, step_meta)
            self._start_next(step_dirs)
            return

        # Build command
        if tool.command_kind != "python_module":
            raise RuntimeError(f"Unsupported command kind: {tool.command_kind}")

        py = sys.executable
        args = ["-m", tool.command_module]

        # common args
        args += [
            "--workdir", str(self.current_step_dir),
            "--doc-id", self.current_item.doc_id,
            "--run-id", self.current_run_id,
            "--tool-id", tool.id,
        ]

        # tool params
        for k, v in req.params.items():
            flag = "--" + k.replace("_", "-")
            args.append(flag)
            args.append(str(v))

        self.proc = QProcess(self.parent)
        self.proc.setProgram(py)
        self.proc.setArguments(args)
        self.proc.setProcessChannelMode(QProcess.SeparateChannels)

        self.proc.readyReadStandardOutput.connect(self._on_stdout)
        self.proc.readyReadStandardError.connect(self._on_stderr)
        self.proc.finished.connect(lambda code, status: self._on_finished(code, status, step_dirs))

        self.proc.start()

    def _emit(self, evt: Dict[str, Any]) -> None:
        if self.on_event:
            self.on_event(evt)

    def _on_stdout(self) -> None:
        if not self.proc:
            return
        data = bytes(self.proc.readAllStandardOutput())
        if not data:
            return
        self.stdout_buf += data

        # parse by lines
        while b"\n" in self.stdout_buf:
            line, self.stdout_buf = self.stdout_buf.split(b"\n", 1)
            if not line.strip():
                continue
            try:
                evt = json.loads(line.decode("utf-8", errors="replace"))
                self._emit(evt)
            except Exception:
                self._emit({"t": now_iso(), "type": "raw", "message": line.decode("utf-8", errors="replace")})

    def _on_stderr(self) -> None:
        if not self.proc:
            return
        data = bytes(self.proc.readAllStandardError())
        if not data:
            return
        msg = data.decode("utf-8", errors="replace").rstrip()
        if msg:
            self._emit({"t": now_iso(), "type": "stderr", "message": msg})

    def _on_finished(self, exit_code: int, _status: QProcess.ExitStatus, step_dirs: Dict[str, Path]) -> None:
        tool = self.current_tool
        step_dir = self.current_step_dir

        meta = None
        if tool and step_dir:
            meta_path = step_dir / "step_meta.json"
            if meta_path.exists():
                try:
                    meta = read_json(meta_path)
                except Exception:
                    meta = None

        if tool and meta and self.on_step_finished:
            self.on_step_finished(tool.id, meta)

        # if failed, stop the queue
        if exit_code != 0:
            if self.on_finished:
                self.on_finished(exit_code, meta)
            self.proc = None
            return

        # else, next
        self.proc = None
        self._start_next(step_dirs)


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        self.setWindowTitle("DocPipe (demo)")
        self.resize(1200, 720)

        self.base_dir = Path(__file__).resolve().parent
        self.work_root = self.base_dir / "work"
        ensure_dir(self.work_root)

        self.orch = Orchestrator(self.work_root)
        self.tools_path = self.base_dir / "tools.yaml"
        self.tools: List[ToolDef] = load_tools_yaml(self.tools_path)
        self.profiles: List[PipelineProfile] = load_pipeline_profiles(self.tools_path, self.tools)

        self.current_item: Optional[WorkItem] = None
        self.current_run_id: Optional[str] = None

        self.runner = ToolRunner(self)
        self.runner.on_event = self.on_tool_event
        self.runner.on_finished = self.on_tool_queue_finished
        self.runner.on_step_finished = self.on_step_finished

        # UI
        self._build_ui()
        self.refresh_items()

        # poll progress.json while running
        self.progress_timer = QTimer(self)
        self.progress_timer.setInterval(250)
        self.progress_timer.timeout.connect(self.poll_progress_file)

    def _build_ui(self) -> None:
        tb = QToolBar("Main")
        self.addToolBar(tb)

        act_import = QAction("Import file...", self)
        act_import.triggered.connect(self.on_import_file)
        tb.addAction(act_import)

        act_refresh = QAction("Refresh", self)
        act_refresh.triggered.connect(self.refresh_items)
        tb.addAction(act_refresh)

        splitter = QSplitter(Qt.Horizontal)
        self.setCentralWidget(splitter)

        # Left: items list
        left = QWidget()
        left_l = QVBoxLayout(left)
        left_l.setContentsMargins(8, 8, 8, 8)

        self.items_list = QListWidget()
        self.items_list.itemSelectionChanged.connect(self.on_item_selected)
        left_l.addWidget(QLabel("Work items"))
        left_l.addWidget(self.items_list, 1)

        self.item_info = QLabel("No item selected")
        self.item_info.setWordWrap(True)
        left_l.addWidget(self.item_info)

        splitter.addWidget(left)
        splitter.setStretchFactor(0, 1)

        # Right: tools + params + console
        right = QWidget()
        right_l = QVBoxLayout(right)
        right_l.setContentsMargins(8, 8, 8, 8)

        # Tool tree (grouped) with checkboxes
        cfg_row = QHBoxLayout()
        self.profile_combo = QComboBox()
        self.profile_combo.addItem("No profile", "")
        for profile in self.profiles:
            self.profile_combo.addItem(profile.name, profile.id)
        self.profile_combo.currentIndexChanged.connect(self.on_profile_changed)

        self.exec_mode_combo = QComboBox()
        self.exec_mode_combo.addItem("Manual (step-by-step)", "manual")
        self.exec_mode_combo.addItem("Automatic (run pipeline)", "automatic")

        cfg_row.addWidget(QLabel("Profile"))
        cfg_row.addWidget(self.profile_combo, 1)
        cfg_row.addWidget(QLabel("Mode"))
        cfg_row.addWidget(self.exec_mode_combo)
        right_l.addLayout(cfg_row)

        top_row = QHBoxLayout()
        self.run_btn = QPushButton("Run")
        self.run_btn.clicked.connect(self.on_run_selected)
        self.run_btn.setEnabled(False)

        self.retry_btn = QPushButton("Retry last failed step")
        self.retry_btn.clicked.connect(self.on_retry_last_failed)
        self.retry_btn.setEnabled(False)

        top_row.addWidget(self.run_btn)
        top_row.addWidget(self.retry_btn)
        top_row.addStretch(1)
        right_l.addLayout(top_row)

        self.tools_tree = QTreeWidget()
        self.tools_tree.setHeaderLabels(["Tool", "Info"])
        self.tools_tree.setColumnWidth(0, 320)
        self.tools_tree.itemSelectionChanged.connect(self.on_tool_selected)
        right_l.addWidget(self.tools_tree, 2)

        # Params area
        self.params_frame = QFrame()
        self.params_layout = QFormLayout(self.params_frame)
        right_l.addWidget(QLabel("Tool parameters"))
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self.params_frame)
        right_l.addWidget(scroll, 1)

        # Progress + status
        prog_row = QHBoxLayout()
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.status_lbl = QLabel("Idle")
        prog_row.addWidget(self.progress, 1)
        prog_row.addWidget(self.status_lbl)
        right_l.addLayout(prog_row)

        # Console
        right_l.addWidget(QLabel("Console / events"))
        self.console = QPlainTextEdit()
        self.console.setReadOnly(True)
        mono = QFont("Consolas")
        mono.setStyleHint(QFont.Monospace)
        self.console.setFont(mono)
        right_l.addWidget(self.console, 2)

        splitter.addWidget(right)
        splitter.setStretchFactor(1, 2)

        self.build_tools_tree()

    def build_tools_tree(self) -> None:
        self.tools_tree.clear()
        groups: Dict[str, QTreeWidgetItem] = {}

        # tools already sorted by (stage, order,...)
        for tool in self.tools:
            grp_name = tool.ui.group
            if grp_name not in groups:
                gitem = QTreeWidgetItem([grp_name, ""])
                gitem.setFlags(gitem.flags() & ~Qt.ItemIsSelectable)
                self.tools_tree.addTopLevelItem(gitem)
                groups[grp_name] = gitem

            gitem = groups[grp_name]
            titem = QTreeWidgetItem([tool.name, tool.description])
            titem.setData(0, Qt.UserRole, tool.id)
            titem.setCheckState(0, Qt.Unchecked)
            gitem.addChild(titem)
            gitem.setExpanded(True)

    def refresh_items(self) -> None:
        self.items_list.clear()
        self.items: List[WorkItem] = self.orch.list_items()
        for it in self.items:
            li = QListWidgetItem(it.display_name)
            li.setData(Qt.UserRole, it.doc_id)
            self.items_list.addItem(li)

        self.current_item = None
        self.current_run_id = None
        self.item_info.setText("No item selected")
        self.run_btn.setEnabled(False)
        self.retry_btn.setEnabled(False)
        self.console.clear()
        self.progress.setValue(0)
        self.status_lbl.setText("Idle")
        self.filter_tools_for_current_item()

    def on_import_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Select a file to import")
        if not path:
            return
        try:
            item = self.orch.import_file(Path(path))
        except Exception as e:
            QMessageBox.critical(self, "Import failed", str(e))
            return

        self.refresh_items()
        # select imported
        for i in range(self.items_list.count()):
            li = self.items_list.item(i)
            if li.data(Qt.UserRole) == item.doc_id:
                self.items_list.setCurrentItem(li)
                break

    def on_item_selected(self) -> None:
        sel = self.items_list.selectedItems()
        if not sel:
            self.current_item = None
            self.current_run_id = None
            self.item_info.setText("No item selected")
            self.run_btn.setEnabled(False)
            self.retry_btn.setEnabled(False)
            self.filter_tools_for_current_item()
            return

        doc_id = sel[0].data(Qt.UserRole)
        item = next((x for x in self.items if x.doc_id == doc_id), None)
        self.current_item = item
        self.console.clear()
        self.progress.setValue(0)
        self.status_lbl.setText("Idle")

        if item:
            m = read_json(item.manifest_path)
            self.current_run_id = m.get("last_run_id")
            self.item_info.setText(
                f"doc_id: {item.doc_id}\n"
                f"type: {item.item_type}\n"
                f"input: {item.input_path}\n"
                f"last_run: {self.current_run_id}"
            )

        self.run_btn.setEnabled(item is not None)
        self.filter_tools_for_current_item()
        self.update_retry_button_state()

    def selected_profile(self) -> Optional[PipelineProfile]:
        profile_id = str(self.profile_combo.currentData() or "")
        if not profile_id:
            return None
        return next((p for p in self.profiles if p.id == profile_id), None)

    def selected_mode(self) -> str:
        return str(self.exec_mode_combo.currentData() or "manual")

    def _artifact_exists_in_run(self, run_id: str, artifact_name: str) -> bool:
        if not self.current_item:
            return False
        run_dir = self.current_item.work_dir / "runs" / run_id / "steps"
        if not run_dir.exists():
            return False
        for step in run_dir.iterdir():
            if not step.is_dir():
                continue
            if (step / artifact_name).exists():
                return True
        return False

    def _tool_is_executable(self, tool: ToolDef) -> bool:
        if not self.current_run_id:
            return True
        for req in tool.requires:
            if not self._artifact_exists_in_run(self.current_run_id, req):
                return False
        return True

    def filter_tools_for_current_item(self) -> None:
        item_type = self.current_item.item_type if self.current_item else None
        profile = self.selected_profile()
        profile_tool_ids = set(profile.tool_ids) if profile else set()

        # show/hide tool items based on input compatibility/profile and disable when prereqs are missing
        root_count = self.tools_tree.topLevelItemCount()
        for gi in range(root_count):
            gitem = self.tools_tree.topLevelItem(gi)
            any_visible = False
            for ci in range(gitem.childCount()):
                titem = gitem.child(ci)
                tool_id = titem.data(0, Qt.UserRole)
                tool = next((t for t in self.tools if t.id == tool_id), None)
                if not tool:
                    titem.setHidden(True)
                    continue

                visible_for_type = bool(item_type and tool_accepts_item(tool, item_type)) if item_type else False
                visible_for_profile = (tool.id in profile_tool_ids) if profile else visible_for_type
                titem.setHidden(not visible_for_profile)
                titem.setDisabled(not self._tool_is_executable(tool))
                if visible_for_profile:
                    any_visible = True
            gitem.setHidden(not any_visible)

    def on_profile_changed(self, _idx: int = 0) -> None:
        profile = self.selected_profile()
        if profile:
            self.run_btn.setText("Run profile")
            for gi in range(self.tools_tree.topLevelItemCount()):
                gitem = self.tools_tree.topLevelItem(gi)
                for ci in range(gitem.childCount()):
                    titem = gitem.child(ci)
                    tool_id = titem.data(0, Qt.UserRole)
                    titem.setCheckState(0, Qt.Checked if tool_id in set(profile.tool_ids) else Qt.Unchecked)
        else:
            self.run_btn.setText("Run")
        self.filter_tools_for_current_item()
    def on_tool_selected(self) -> None:
        sel = self.tools_tree.selectedItems()
        if not sel:
            self.clear_params()
            return
        titem = sel[0]
        tool_id = titem.data(0, Qt.UserRole)
        if not tool_id:
            self.clear_params()
            return
        tool = next((t for t in self.tools if t.id == tool_id), None)
        if not tool:
            self.clear_params()
            return
        self.build_params_for_tool(tool)

    def clear_params(self) -> None:
        while self.params_layout.count():
            it = self.params_layout.takeAt(0)
            w = it.widget()
            if w:
                w.deleteLater()

    def build_params_for_tool(self, tool: ToolDef) -> None:
        self.clear_params()
        self.param_widgets: Dict[str, QWidget] = {}

        for p in tool.params:
            w: QWidget
            if p.type == "int":
                sb = QSpinBox()
                if p.min is not None:
                    sb.setMinimum(int(p.min))
                if p.max is not None:
                    sb.setMaximum(int(p.max))
                if p.default is not None:
                    sb.setValue(int(p.default))
                w = sb
            elif p.type == "float":
                dsb = QDoubleSpinBox()
                dsb.setDecimals(3)
                if p.min is not None:
                    dsb.setMinimum(float(p.min))
                if p.max is not None:
                    dsb.setMaximum(float(p.max))
                if p.default is not None:
                    dsb.setValue(float(p.default))
                w = dsb
            elif p.type == "bool":
                cb = QCheckBox()
                cb.setChecked(bool(p.default))
                w = cb
            elif p.type == "enum":
                combo = QComboBox()
                vals = p.enum or []
                combo.addItems(vals)
                if p.default in vals:
                    combo.setCurrentText(p.default)
                w = combo
            else:
                le = QLineEdit()
                if p.default is not None:
                    le.setText(str(p.default))
                w = le

            self.params_layout.addRow(QLabel(p.label), w)
            self.param_widgets[p.id] = w

    def get_params_for_tool(self, tool: ToolDef) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for p in tool.params:
            w = self.param_widgets.get(p.id)
            if not w:
                continue
            if p.type == "int":
                out[p.id] = int(w.value())  # type: ignore
            elif p.type == "float":
                out[p.id] = float(w.value())  # type: ignore
            elif p.type == "bool":
                out[p.id] = bool(w.isChecked())  # type: ignore
            elif p.type == "enum":
                out[p.id] = str(w.currentText())  # type: ignore
            else:
                out[p.id] = str(w.text())  # type: ignore
        return out

    def _tool_outputs_exist(self, step_dir: Path, tool: ToolDef) -> bool:
        if not tool.produces:
            return False
        return all((step_dir / out).exists() for out in tool.produces)

    def _find_reusable_step_dir(self, tool: ToolDef) -> Optional[Path]:
        if not self.current_item or not tool.produces:
            return None

        runs_dir = self.current_item.work_dir / "runs"
        if not runs_dir.exists():
            return None

        # newest first
        for run_dir in sorted([d for d in runs_dir.iterdir() if d.is_dir()], reverse=True):
            step_dir = run_dir / "steps" / tool.id
            if self._tool_outputs_exist(step_dir, tool):
                return step_dir
        return None

    def _prepare_skipped_outputs(self, src_step_dir: Path, dst_step_dir: Path, tool: ToolDef) -> None:
        for rel_out in tool.produces:
            src = src_step_dir / rel_out
            dst = dst_step_dir / rel_out
            if not src.exists():
                continue
            ensure_dir(dst.parent)
            dst.write_bytes(src.read_bytes())

    def on_run_selected(self) -> None:
        if not self.current_item:
            return
        if self.runner.is_running():
            return

        mode = self.selected_mode()
        profile = self.selected_profile()

        if mode == "automatic":
            if not profile:
                QMessageBox.information(self, "Automatic mode", "Select a pipeline profile to run automatically.")
                return
            profile_ids = set(profile.tool_ids)
            tools = [t for t in self.tools if t.id in profile_ids]
            tools.sort(key=lambda x: (x.ui.stage, x.ui.order, x.id))
        else:
            # Manual mode = one step at a time (selected tool only).
            sel = self.tools_tree.selectedItems()
            tool_id = sel[0].data(0, Qt.UserRole) if sel else None
            tool = next((t for t in self.tools if t.id == tool_id), None)
            if not tool or not self._tool_is_executable(tool):
                QMessageBox.information(self, "Manual mode", "Select one executable tool from the list.")
                return
            tools = [tool]

        if not tools:
            QMessageBox.information(self, "No tools selected", "Select at least one executable tool.")
            return

        # Always create a new run when pressing Run.
        run_id = self.orch.new_run(self.current_item)
        self.current_run_id = run_id

        # prepare step dirs and requests
        step_dirs: Dict[str, Path] = {}
        requests: List[ToolRunRequest] = []

        for tool in tools:
            step_dir = self.orch.step_dir(self.current_item, run_id, tool.id)
            step_dirs[tool.id] = step_dir

            params: Dict[str, Any] = {}
            if mode == "manual" and hasattr(self, "param_widgets"):
                params = self.get_params_for_tool(tool)
            else:
                for p in tool.params:
                    if p.default is not None:
                        params[p.id] = p.default

            skip = False
            skip_reason = ""
            if mode == "automatic":
                src_step_dir = self._find_reusable_step_dir(tool)
                if src_step_dir is not None:
                    self._prepare_skipped_outputs(src_step_dir, step_dir, tool)
                    skip = True
                    skip_reason = "Skipped: outputs already available from a previous run"

            requests.append(ToolRunRequest(tool=tool, params=params, skip=skip, skip_reason=skip_reason))

        self.console.appendPlainText(f"[{now_iso()}] RUN {run_id} mode={mode}: " + ", ".join(t.id for t in tools))
        self.status_lbl.setText(f"Running (run_id={run_id})")
        self.progress.setValue(0)
        self.retry_btn.setEnabled(False)

        self.progress_timer.start()
        self.runner.start_queue(self.current_item, run_id, step_dirs, requests)

    def on_retry_last_failed(self) -> None:
        if not self.current_item or self.runner.is_running():
            return
        last = self.orch.get_last_run_manifest(self.current_item)
        if not last:
            return

        # find first failed step in last run manifest
        failed_tool_id = None
        for tool_id, meta in (last.get("steps") or {}).items():
            if meta and meta.get("status") == "failed":
                failed_tool_id = tool_id
                break
        if not failed_tool_id:
            return

        tool = next((t for t in self.tools if t.id == failed_tool_id), None)
        if not tool:
            return

        run_id = self.orch.new_run(self.current_item)
        self.current_run_id = run_id

        step_dir = self.orch.step_dir(self.current_item, run_id, tool.id)

        # defaults for retry
        params = {}
        for p in tool.params:
            if p.default is not None:
                params[p.id] = p.default

        self.console.appendPlainText(f"[{now_iso()}] RETRY tool={tool.id} in new run {run_id}")
        self.status_lbl.setText(f"Retrying {tool.id} (run_id={run_id})")
        self.progress.setValue(0)

        self.progress_timer.start()
        self.runner.start_queue(self.current_item, run_id, {tool.id: step_dir}, [ToolRunRequest(tool=tool, params=params)])

    def poll_progress_file(self) -> None:
        # If a tool is running, try to read its progress.json (cheap polling).
        if not self.runner.is_running():
            return
        step_dir = self.runner.current_step_dir
        tool = self.runner.current_tool
        if not step_dir or not tool:
            return
        p = step_dir / "progress.json"
        if p.exists():
            try:
                obj = read_json(p)
                prog = float(obj.get("progress", 0.0))
                self.progress.setValue(int(prog * 100))
                msg = obj.get("message", "")
                self.status_lbl.setText(f"{tool.id}: {msg}")
            except Exception:
                pass

    def on_tool_event(self, evt: Dict[str, Any]) -> None:
        # Render events in the console + write per-app log
        t = evt.get("t", now_iso())
        etype = evt.get("type", "event")

        if etype == "progress":
            prog = float(evt.get("progress", 0.0))
            self.progress.setValue(int(prog * 100))
            msg = evt.get("message", "")
            self.status_lbl.setText(f"{evt.get('tool_id', '')}: {msg}")
        elif etype == "log":
            level = evt.get("level", "info")
            msg = evt.get("message", "")
            self.console.appendPlainText(f"[{t}] [{level}] {msg}")
        elif etype == "stderr":
            self.console.appendPlainText(f"[{t}] [stderr] {evt.get('message','')}")
        else:
            self.console.appendPlainText(f"[{t}] {json.dumps(evt, ensure_ascii=False)}")

        # app-level log
        jsonl_append(self.work_root / "ui_events.log.jsonl", {"t": t, **evt})

    def on_step_finished(self, tool_id: str, step_meta: Dict[str, Any]) -> None:
        if self.current_item and self.current_run_id:
            self.orch.record_step_result(self.current_item, self.current_run_id, tool_id, step_meta)
        self.filter_tools_for_current_item()

    def on_tool_queue_finished(self, exit_code: int, step_meta: Optional[Dict[str, Any]]) -> None:
        self.progress_timer.stop()

        if exit_code == 0:
            self.status_lbl.setText("Done")
            self.console.appendPlainText(f"[{now_iso()}] DONE")
            self.progress.setValue(100)
        else:
            self.status_lbl.setText("Failed")
            self.console.appendPlainText(f"[{now_iso()}] FAILED (exit_code={exit_code})")
            self.retry_btn.setEnabled(True)

        self.update_retry_button_state()

    def update_retry_button_state(self) -> None:
        if not self.current_item:
            self.retry_btn.setEnabled(False)
            return
        last = self.orch.get_last_run_manifest(self.current_item)
        if not last:
            self.retry_btn.setEnabled(False)
            return
        any_failed = any((meta or {}).get("status") == "failed" for meta in (last.get("steps") or {}).values())
        self.retry_btn.setEnabled(any_failed and not self.runner.is_running())


def main() -> int:
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
