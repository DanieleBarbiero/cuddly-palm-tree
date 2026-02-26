# core/tool_registry.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class ToolParam:
    id: str
    label: str
    type: str  # "int"|"float"|"bool"|"str"|"enum"
    default: Any = None
    min: Optional[float] = None
    max: Optional[float] = None
    enum: Optional[List[str]] = None


@dataclass(frozen=True)
class ToolUI:
    group: str
    stage: int
    order: int


@dataclass(frozen=True)
class ToolDef:
    id: str
    name: str
    description: str
    command_kind: str  # "python_module"
    command_module: str
    accepts: List[str]         # e.g. ["pdf","xlsx","*"]
    requires: List[str]        # artifact names
    produces: List[str]        # artifact names
    ui: ToolUI
    params: List[ToolParam]


def load_tools_yaml(path: Path) -> List[ToolDef]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    tools_raw = raw.get("tools", [])
    out: List[ToolDef] = []

    for t in tools_raw:
        cmd = t.get("command", {})
        ui = t.get("ui", {})

        params: List[ToolParam] = []
        for p in t.get("params", []):
            params.append(
                ToolParam(
                    id=p["id"],
                    label=p.get("label", p["id"]),
                    type=p["type"],
                    default=p.get("default"),
                    min=p.get("min"),
                    max=p.get("max"),
                    enum=p.get("enum"),
                )
            )

        out.append(
            ToolDef(
                id=t["id"],
                name=t.get("name", t["id"]),
                description=t.get("description", ""),
                command_kind=cmd.get("kind", "python_module"),
                command_module=cmd.get("module", ""),
                accepts=list(t.get("accepts", ["*"])),
                requires=list(t.get("requires", [])),
                produces=list(t.get("produces", [])),
                ui=ToolUI(
                    group=ui.get("group", "99. Other"),
                    stage=int(ui.get("stage", 99)),
                    order=int(ui.get("order", 999)),
                ),
                params=params,
            )
        )

    # Stable sort for UI
    out.sort(key=lambda x: (x.ui.stage, x.ui.order, x.ui.group, x.id))
    return out


def tool_accepts_item(tool: ToolDef, item_type: str) -> bool:
    if "*" in tool.accepts:
        return True
    return item_type in tool.accepts