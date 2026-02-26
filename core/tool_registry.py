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


@dataclass(frozen=True)
class PipelineProfile:
    id: str
    name: str
    description: str
    tool_ids: List[str]


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


def load_pipeline_profiles(path: Path, tools: List[ToolDef]) -> List[PipelineProfile]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    profiles_raw = raw.get("profiles", [])

    tool_by_id = {t.id: t for t in tools}
    out: List[PipelineProfile] = []

    for p in profiles_raw:
        ids = [str(x) for x in p.get("tools", [])]
        # keep only tools that currently exist and sort by stage/order
        ids = [x for x in ids if x in tool_by_id]
        ids.sort(key=lambda x: (tool_by_id[x].ui.stage, tool_by_id[x].ui.order, x))

        out.append(
            PipelineProfile(
                id=str(p["id"]),
                name=str(p.get("name", p["id"])),
                description=str(p.get("description", "")),
                tool_ids=ids,
            )
        )

    return out


def tool_accepts_item(tool: ToolDef, item_type: str) -> bool:
    if "*" in tool.accepts:
        return True
    return item_type in tool.accepts
