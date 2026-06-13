"""File-backed workflow template catalog.

Catalog loading is intentionally boring: template names are single safe path
segments, roots are explicit directories, and template files are JSON workflow
objects named ``<template>.workflow.json``. The catalog never executes anything;
it only lists and loads definitions for the public primitives.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable

from .models import Diagnostic

from . import schema as _schema
from . import sandbox as _sandbox

__all__ = ["FileWorkflowCatalog", "safe_template_name", "default_catalog_roots"]


def safe_template_name(name: str) -> str:
    """Return ``name`` when it is a safe template id, else raise ``ValueError``."""
    if not isinstance(name, str) or not name:
        raise ValueError("template_name must be a non-empty string")
    if "/" in name or "\\" in name or ".." in name:
        raise ValueError(f"unsafe template_name: {name!r}")
    if not all(c.isalnum() or c in "._-" for c in name):
        raise ValueError(f"unsafe template_name: {name!r}")
    safe = name[:-14] if name.endswith(".workflow.json") else name
    if not safe:
        raise ValueError("template_name must be a non-empty string")
    return safe


def default_catalog_roots() -> list[Path]:
    """Return bundled + profile-local catalog roots in lookup order."""
    roots: list[Path] = []
    package_root = Path(__file__).resolve().parents[2]
    bundled = package_root / "examples"
    if bundled.exists():
        roots.append(bundled)
    env_root = os.getenv("HERMES_WORKFLOWS_CATALOG_DIR")
    if env_root:
        roots.append(Path(env_root).expanduser())
    else:
        hermes_home = Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes").expanduser()
        roots.append(hermes_home / "dynamic-workflows" / "templates")
    return roots


class FileWorkflowCatalog:
    """Read-only catalog resolving safe template names from configured roots."""

    def __init__(self, roots: Iterable[str | Path] | None = None) -> None:
        self.roots = [Path(r).expanduser() for r in (roots if roots is not None else default_catalog_roots())]

    def list_templates(self) -> list[dict[str, Any]]:
        """List discovered templates with metadata only."""
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for root in self.roots:
            if not root.exists() or not root.is_dir():
                continue
            for path in sorted(root.glob("*.workflow.json")):
                name = safe_template_name(path.name)
                if name in seen:
                    continue
                try:
                    resolved_root = root.resolve(strict=False)
                    resolved_path = path.resolve(strict=False)
                    resolved_path.relative_to(resolved_root)
                except ValueError:
                    continue
                seen.add(name)
                try:
                    definition = self._load_path(path)
                    workflow_name = definition.get("name")
                    version = definition.get("version")
                    description = definition.get("description")
                    required_inputs = sorted((definition.get("inputs") or {}).keys()) if isinstance(definition.get("inputs"), dict) else []
                    digest = _schema.def_hash(definition)
                    validation_errors = _schema.validate_structure(definition)
                    if not validation_errors:
                        validation_errors.extend(_sandbox.policy_lint(definition))
                    validation_errors = [
                        Diagnostic(severity="error", code=d.code, message=d.message, pointer=d.pointer)
                        if d.severity == "warning"
                        else d
                        for d in validation_errors
                    ]
                    ok = not any(d.severity == "error" for d in validation_errors)
                except Exception as exc:
                    out.append(
                        {
                            "name": name,
                            "path": str(path),
                            "ok": False,
                            "error": {"type": type(exc).__name__, "message": str(exc)},
                        }
                    )
                    continue
                entry = {
                    "name": name,
                    "path": str(path),
                    "ok": ok,
                    "workflow_name": workflow_name,
                    "version": version,
                    "description": description,
                    "required_inputs": required_inputs,
                    "def_hash": digest,
                }
                if validation_errors:
                    entry["validation_errors"] = [d.as_dict() for d in validation_errors]
                out.append(entry)
        return out

    def load_template(self, name: str) -> dict[str, Any]:
        """Load one template definition by safe catalog name."""
        safe = safe_template_name(name)
        filename = f"{safe}.workflow.json"
        for root in self.roots:
            candidate = root / filename
            try:
                resolved_root = root.resolve(strict=False)
                resolved_candidate = candidate.resolve(strict=False)
                resolved_candidate.relative_to(resolved_root)
            except ValueError as exc:
                raise ValueError(f"template path escaped catalog root: {name!r}") from exc
            if candidate.exists() and candidate.is_file():
                return self._load_path(candidate)
        raise FileNotFoundError(f"workflow template not found: {safe!r}")

    @staticmethod
    def _load_path(path: Path) -> dict[str, Any]:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"workflow template must be a JSON object: {path}")
        return data
