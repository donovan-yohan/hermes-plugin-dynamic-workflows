"""Versioned file-backed catalog for saved Python workflow-script harnesses.

A script harness is a model-authored Python workflow script accepted by the
subprocess VM launch gate. The catalog is deliberately boring and file-backed:
script names are single safe path segments, every save creates or replaces an
explicit integer version, and source is validated before it is persisted.

Layout under each configured root::

    <root>/<script-name>/v000001.workflow.py
    <root>/<script-name>/v000001.meta.json

The catalog never executes scripts by itself; callers load source and pass it to
:func:`hermes_workflows.primitives.run_workflow_script` / the VM boundary.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

from .errors import ScriptValidationError
from .script_store import script_sha256
from .script_validator import validate_script


class _ScriptCatalogMetadataError(Exception):
    """Raised when a saved script metadata sidecar cannot be trusted."""


__all__ = [
    "ScriptCatalogEntry",
    "FileWorkflowScriptCatalog",
    "default_script_catalog_roots",
    "safe_script_name",
    "safe_script_path",
]

_SCRIPT_VERSION_RE = re.compile(r"^v(?P<n>[0-9]{6})\.workflow(?:\.py)?$")


def safe_script_name(name: str) -> str:
    """Return ``name`` when it is a safe script id, else raise ``ValueError``."""
    if not isinstance(name, str) or not name:
        raise ValueError("script_name must be a non-empty string")
    if name.endswith(".workflow.py"):
        name = name[: -len(".workflow.py")]
    if "/" in name or "\\" in name or ".." in name:
        raise ValueError(f"unsafe script_name: {name!r}")
    if not all(c.isalnum() or c in "._-" for c in name):
        raise ValueError(f"unsafe script_name: {name!r}")
    if not name or name.startswith("."):
        raise ValueError(f"unsafe script_name: {name!r}")
    return name


def safe_script_path(path: str) -> str:
    """Return a safe catalog-relative script path, else raise ``ValueError``."""
    if not isinstance(path, str) or not path:
        raise ValueError("script_path must be a non-empty string")
    if "\\" in path:
        raise ValueError(f"unsafe script_path: {path!r}")
    candidate = Path(path)
    if candidate.is_absolute():
        raise ValueError(f"unsafe script_path: {path!r}")
    parts = candidate.parts
    if not parts or any(part in ("", ".", "..") for part in parts):
        raise ValueError(f"unsafe script_path: {path!r}")
    if any(part.startswith(".") for part in parts):
        raise ValueError(f"unsafe script_path: {path!r}")
    if not candidate.name.endswith((".workflow", ".workflow.py")):
        raise ValueError("script_path must point to a .workflow or .workflow.py file")
    return "/".join(parts)


def default_script_catalog_roots() -> list[Path]:
    """Return profile-local + bundled script catalog roots in lookup order."""
    roots: list[Path] = []
    env_root = os.getenv("HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR")
    if env_root:
        roots.append(Path(env_root).expanduser())
    else:
        hermes_home = Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes").expanduser()
        roots.append(hermes_home / "dynamic-workflows" / "scripts")
    package_root = Path(__file__).resolve().parent
    bundled = package_root / "examples" / "scripts"
    if bundled.exists():
        roots.append(bundled)
    # Source checkouts keep human-readable examples at the repository root.
    # Wheels/sdists carry the package-local copy via package-data below.
    repo_bundled = package_root.parents[1] / "examples" / "scripts"
    if repo_bundled.exists() and repo_bundled.resolve(strict=False) != bundled.resolve(strict=False):
        roots.append(repo_bundled)
    return roots


def _version_token(version: int) -> str:
    if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
        raise ValueError("script version must be a positive integer")
    return f"v{version:06d}"


@dataclass(frozen=True)
class ScriptCatalogEntry:
    """Metadata for one saved script harness version."""

    name: str
    version: int
    path: str
    meta: Optional[dict[str, Any]]
    script_sha256: str
    ok: bool
    diagnostics: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "name": self.name,
            "version": self.version,
            "path": self.path,
            "meta": self.meta,
            "script_sha256": self.script_sha256,
            "ok": self.ok,
        }
        if self.diagnostics:
            out["diagnostics"] = list(self.diagnostics)
        return out


class FileWorkflowScriptCatalog:
    """Versioned file-backed catalog for validated workflow-script harnesses."""

    def __init__(self, roots: Iterable[str | Path] | None = None) -> None:
        self.roots = [Path(r).expanduser() for r in (roots if roots is not None else default_script_catalog_roots())]

    def list_scripts(self, *, include_versions: bool = False) -> list[dict[str, Any]]:
        """List saved scripts.

        By default returns only the latest version of each script name. With
        ``include_versions=True`` returns every discovered version.
        """
        entries: list[ScriptCatalogEntry] = []
        seen_latest: set[str] = set()
        seen_versions: set[tuple[str, int]] = set()
        for root in self.roots:
            if not root.exists() or not root.is_dir():
                continue
            try:
                script_dirs = sorted(p for p in root.iterdir() if p.is_dir())
            except OSError:
                continue
            for script_dir in script_dirs:
                try:
                    name = safe_script_name(script_dir.name)
                    resolved_root = root.resolve(strict=False)
                    resolved_dir = script_dir.resolve(strict=False)
                    resolved_dir.relative_to(resolved_root)
                except ValueError:
                    continue
                versions = self._versions_for_dir(root, script_dir)
                if not versions:
                    continue
                selected = versions if include_versions else [versions[-1]]
                if not include_versions and name in seen_latest:
                    continue
                for version in selected:
                    key = (name, version)
                    if key in seen_versions:
                        continue
                    try:
                        entry_path = self._script_path_for_version(root, script_dir, version)
                        entry = self._entry_for(entry_path, name, version)
                    except (OSError, UnicodeDecodeError, ValueError, FileNotFoundError):
                        continue
                    seen_versions.add(key)
                    entries.append(entry)
                    if not include_versions:
                        seen_latest.add(name)
        return [e.as_dict() for e in entries]

    def list_versions(self, name: str) -> list[int]:
        """Return known versions for one script name across all roots."""
        safe = safe_script_name(name)
        versions: set[int] = set()
        for root in self.roots:
            script_dir = root / safe
            try:
                self._ensure_within_root(root, script_dir)
            except ValueError:
                continue
            versions.update(self._versions_for_dir(root, script_dir))
        return sorted(versions)

    def inspect_script(self, name: str, *, version: Optional[int] = None, include_source: bool = False) -> dict[str, Any]:
        """Inspect one saved script version, optionally including source text."""
        source_path = self._resolve_script_path(name, version=version)
        entry = self._entry_for(source_path, safe_script_name(name), self._version_from_path(source_path)).as_dict()
        if include_source:
            entry["source"] = source_path.read_text(encoding="utf-8")
        return entry

    def load_script(self, name: str, *, version: Optional[int] = None) -> str:
        """Load one saved script source by name/version."""
        return self._resolve_script_path(name, version=version).read_text(encoding="utf-8")

    def load_script_path(self, path: str) -> str:
        """Load one catalog-relative script file without allowing root escape."""
        return self._resolve_relative_script_path(path).read_text(encoding="utf-8")

    def save_script(
        self,
        name: str,
        source: str,
        *,
        version: Optional[int] = None,
        replace: bool = False,
    ) -> dict[str, Any]:
        """Validate and save a script harness version.

        ``version=None`` appends after the highest visible version across all
        roots. Existing versions are immutable unless ``replace=True``.
        """
        safe = safe_script_name(name)
        validation = validate_script(source)
        if not validation.ok:
            raise ScriptValidationError(validation.diagnostics)
        root = self._write_root()
        script_dir = root / safe
        self._ensure_within_root(root, script_dir)
        all_versions = self.list_versions(safe)
        chosen = version if version is not None else ((all_versions[-1] + 1) if all_versions else 1)
        token = _version_token(chosen)
        script_path = script_dir / f"{token}.workflow.py"
        meta_path = script_dir / f"{token}.meta.json"
        if chosen in all_versions and not replace:
            raise FileExistsError(f"script version already exists: {safe}@{chosen}")
        script_dir.mkdir(parents=True, exist_ok=True)
        self._atomic_write_text(script_path, source)
        metadata = {
            "name": safe,
            "version": chosen,
            "script_sha256": script_sha256(source),
            "meta": validation.meta,
        }
        self._atomic_write_text(meta_path, json.dumps(metadata, sort_keys=True, indent=2) + "\n")
        return self._entry_for(script_path, safe, chosen).as_dict()

    def _write_root(self) -> Path:
        if not self.roots:
            raise ValueError("script catalog has no roots")
        return self.roots[0]

    @staticmethod
    def _ensure_within_root(root: Path, path: Path) -> None:
        try:
            resolved_root = root.resolve(strict=False)
            resolved_path = path.resolve(strict=False)
            resolved_path.relative_to(resolved_root)
        except ValueError as exc:
            raise ValueError("script catalog path escaped root") from exc

    def _resolve_script_path(self, name: str, *, version: Optional[int]) -> Path:
        safe = safe_script_name(name)
        for root in self.roots:
            script_dir = root / safe
            try:
                self._ensure_within_root(root, script_dir)
            except ValueError:
                continue
            versions = self._versions_for_dir(root, script_dir)
            if not versions:
                continue
            chosen = version if version is not None else versions[-1]
            if chosen not in versions:
                continue
            path = self._script_path_for_version(root, script_dir, chosen)
            return path
        suffix = "latest" if version is None else str(version)
        raise FileNotFoundError(f"workflow script not found: {safe}@{suffix}")

    def _resolve_relative_script_path(self, path: str) -> Path:
        rel = Path(safe_script_path(path))
        for root in self.roots:
            candidate = root / rel
            try:
                self._ensure_within_root(root, candidate)
            except ValueError:
                continue
            if candidate.exists() and candidate.is_file():
                return candidate
        raise FileNotFoundError(f"workflow script path not found: {rel.as_posix()}")

    def _script_path_for_version(self, root: Path, script_dir: Path, version: int) -> Path:
        token = _version_token(version)
        for suffix in (".workflow.py", ".workflow"):
            path = script_dir / f"{token}{suffix}"
            try:
                self._ensure_within_root(root, path)
            except ValueError:
                continue
            if path.exists() and path.is_file():
                return path
        raise FileNotFoundError(f"workflow script version not found: {script_dir.name}@{version}")

    def _versions_for_dir(self, root: Path, script_dir: Path) -> list[int]:
        if not script_dir.exists() or not script_dir.is_dir():
            return []
        versions: list[int] = []
        try:
            candidates = list(script_dir.glob("v*.workflow.py")) + list(script_dir.glob("v*.workflow"))
        except OSError:
            return []
        for path in candidates:
            m = _SCRIPT_VERSION_RE.match(path.name)
            if not m or not path.is_file():
                continue
            try:
                self._ensure_within_root(root, path)
            except ValueError:
                continue
            versions.append(int(m.group("n")))
        return sorted(set(versions))

    @staticmethod
    def _version_from_path(path: Path) -> int:
        m = _SCRIPT_VERSION_RE.match(path.name)
        if not m:
            raise ValueError(f"not a script version path: {path}")
        return int(m.group("n"))

    def _root_for_path(self, path: Path) -> Path:
        for root in self.roots:
            try:
                self._ensure_within_root(root, path)
                return root
            except ValueError:
                continue
        raise ValueError("script catalog path escaped root")

    def _entry_for(self, path: Path, name: str, version: int) -> ScriptCatalogEntry:
        root = self._root_for_path(path)
        self._ensure_within_root(root, path)
        source = path.read_text(encoding="utf-8")
        digest = script_sha256(source)
        meta_path = path.with_name(f"{_version_token(version)}.meta.json")
        try:
            self._ensure_within_root(root, meta_path)
        except ValueError:
            pass
        else:
            if meta_path.exists() and meta_path.is_file():
                try:
                    data = self._load_meta(meta_path)
                    if data.get("script_sha256") == digest:
                        return ScriptCatalogEntry(
                            name=name,
                            version=version,
                            path=str(path),
                            meta=data.get("meta"),
                            script_sha256=digest,
                            ok=True,
                        )
                except _ScriptCatalogMetadataError:
                    pass
        validation = validate_script(source)
        return ScriptCatalogEntry(
            name=name,
            version=version,
            path=str(path),
            meta=validation.meta,
            script_sha256=digest,
            ok=validation.ok,
            diagnostics=[d.as_dict() for d in validation.diagnostics],
        )

    @staticmethod
    def _load_meta(path: Path) -> dict[str, Any]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _ScriptCatalogMetadataError(f"failed to load script metadata: {path}") from exc
        if not isinstance(data, dict):
            raise _ScriptCatalogMetadataError(f"script metadata must be an object: {path}")
        return data

    @staticmethod
    def _atomic_write_text(path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, path)
        finally:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
