"""Tests for the workflow-script launch gate (issue #2 validation / issue #4).

These prove the static contract that decides whether a Python workflow script is
safe to launch in the subprocess VM: a valid orchestration script passes, and
every forbidden capability (imports, fs/process/net/clock/randomness reach,
eval/exec, dunder traversal, malformed meta) is rejected with a stable code and
a line-anchored pointer. Pure stdlib, no subprocess.
"""

from hermes_workflows.script_validator import (
    MAX_SOURCE_BYTES,
    validate_script,
    wrap_source,
)

META = 'meta = {"name": "x", "description": "y"}\n'


def _codes(source: str) -> set[str]:
    return {d.code for d in validate_script(source).diagnostics}


# --------------------------------------------------------------------------- #
# Allowed scripts
# --------------------------------------------------------------------------- #

def test_minimal_valid_script_passes():
    v = validate_script(META + 'log("hi")\nreturn {"ok": True}\n')
    assert v.ok
    assert v.diagnostics == []
    assert v.meta == {"name": "x", "description": "y"}


def test_control_flow_and_await_are_allowed():
    src = META + (
        "phase('plan')\n"
        "total = 0\n"
        "for i in range(3):\n"
        "    try:\n"
        "        r = await agent('hermes.echo', {'i': i})\n"
        "        total = total + len(r)\n"
        "    except CapabilityError:\n"
        "        total = total + 1\n"
        "while total < 0:\n"
        "    total = total + 1\n"
        "results = [x for x in range(total)]\n"
        "return {'total': total, 'n': len(results)}\n"
    )
    v = validate_script(src)
    assert v.ok, [d.as_dict() for d in v.diagnostics]


def test_meta_phases_field_is_allowed():
    src = (
        'meta = {"name": "x", "description": "y", "phases": '
        '[{"title": "Plan", "detail": "choose work"}, {"title": "Build"}]}\n'
        "log('go')\n"
    )
    v = validate_script(src)
    assert v.ok
    assert v.meta["phases"] == [
        {"title": "Plan", "detail": "choose work"},
        {"title": "Build"},
    ]


def test_invalid_meta_phases_are_rejected_with_stable_diagnostics():
    v = validate_script(
        'meta = {"name": "x", "description": "y", "phases": '
        '["plan", {"detail": "missing title"}, {"title": "", "detail": "x"}, {"title": "Build", "detail": 3}]}\n'
        "log('go')\n"
    )
    diagnostics = [d.as_dict() for d in v.diagnostics]
    assert not v.ok
    assert {d["code"] for d in diagnostics} == {"E_SCRIPT_META_PHASES"}
    assert [d["pointer"] for d in diagnostics] == [
        "/script/meta/phases/0",
        "/script/meta/phases/1/title",
        "/script/meta/phases/2/title",
        "/script/meta/phases/3/detail",
    ]


def test_safe_builtins_and_helpers_are_allowed():
    src = META + (
        "data = json.dumps({'a': 1})\n"
        "n = math.floor(3.7)\n"
        "xs = sorted([3, 1, 2])\n"
        "return {'data': data, 'n': n, 'xs': xs}\n"
    )
    assert validate_script(src).ok


def test_generic_capability_global_is_allowed():
    src = META + "result = await capability('tools.echo', {'x': 1})\nreturn result\n"
    assert validate_script(src).ok


# --------------------------------------------------------------------------- #
# meta contract
# --------------------------------------------------------------------------- #

def test_missing_meta_is_rejected():
    assert "E_SCRIPT_META_POSITION" in _codes('log("hi")\n')


def test_meta_not_first_statement_is_rejected():
    assert "E_SCRIPT_META_POSITION" in _codes('log("hi")\n' + META)


def test_meta_must_be_pure_literal():
    assert "E_SCRIPT_META_SHAPE" in _codes('meta = dict(name="a", description="b")\n')


def test_meta_requires_name_and_description():
    assert "E_SCRIPT_META_FIELDS" in _codes('meta = {"name": "a"}\n')
    assert "E_SCRIPT_META_FIELDS" in _codes('meta = {"description": "b"}\n')


# --------------------------------------------------------------------------- #
# Forbidden capabilities (the security contract)
# --------------------------------------------------------------------------- #

def test_import_is_rejected():
    assert "E_SCRIPT_IMPORT" in _codes(META + "import os\n")


def test_from_import_is_rejected():
    assert "E_SCRIPT_IMPORT" in _codes(META + "from socket import socket\n")


def test_filesystem_open_is_rejected():
    assert "E_SCRIPT_FORBIDDEN_NAME" in _codes(META + 'open("/etc/passwd")\n')


def test_eval_exec_compile_are_rejected():
    assert "E_SCRIPT_FORBIDDEN_NAME" in _codes(META + 'eval("1+1")\n')
    assert "E_SCRIPT_FORBIDDEN_NAME" in _codes(META + 'exec("x=1")\n')
    assert "E_SCRIPT_FORBIDDEN_NAME" in _codes(META + 'compile("1", "<s>", "eval")\n')


def test_getattr_family_is_rejected():
    assert "E_SCRIPT_FORBIDDEN_NAME" in _codes(META + 'getattr(args, "x")\n')
    assert "E_SCRIPT_FORBIDDEN_NAME" in _codes(META + 'setattr(args, "x", 1)\n')


def test_dynamic_import_dunder_is_rejected():
    assert "E_SCRIPT_DUNDER" in _codes(META + '__import__("os")\n')


def test_dunder_attribute_traversal_is_rejected():
    assert "E_SCRIPT_DUNDER" in _codes(META + "x = (1).__class__\n")
    # The classic breakout chain trips on the first dunder attribute.
    assert "E_SCRIPT_DUNDER" in _codes(META + "x = ().__class__.__bases__\n")


def test_frame_and_coroutine_internals_are_rejected():
    # cr_frame.f_globals -> sys.modules -> os is the classic restricted-builtins
    # escape; none of these internal attributes are dunders, so they need their
    # own rule.
    assert "E_SCRIPT_INTERNAL_ATTR" in _codes(META + 'c = agent("hermes.echo", {})\nx = c.cr_frame\n')
    assert "E_SCRIPT_INTERNAL_ATTR" in _codes(META + "g = (i for i in [1])\nx = g.gi_frame\n")
    assert "E_SCRIPT_INTERNAL_ATTR" in _codes(META + "x = (lambda: 1).co_consts\n")
    assert "E_SCRIPT_INTERNAL_ATTR" in _codes(META + "x = (1).mro\n")
    assert "E_SCRIPT_INTERNAL_ATTR" in _codes(META + "def h():\n    pass\nx = h.f_globals\n")


def test_ordinary_methods_with_internal_like_names_are_allowed():
    # The prefixes end in '_' so real string/list methods never match.
    assert validate_script(META + 'x = "abc".find("b")\ny = [1].count(1)\nz = {1}.copy()\n').ok


def test_class_definition_is_rejected():
    assert "E_SCRIPT_CLASSDEF" in _codes(META + "class C:\n    pass\n")


def test_global_nonlocal_are_rejected():
    assert "E_SCRIPT_SCOPE" in _codes(META + "global z\n")


def test_print_is_rejected_to_protect_rpc_stream():
    # print() would write to stdout and corrupt the framed RPC channel.
    assert "E_SCRIPT_FORBIDDEN_NAME" in _codes(META + 'print("x")\n')


def test_str_format_is_rejected_template_traverses_attributes():
    # "{0.__class__.__base__}".format(x) reaches dunders at runtime, invisible to
    # the AST gate; f-strings remain the safe formatting path.
    assert "E_SCRIPT_FORBIDDEN_NAME" in _codes(META + 'x = "{0}".format(args)\n')
    assert "E_SCRIPT_FORBIDDEN_NAME" in _codes(META + 'x = "{0}".format_map(args)\n')


def test_fstring_attribute_access_is_validated_normally():
    # f-string interpolations are real AST, so a dunder inside one is caught.
    assert "E_SCRIPT_DUNDER" in _codes(META + 'x = f"{args.__class__}"\n')


def test_clock_and_randomness_modules_are_rejected_via_import_ban():
    assert "E_SCRIPT_IMPORT" in _codes(META + "import time\n")
    assert "E_SCRIPT_IMPORT" in _codes(META + "import random\n")
    assert "E_SCRIPT_IMPORT" in _codes(META + "import subprocess\n")


# --------------------------------------------------------------------------- #
# Diagnostics quality + bounds
# --------------------------------------------------------------------------- #

def test_syntax_error_becomes_diagnostic_not_exception():
    v = validate_script(META + "def (:\n")
    assert not v.ok
    assert "E_SCRIPT_SYNTAX" in {d.code for d in v.diagnostics}


def test_diagnostics_point_to_original_line():
    # The offending import is on original line 2 (meta is line 1).
    diag = next(d for d in validate_script(META + "import os\n").diagnostics if d.code == "E_SCRIPT_IMPORT")
    assert diag.pointer == "/script/line/2"


def test_empty_script_is_rejected():
    assert "E_SCRIPT_EMPTY" in _codes("   \n  \n")


def test_oversized_script_is_rejected():
    big = META + ("log('x')\n" * 1)  # base
    big = META + ("x = 1\n" * (MAX_SOURCE_BYTES // 2))
    assert "E_SCRIPT_TOO_LARGE" in _codes(big)


def test_non_string_source_is_rejected():
    assert validate_script(None) is not None  # type: ignore[arg-type]
    assert not validate_script(None).ok  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# wrap_source line mapping
# --------------------------------------------------------------------------- #

def test_wrap_source_offsets_by_one_line():
    wrapped = wrap_source("log('a')\nlog('b')\n")
    lines = wrapped.splitlines()
    assert lines[0].startswith("async def ")
    assert lines[1].strip() == "log('a')"
