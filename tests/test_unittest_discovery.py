"""unittest discovery bridge for pytest-style function tests.

The individual test modules are plain functions so pytest can collect them. This
bridge keeps the documented stdlib path useful too: ``python -m unittest
discover -s tests -v`` imports the function-test modules and executes every
``test_*`` function under subTest.

A handful of tests (issue #110's backend parametrization) take a single
``backend`` parameter fed by a pytest fixture rather than being zero-arg
functions; a module opts into that by exposing a module-level ``STORE_BACKENDS``
mapping (name -> store class), and this bridge calls each such test once per
entry in that mapping so both discovery paths still exercise every backend.
"""

from __future__ import annotations

import importlib
import inspect
import unittest


MODULES = (
    "test_validate",
    "test_run",
    "test_status",
    "test_plugin_registration",
    "test_file_store_and_kanban",
    "test_control_flow",
    "test_catalog",
    "test_relay_github_contract",
    "test_script_validator",
    "test_rpc",
    "test_vm_subprocess",
    "test_script_store",
    "test_script_store_sqlite",
    "test_pending_writes_resume_contract",
    "test_kanban_agent",
    "test_kanban_result_contract",
    "test_kanban_durable_resume",
    "test_kanban_event_log",
    "test_kanban_notify",
)


def _import_test_module(module_name: str):
    """Import test modules under both unittest and pytest discovery layouts."""
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        return importlib.import_module(f"tests.{module_name}")


class FunctionTestBridge(unittest.TestCase):
    def test_function_modules(self) -> None:
        for module_name in MODULES:
            module = _import_test_module(module_name)
            store_backends = getattr(module, "STORE_BACKENDS", None)
            for name, fn in sorted(vars(module).items()):
                if not (name.startswith("test_") and inspect.isfunction(fn)):
                    continue
                params = list(inspect.signature(fn).parameters)
                if not params:
                    with self.subTest(module=module_name, test=name):
                        fn()
                elif params == ["backend"] and store_backends:
                    for backend_name, store_cls in sorted(store_backends.items()):
                        with self.subTest(module=module_name, test=name, backend=backend_name):
                            fn(store_cls)
