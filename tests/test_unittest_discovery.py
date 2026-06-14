"""unittest discovery bridge for pytest-style function tests.

The individual test modules are plain functions so pytest can collect them. This
bridge keeps the documented stdlib path useful too: ``python -m unittest
discover -s tests -v`` imports the function-test modules and executes every
``test_*`` function under subTest.
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
    "test_kanban_agent",
    "test_kanban_result_contract",
    "test_kanban_durable_resume",
    "test_kanban_event_log",
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
            for name, fn in sorted(vars(module).items()):
                if name.startswith("test_") and inspect.isfunction(fn):
                    with self.subTest(module=module_name, test=name):
                        fn()
