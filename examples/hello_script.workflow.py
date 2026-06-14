# Example Python workflow *script* for the subprocess VM (issue #2).
#
# Unlike the declarative `*.workflow.json` files, this is a deterministic
# orchestration *script*. It never runs inside the parent Hermes process: the
# parent statically validates it (workflow_validate_script), then executes it in
# a sandboxed subprocess with a scrubbed environment and a narrow RPC channel.
# Every `agent(...)` / `log(...)` / `phase(...)` call is brokered by the parent.
#
# It is NOT a model-facing tool and is NOT loaded by the JSON template catalog
# (which only globs `*.workflow.json`). Run it explicitly, e.g.:
#
#     from hermes_workflows import run_workflow_script
#     src = open("examples/hello_script.workflow.py").read()
#     print(run_workflow_script(src, args={"name": "world"}).value)
#
# Allowed: deterministic control flow, the RPC-backed globals
# (agent/kanban_agent/parallel/pipeline/phase/log/workflow), args/budget, and the
# pre-bound deterministic json/math. Forbidden (rejected before launch): imports,
# filesystem/network/process/env/clock/randomness, dunder traversal, eval/exec.

meta = {
    "name": "hello-script",
    "description": "Greet a subject then shout it, via brokered agent calls.",
    "phases": ["greet", "shout"],
}

phase("greet")
log("greeting " + str(args["name"]))
greeting = await agent(
    "hermes.greeter",
    {"subject": args["name"]},
    schema={"greeting": "string"},
)

phase("shout")
shout = await agent(
    "hermes.uppercaser",
    {"text": greeting["greeting"]},
    schema={"result": "string"},
)

return {"greeting": greeting["greeting"], "shout": shout["result"]}
