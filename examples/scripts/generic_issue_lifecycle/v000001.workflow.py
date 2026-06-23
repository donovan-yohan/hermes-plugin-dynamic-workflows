meta = {
    "name": "generic_issue_lifecycle_harness",
    "description": "Generic issue lifecycle harness driven by repo/profile args instead of hardcoded integrations",
}

log("inventory")
plan = await agent(
    "hermes.echo",
    {
        "phase": "plan",
        "repo": args["repo"],
        "issue": args["issue"],
        "workspace": args.get("workspace", ""),
    },
)

review = await kanban_agent(
    args["review_profile"],
    {"goal": "review exact head", "issue": args["issue"]},
    {"repo": args["repo"], "workspace": args.get("workspace", "")},
)
qa = await kanban_agent(
    args["qa_profile"],
    {"goal": "behavioral QA", "issue": args["issue"]},
    {"repo": args["repo"], "workspace": args.get("workspace", "")},
)

return {
    "planned_phase": plan["echo"]["phase"],
    "review_profile": review["profile"],
    "qa_profile": qa["profile"],
    "issue": args["issue"],
}
