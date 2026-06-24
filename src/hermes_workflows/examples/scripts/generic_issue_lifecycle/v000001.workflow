meta = {
    "name": "generic_issue_lifecycle_harness",
    "description": "Argument-bound GitHub issue lifecycle harness with exact-head review, QA, fix-loop, and release closeout gates",
    "phases": ["inventory", "plan", "implement", "verify", "fix", "release", "closeout"],
}

repo = args["repo"]
issue = args.get("issue") or args.get("issue_number")
base_branch = args.get("base_branch", "main")
workspace_path = args.get("workspace", "")
profiles = args.get("profile_bindings", {})
board = args.get("board")
tenant = args.get("tenant")
max_fix_attempts = args.get("max_fix_attempts", 1)

workspace_cfg = {"type": "dir", "path": workspace_path} if workspace_path else {"type": "scratch"}
issue_ref = {"repo": repo, "issue": issue, "base_branch": base_branch}
card_schema = {"task_id": "string", "status": "string", "result": "object"}
gate_schema = {"approved": "bool", "head_sha": "string", "blockers": "list"}


def profile_for(role, fallback):
    if isinstance(profiles, dict) and profiles.get(role):
        return profiles[role]
    legacy = args.get(role + "_profile")
    if legacy:
        return legacy
    return fallback


def result_payload(call):
    if not isinstance(call, dict):
        return {}
    workflow_result = call.get("workflow_result")
    if isinstance(workflow_result, dict):
        return workflow_result
    result = call.get("result")
    if isinstance(result, dict):
        return result
    return {}


def gate_payload(call):
    if not isinstance(call, dict):
        return {}
    if "approved" in call or "head_sha" in call:
        return call
    return result_payload(call)


def gate_approved(call, expected_head):
    result = gate_payload(call)
    return (
        result.get("approved") is True
        and bool(expected_head)
        and result.get("head_sha") == expected_head
    )


planner = profile_for("planner", "planner")
implementer = profile_for("implementer", "implementer")
reviewer = profile_for("reviewer", "reviewer")
qa_profile = profile_for("qa", "qa")
fixer = profile_for("fixer", implementer)
ops = profile_for("ops", "ops")

phase("inventory")
inventory = await agent(
    "hermes.echo",
    {
        "phase": "inventory",
        "repo": repo,
        "issue": issue,
        "base_branch": base_branch,
        "workspace": workspace_path,
        "required_outputs": [
            "issue acceptance criteria",
            "linked PRs and exact heads",
            "known blockers",
            "docs/examples that may need updates",
        ],
    },
    schema={"echo": "object", "digest": "string"},
)

phase("plan")
plan = await kanban_agent(
    planner,
    {
        "goal": "Plan one honest implementation slice for this GitHub issue.",
        "issue": issue_ref,
        "inventory": inventory["echo"],
        "acceptance": [
            "profiles come from args/profile_bindings, never hardcoded deployment names",
            "implementation opens or updates exactly one PR against base_branch",
            "review and QA must validate the exact current PR head",
            "release/closeout runs only after exact-head gates pass",
        ],
    },
    {"repo": repo, "workspace": workspace_path, "base_branch": base_branch},
    title="plan issue lifecycle slice",
    board=board,
    tenant=tenant,
    workspace=workspace_cfg,
    schema=card_schema,
)

phase("implement")
implementation = await kanban_agent(
    implementer,
    {
        "goal": "Implement the planned slice and produce a PR/handoff.",
        "issue": issue_ref,
        "plan": result_payload(plan),
        "requirements": [
            "record PR URL/number and head sha in structured output when available",
            "include verification commands and docs changes",
            "do not close parent/epic issues unless all acceptance criteria are satisfied",
        ],
    },
    {"repo": repo, "workspace": workspace_path, "base_branch": base_branch},
    title="implement issue lifecycle slice",
    board=board,
    tenant=tenant,
    workspace=workspace_cfg,
    schema=card_schema,
)

phase("exact-head")
impl_result = result_payload(implementation)
head = await agent(
    "hermes.github.pr_head",
    {
        "repo": repo,
        "issue": issue,
        "implementation": impl_result,
        "expected_head_sha": args.get("expected_head_sha", impl_result.get("head_sha", "")),
    },
    schema={"head_sha": "string", "head_ref": "string"},
)
expected_head = head["head_sha"]

phase("verify")
review = await kanban_agent(
    reviewer,
    {
        "goal": "Review the exact PR head for correctness and release risk.",
        "issue": issue_ref,
        "expected_head_sha": expected_head,
        "implementation": impl_result,
        "return_contract": gate_schema,
    },
    {"repo": repo, "workspace": workspace_path, "head_sha": expected_head},
    title="review exact PR head",
    board=board,
    tenant=tenant,
    workspace=workspace_cfg,
    schema=gate_schema,
)
qa = await kanban_agent(
    qa_profile,
    {
        "goal": "Run behavioral QA against the exact PR head.",
        "issue": issue_ref,
        "expected_head_sha": expected_head,
        "implementation": impl_result,
        "return_contract": gate_schema,
    },
    {"repo": repo, "workspace": workspace_path, "head_sha": expected_head},
    title="qa exact PR head",
    board=board,
    tenant=tenant,
    workspace=workspace_cfg,
    schema=gate_schema,
)

review_ok = gate_approved(review, expected_head)
qa_ok = gate_approved(qa, expected_head)
fix_attempts = 0
fix = {}

while (not review_ok or not qa_ok) and fix_attempts < max_fix_attempts:
    phase("fix")
    fix_attempts = fix_attempts + 1
    fix = await kanban_agent(
        fixer,
        {
            "goal": "Fix review/QA blockers and produce a fresh PR head.",
            "attempt": fix_attempts,
            "issue": issue_ref,
            "expected_head_sha": expected_head,
            "review": gate_payload(review),
            "qa": gate_payload(qa),
            "requirements": ["update the PR", "report the fresh head sha", "preserve evidence"],
        },
        {"repo": repo, "workspace": workspace_path, "previous_head_sha": expected_head, "attempt": fix_attempts},
        title="fix blocked issue lifecycle slice",
        board=board,
        tenant=tenant,
        workspace=workspace_cfg,
        schema=card_schema,
    )
    fix_result = result_payload(fix)
    head = await agent(
        "hermes.github.pr_head",
        {
            "repo": repo,
            "issue": issue,
            "implementation": fix_result,
            "expected_head_sha": fix_result.get("head_sha", expected_head),
        },
        schema={"head_sha": "string", "head_ref": "string"},
    )
    expected_head = head["head_sha"]
    review = await kanban_agent(
        reviewer,
        {
            "goal": "Re-review the fresh exact PR head.",
            "issue": issue_ref,
            "expected_head_sha": expected_head,
            "fix": fix_result,
            "return_contract": gate_schema,
        },
        {"repo": repo, "workspace": workspace_path, "head_sha": expected_head, "attempt": fix_attempts},
        title="review fixed exact PR head",
        board=board,
        tenant=tenant,
        workspace=workspace_cfg,
        schema=gate_schema,
    )
    qa = await kanban_agent(
        qa_profile,
        {
            "goal": "Re-run QA against the fresh exact PR head.",
            "issue": issue_ref,
            "expected_head_sha": expected_head,
            "fix": fix_result,
            "return_contract": gate_schema,
        },
        {"repo": repo, "workspace": workspace_path, "head_sha": expected_head, "attempt": fix_attempts},
        title="qa fixed exact PR head",
        board=board,
        tenant=tenant,
        workspace=workspace_cfg,
        schema=gate_schema,
    )
    review_ok = gate_approved(review, expected_head)
    qa_ok = gate_approved(qa, expected_head)

if not review_ok or not qa_ok:
    return {
        "repo": repo,
        "issue": issue,
        "profiles": {
            "planner": planner,
            "implementer": implementer,
            "reviewer": reviewer,
            "qa": qa_profile,
            "fixer": fixer,
            "ops": ops,
        },
        "head_sha": expected_head,
        "review_ok": review_ok,
        "qa_ok": qa_ok,
        "fix_attempted": fix_attempts > 0,
        "fix_attempts": fix_attempts,
        "release": False,
        "blocked": True,
        "blockers": {"review": gate_payload(review).get("blockers", []), "qa": gate_payload(qa).get("blockers", [])},
        "closeout_status": None,
        "closeout_task_id": None,
    }

phase("release")
release_gate = await agent(
    "hermes.github.release_exact_head",
    {
        "repo": repo,
        "issue": issue,
        "expected_head_sha": expected_head,
        "review": {"approved": review_ok, "head_sha": expected_head, "raw": gate_payload(review)},
        "qa": {"approved": qa_ok, "head_sha": expected_head, "raw": gate_payload(qa)},
    },
    schema={"release": "bool", "head_sha": "string"},
)

phase("closeout")
closeout = await kanban_agent(
    ops,
    {
        "goal": "Perform release closeout after exact-head gates passed.",
        "issue": issue_ref,
        "release_gate": release_gate,
        "expected_head_sha": expected_head,
        "review_ok": review_ok,
        "qa_ok": qa_ok,
        "requirements": [
            "merge/release only the verified head when release=true",
            "comment with PR, merge commit, tests, docs, and residuals",
            "close only fully satisfied issues",
            "open follow-up issues for remaining blockers",
        ],
    },
    {"repo": repo, "workspace": workspace_path, "head_sha": expected_head},
    title="release and close issue lifecycle",
    board=board,
    tenant=tenant,
    workspace=workspace_cfg,
    schema=card_schema,
)

return {
    "repo": repo,
    "issue": issue,
    "profiles": {
        "planner": planner,
        "implementer": implementer,
        "reviewer": reviewer,
        "qa": qa_profile,
        "fixer": fixer,
        "ops": ops,
    },
    "head_sha": expected_head,
    "review_ok": review_ok,
    "qa_ok": qa_ok,
    "fix_attempted": fix_attempts > 0,
    "fix_attempts": fix_attempts,
    "release": release_gate["release"],
    "blocked": False,
    "closeout_status": closeout.get("status"),
    "closeout_task_id": closeout.get("task_id"),
}
