meta = {
    "name": "loop_until_dry_bughunt_fixture",
    "description": "Sanitized loop-until-dry parity fixture for archive-style bughunt workflows",
    "phases": [
        {"title": "round find", "detail": "run concurrent finder child agents"},
        {"title": "verify", "detail": "verify deduplicated candidate bugs"},
        {"title": "decision", "detail": "update dry counter and continuation state"},
    ],
}

script_args = args or {}
cart_source = script_args.get("cart_source", "")
areas = list(script_args.get("areas", ["totals", "state", "accessibility"]))
verifiers = list(script_args.get("verifiers", ["unit", "integration"]))
max_rounds = script_args.get("max_rounds", 4)
dry_round_limit = script_args.get("dry_round_limit", 1)
round_index = 0
dry_count = 0
candidate_bug_ids = []
verified_bug_ids = []
verified_lookup = {}
remaining_areas = areas
max_round_reached = False


def remember_bug_id(bug_id, target):
    if bug_id not in target:
        target.append(bug_id)


async def find_area(area, round_number):
    return await agent(
        "Find candidate cart.js bugs for area " + area,
        {
            "label": "finder:" + area,
            "phase": "find",
            "schema": {"bugs": "list", "followups": "list"},
            "context": {"area": area, "round": round_number, "cart_source": cart_source},
        },
    )


async def verify_bug(verifier, bug, round_number):
    return await agent(
        "Verify candidate cart.js bug " + bug.get("id", "unknown") + " using " + verifier,
        {
            "label": "verifier:" + verifier + ":" + bug.get("id", "unknown"),
            "phase": "verify",
            "schema": {"verdict": "string", "notes": "string"},
            "context": {"verifier": verifier, "bug": bug, "round": round_number},
        },
    )


while remaining_areas and dry_count < dry_round_limit and round_index < max_rounds:
    round_number = round_index + 1
    phase("round " + str(round_number) + " find")
    finder_results = await parallel([lambda area=area: find_area(area, round_number) for area in remaining_areas])

    bugs_by_id = {}
    next_areas = []
    for result in finder_results:
        for bug in result.get("bugs", []):
            bug_id = bug.get("id")
            if bug_id and bug_id not in bugs_by_id:
                bugs_by_id[bug_id] = bug
                remember_bug_id(bug_id, candidate_bug_ids)
        for area in result.get("followups", []):
            if area not in next_areas:
                next_areas.append(area)

    unverified_bugs = []
    for bug_id in candidate_bug_ids:
        if bug_id in bugs_by_id and bug_id not in verified_lookup:
            unverified_bugs.append(bugs_by_id[bug_id])

    new_verified = []
    if unverified_bugs:
        phase("round " + str(round_number) + " verify")
        verifier_results = await parallel([
            lambda bug=bug, verifier=verifier: verify_bug(verifier, bug, round_number)
            for bug in unverified_bugs
            for verifier in verifiers
        ])
        offset = 0
        for bug in unverified_bugs:
            bug_verdicts = verifier_results[offset:offset + len(verifiers)]
            offset = offset + len(verifiers)
            confirmed = bool(bug_verdicts) and all(v.get("verdict") == "confirmed" for v in bug_verdicts)
            if confirmed:
                bug_id = bug.get("id")
                verified_lookup[bug_id] = True
                remember_bug_id(bug_id, verified_bug_ids)
                remember_bug_id(bug_id, new_verified)

    phase("round " + str(round_number) + " decision")
    if new_verified:
        dry_count = 0
    else:
        dry_count = dry_count + 1
    log(
        "round " + str(round_number) + ": "
        + str(len(bugs_by_id)) + " unique candidates, "
        + str(len(new_verified)) + " new verified, dry_count=" + str(dry_count)
    )
    remaining_areas = next_areas
    round_index = round_index + 1

if remaining_areas and round_index >= max_rounds:
    max_round_reached = True

return {
    "rounds": round_index,
    "dry_count": dry_count,
    "max_round_reached": max_round_reached,
    "verified_bug_ids": sorted(verified_bug_ids),
    "candidate_bug_ids": sorted(candidate_bug_ids),
    "remaining_areas": remaining_areas,
}
