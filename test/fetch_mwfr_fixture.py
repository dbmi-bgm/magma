#!/usr/bin/env python3
"""Capture a `mwfr_from_input` test case from real portal data.

Fetches a posted MetaWorkflowRun and the MetaWorkflow it was built from, reduces both
to the fields `test_mwfr_from_input_smaht.py` compares, and writes them as a case
directory under `test/files/mwfr_from_input/`.

    python test/fetch_mwfr_fixture.py --mwfr SMAMR42VUPMD \
        --input-arg input_files_bam --env data

The case directory is named after the MetaWorkflow unless `--case` says otherwise, so
re-capturing the same pipeline overwrites its fixture instead of accumulating variants.

This is a developer tool, run by hand. It is never exercised by the test suite (pytest
only collects `test_*.py`) and it is the only thing here that touches the network.

Before writing anything it regenerates the MetaWorkflowRun from the captured pair and
compares. A mismatch means the case is not usable and the script says why rather than
committing a fixture that lands as a red test -- see MetaWorkflow drift below.
"""

import argparse
import json
import os
import sys

# Allow `python test/fetch_mwfr_fixture.py` from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from magma_smaht.utils import get_auth_key, get_item, mwfr_from_input

from test.test_mwfr_from_input_smaht import CASES_DIR, describe_difference, normalize

# Runtime state written by the pipeline after the MWFR was posted. None of it is
# produced by mwfr_from_input, so none of it can be compared against.
VOLATILE_RUN_FIELDS = ("output", "job_id", "workflow_run", "status")

# Everything the fixture deliberately does not keep, and why:
#   - portal-assigned on POST:  accession, schema_version, status, date_created,
#     last_modified, uuid
#   - runtime state:            final_status, cost, failed_jobs
#   - set by callers, not by mwfr_from_input: common_fields, file_sets, analysis_runs,
#     description
#   - identifying information that must not land in a public repo: submitted_by is a
#     user email, and `tags` carries a donor accession for sample identity checks
#     (see get_tag_for_sample_identity_check in magma_smaht/utils.py)
# `consortia` and `submission_centers` are dropped too: they are linkTos, so the raw
# frame returns UUIDs while mwfr_from_input returns the identifiers "smaht" /
# "smaht_dac". The test asserts those literals directly instead.
KEPT_MWFR_FIELDS = ("meta_workflow", "input", "workflow_runs")


def reduce_mwfr(mwfr):
    """The posted MetaWorkflowRun cut down to what the test can legitimately compare."""
    reduced = {field: mwfr[field] for field in KEPT_MWFR_FIELDS}
    reduced["workflow_runs"] = [
        {
            key: value
            for key, value in workflow_run.items()
            if key not in VOLATILE_RUN_FIELDS
        }
        for workflow_run in mwfr["workflow_runs"]
    ]
    return reduced


def check_not_superseded(mwfr):
    """Reject MetaWorkflowRuns whose shard graph cannot be regenerated.

    `rerun_mwfr` (magma_smaht/wrangler_utils.py) splices whole workflow_run dicts from an
    older run into a new one and then marks the old run deleted and tagged `rerun`.
    Either side of that swap can carry dependencies that today's MetaWorkflow will not
    reproduce, so neither makes a usable fixture.
    """
    reasons = []
    if mwfr.get("status") == "deleted":
        reasons.append("status is 'deleted'")
    if "rerun" in mwfr.get("tags", []):
        reasons.append("tagged 'rerun'")
    if mwfr.get("description", "").startswith("Rerun of"):
        reasons.append("description marks it as a rerun")
    return reasons


def check_metaworkflow_drift(metaworkflow, mwfr):
    """Warn when the MetaWorkflow was edited after the MetaWorkflowRun was posted.

    MetaWorkflow items are patched in place. A run posted before such an edit links to a
    uuid whose `workflows` no longer describe the graph that was actually built -- this
    is not hypothetical, it is why the PacBio pair in the smaht-portal test inserts no
    longer regenerates. The comparison below is what actually decides, but this makes the
    cause legible when it fails.
    """
    modified = metaworkflow.get("last_modified", {}).get("date_modified")
    created = mwfr.get("date_created")
    if modified and created and modified > created:
        return (
            "MetaWorkflow was last modified {0}, after the MetaWorkflowRun was created "
            "{1} -- it may no longer describe the graph this run was built from".format(
                modified, created
            )
        )
    return None


def check_input_arg(mwfr, input_arg):
    """Validate --input-arg, and report other file arguments that imply a different shape.

    mwfr_from_input derives the whole scatter structure from this one argument and raises
    an unhelpful UnboundLocalError if it does not match anything, so check it here. Which
    argument is correct cannot be inferred: several builders (ONT, paired-end) post two
    file arguments with identical dimensions, and picking by shape would silently bake
    the wrong contract into the fixture.
    """
    file_args = {
        arg["argument_name"]: arg
        for arg in mwfr["input"]
        if arg.get("argument_type") == "file" and arg.get("files")
    }
    if input_arg not in file_args:
        raise SystemExit(
            "--input-arg '{0}' is not a file argument with files on this "
            "MetaWorkflowRun.\nCandidates: {1}".format(
                input_arg, ", ".join(sorted(file_args)) or "(none)"
            )
        )

    from magma_smaht.utils import generate_input_structure

    chosen = generate_input_structure(file_args[input_arg]["files"])
    ambiguous = sorted(
        name
        for name, arg in file_args.items()
        if name != input_arg and generate_input_structure(arg["files"]) != chosen
    )
    return chosen, ambiguous


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case",
        help="fixture directory name (defaults to the MetaWorkflow's name)",
    )
    parser.add_argument("--mwfr", required=True, help="MetaWorkflowRun accession or uuid")
    parser.add_argument(
        "--input-arg",
        required=True,
        help="argument_name whose dimensions define the input structure",
    )
    parser.add_argument("--env", default="data", help="key in ~/.smaht-keys.json")
    parser.add_argument(
        "--description", default="", help="one-line note stored in case.json"
    )
    parser.add_argument(
        "--force", action="store_true", help="write the fixture even if it does not match"
    )
    args = parser.parse_args()

    key = get_auth_key(args.env)
    print("Fetching MetaWorkflowRun {0} from '{1}'...".format(args.mwfr, args.env))
    mwfr = get_item(args.mwfr, key)

    superseded = check_not_superseded(mwfr)
    if superseded:
        raise SystemExit(
            "{0} is not usable as a fixture ({1}). Pick a run that was posted directly "
            "rather than one produced or replaced by rerun_mwfr.".format(
                args.mwfr, "; ".join(superseded)
            )
        )

    # Fetch the MetaWorkflow *through* the run so the pair is guaranteed consistent.
    print("Fetching MetaWorkflow {0}...".format(mwfr["meta_workflow"]))
    metaworkflow = get_item(mwfr["meta_workflow"], key)

    case_name = args.case or metaworkflow.get("name")
    if not case_name:
        raise SystemExit(
            "MetaWorkflow {0} has no name to fall back on -- pass --case "
            "explicitly.".format(metaworkflow["uuid"])
        )
    if not args.case:
        print("Using MetaWorkflow name '{0}' as the case name.".format(case_name))

    drift = check_metaworkflow_drift(metaworkflow, mwfr)
    if drift:
        print("WARNING: {0}".format(drift))

    structure, ambiguous = check_input_arg(mwfr, args.input_arg)
    print("Input structure from '{0}': {1}".format(args.input_arg, structure))
    if ambiguous:
        print(
            "NOTE: these file arguments imply a different structure, so the choice of "
            "--input-arg matters here: {0}".format(", ".join(ambiguous))
        )

    expected = reduce_mwfr(mwfr)
    actual = mwfr_from_input(
        metaworkflow["uuid"], expected["input"], args.input_arg, key
    )

    difference = describe_difference(normalize(actual), normalize(expected))
    if difference:
        print("\nFAIL: regenerating this MetaWorkflowRun does not reproduce it.\n")
        print(difference)
        if drift:
            print("\nMost likely cause: {0}".format(drift))
        if not args.force:
            raise SystemExit(
                "\nNot writing the fixture. Pass --force to capture it anyway (only do "
                "that if you intend to pin a known divergence)."
            )
        print("\n--force given; writing the fixture anyway.")
    else:
        print(
            "\nPASS: regenerated {0} workflow runs identically.".format(
                len(expected["workflow_runs"])
            )
        )

    case = {
        "description": args.description,
        "input_arg": args.input_arg,
        "source": {
            "env": args.env,
            "mwfr_accession": mwfr.get("accession"),
            "mwfr_uuid": mwfr["uuid"],
            "mwfr_date_created": mwfr.get("date_created"),
            "meta_workflow_uuid": metaworkflow["uuid"],
            "meta_workflow_name": metaworkflow.get("name"),
            "meta_workflow_version": metaworkflow.get("version"),
            "ambiguous_input_args": ambiguous,
        },
    }

    case_dir = os.path.join(CASES_DIR, case_name)
    os.makedirs(case_dir, exist_ok=True)
    for name, payload in (
        ("metaworkflow.json", metaworkflow),
        ("case.json", case),
        ("expected_mwfr.json", expected),
    ):
        path = os.path.join(case_dir, name)
        with open(path, "w") as file_handle:
            json.dump(payload, file_handle, indent=2, sort_keys=True)
            file_handle.write("\n")
        print("Wrote {0} ({1:,} bytes)".format(path, os.path.getsize(path)))


if __name__ == "__main__":
    main()
