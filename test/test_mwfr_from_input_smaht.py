"""Tests for `magma_smaht.utils.mwfr_from_input` against real portal data.

Each case under `test/files/mwfr_from_input/<name>/` holds a MetaWorkflow item and a
MetaWorkflowRun that was actually posted from it, both pulled from the SMaHT portal by
`test/fetch_mwfr_fixture.py`. The test regenerates the run and checks it comes back with
the same shard graph.

`mwfr_from_input` makes exactly one portal call -- `get_item` -- so mocking that single
seam is enough; nothing here touches the network.

Add a scenario with::

    python test/fetch_mwfr_fixture.py --mwfr <accession> \
        --input-arg <argument_name> --env data
"""

import json
import os
import uuid as uuid_module

import mock
import pytest

import magma_smaht.utils as utils_module
from test.utils import patch_context

CASES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files", "mwfr_from_input")

AUTH_KEY = {"key": "mocked", "secret": "out", "server": "https://example.org"}


def case_names():
    """Every fixture directory holding a case.json, discovered at collection time."""
    if not os.path.isdir(CASES_DIR):
        return []
    return sorted(
        name
        for name in os.listdir(CASES_DIR)
        if os.path.exists(os.path.join(CASES_DIR, name, "case.json"))
    )


def load_case(case_name):
    """Load a case fresh.

    Deliberately not cached at module scope: mwfr_from_input returns the caller's `input`
    list by reference, so a shared copy could be mutated by one test and observed by
    another.
    """
    case_dir = os.path.join(CASES_DIR, case_name)

    def read(name):
        with open(os.path.join(case_dir, name)) as json_file:
            return json.load(json_file)

    return read("case.json"), read("metaworkflow.json"), read("expected_mwfr.json")


def shard_sort_key(workflow_run):
    """Order shards numerically, so '1:2' sorts before '1:10'."""
    return (
        workflow_run["name"],
        tuple(int(part) for part in workflow_run["shard"].split(":")),
    )


def normalize(mwfr):
    """A MetaWorkflowRun reduced to the shard graph, in a comparable order.

    Two things have to be absorbed. `workflow_runs` ordering is nondeterministic across
    processes -- MetaWorkflow._order_run iterates a set of identity-hashed StepWorkflow
    objects, so any graph with more than one entry point comes out differently run to run
    (not fixable with PYTHONHASHSEED). And a posted run has since executed, so its
    entries carry job_id/output/status/workflow_run that a fresh one cannot have.

    `dependencies` is absent rather than empty on entry steps -- magma only creates the
    key when there is something to put in it, and the portal schema requires minItems 1.
    """
    return {
        "meta_workflow": mwfr["meta_workflow"],
        "workflow_runs": [
            {
                "name": workflow_run["name"],
                "shard": workflow_run["shard"],
                "dependencies": sorted(workflow_run.get("dependencies", [])),
            }
            for workflow_run in sorted(mwfr["workflow_runs"], key=shard_sort_key)
        ],
    }


def describe_difference(actual, expected):
    """A readable summary of how two normalized runs differ, or '' if they match.

    Shared with fetch_mwfr_fixture.py so a bad case is explained at capture time.
    """
    if actual == expected:
        return ""

    lines = []
    if actual["meta_workflow"] != expected["meta_workflow"]:
        lines.append(
            "meta_workflow: {0} != {1}".format(
                actual["meta_workflow"], expected["meta_workflow"]
            )
        )

    actual_runs = {(run["name"], run["shard"]): run for run in actual["workflow_runs"]}
    expected_runs = {(run["name"], run["shard"]): run for run in expected["workflow_runs"]}

    for label, missing in (
        ("only generated", sorted(set(actual_runs) - set(expected_runs))),
        ("only expected", sorted(set(expected_runs) - set(actual_runs))),
    ):
        if missing:
            lines.append(
                "{0} ({1}): {2}".format(
                    label,
                    len(missing),
                    ", ".join("{0}:{1}".format(*key) for key in missing),
                )
            )

    for key in sorted(set(actual_runs) & set(expected_runs)):
        if actual_runs[key]["dependencies"] != expected_runs[key]["dependencies"]:
            lines.append(
                "{0}:{1} dependencies: {2} != {3}".format(
                    key[0],
                    key[1],
                    actual_runs[key]["dependencies"],
                    expected_runs[key]["dependencies"],
                )
            )

    return "\n".join(lines)


def generate(metaworkflow, case, mwfr_input):
    with patch_context(
        utils_module, "get_item", return_value=metaworkflow
    ) as mocked_get_item:
        result = utils_module.mwfr_from_input(
            metaworkflow["uuid"], mwfr_input, case["input_arg"], AUTH_KEY
        )
    assert mocked_get_item.call_args_list == [
        mock.call(metaworkflow["uuid"], AUTH_KEY)
    ]
    return result


def test_cases_discovered():
    """An empty fixture directory would parametrize to zero tests and pass silently."""
    assert case_names(), "no cases found under {0}".format(CASES_DIR)


@pytest.mark.parametrize("case_name", case_names())
def test_shard_graph_matches_posted_run(case_name):
    """The regenerated run reproduces the graph of the one actually posted."""
    case, metaworkflow, expected = load_case(case_name)
    actual = generate(metaworkflow, case, expected["input"])

    difference = describe_difference(normalize(actual), normalize(expected))
    assert not difference, "{0} does not reproduce {1}:\n{2}".format(
        case_name, case["source"].get("mwfr_accession"), difference
    )


@pytest.mark.parametrize("case_name", case_names())
def test_generated_run_is_fresh_and_complete(case_name):
    """The fields the shard-graph comparison drops, which are contract rather than graph."""
    case, metaworkflow, expected = load_case(case_name)
    actual = generate(metaworkflow, case, expected["input"])

    # A new field appearing here is a behaviour change worth noticing.
    assert set(actual) == {
        "meta_workflow",
        "workflow_runs",
        "input",
        "final_status",
        "uuid",
        "consortia",
        "submission_centers",
    }

    # Taken from the *fetched item*, not the identifier argument -- they differ whenever
    # a caller passes an accession.
    assert actual["meta_workflow"] == metaworkflow["uuid"]

    assert actual["final_status"] == "pending"
    assert all(run["status"] == "pending" for run in actual["workflow_runs"])
    assert not any(
        set(run) & {"job_id", "output", "workflow_run"} for run in actual["workflow_runs"]
    )

    # Compared against the literals rather than the fixture: these are linkTos, so the
    # raw frame of a posted run holds UUIDs, not these identifiers.
    assert actual["consortia"] == ["smaht"]
    assert actual["submission_centers"] == ["smaht_dac"]

    assert uuid_module.UUID(actual["uuid"]).version == 4


@pytest.mark.parametrize("case_name", case_names())
def test_input_passed_through_unmodified(case_name):
    """`input` is handed back as-is.

    Weak by construction -- mwfr_from_input assigns the caller's list by reference -- so
    this only pins that nothing mutates it along the way.
    """
    case, metaworkflow, expected = load_case(case_name)
    actual = generate(metaworkflow, case, expected["input"])

    _, _, untouched = load_case(case_name)
    assert actual["input"] == untouched["input"]


@pytest.mark.parametrize("case_name", case_names())
def test_uuid_is_unique_per_call(case_name):
    case, metaworkflow, expected = load_case(case_name)
    first = generate(metaworkflow, case, expected["input"])
    second = generate(metaworkflow, case, expected["input"])
    assert first["uuid"] != second["uuid"]


def load_synthetic_metaworkflow():
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "files", "test_METAWFL_smaht.json"
        )
    ) as json_file:
        return json.load(json_file)


class TestInputStructure:
    """`generate_input_structure` decides the whole scatter shape from one argument."""

    @pytest.mark.parametrize(
        "dimensions,expected",
        [
            pytest.param(["0"], [0], id="single_file"),
            pytest.param(["0", "1", "2"], [0, 1, 2], id="one_dimension"),
            pytest.param([None], [0], id="dimension_omitted"),
            pytest.param([None, None], [0, 1], id="dimension_omitted_multiple"),
            pytest.param(
                ["0,0", "0,1", "1,0", "1,1", "1,2"],
                [[0, 1], [0, 1, 2]],
                id="two_dimensions_ragged",
            ),
            pytest.param(
                ["1,0", "0,0", "1,1", "0,1", "1,2"],
                [[0, 1], [0, 1, 2]],
                id="two_dimensions_unsorted",
            ),
        ],
    )
    def test_structure(self, dimensions, expected):
        files = [
            {"file": "f{0}".format(index)}
            if dimension is None
            else {"file": "f{0}".format(index), "dimension": dimension}
            for index, dimension in enumerate(dimensions)
        ]
        assert utils_module.generate_input_structure(files) == expected

    def test_three_dimensions_exits(self):
        """Not supported -- the current code prints and calls exit()."""
        files = [{"file": "f1", "dimension": "0,0,0"}]
        with pytest.raises(SystemExit):
            utils_module.generate_input_structure(files)

    def test_only_the_first_file_decides_the_shape(self):
        """A documented assumption: files[0]'s dimension is taken as representative."""
        files = [
            {"file": "f1", "dimension": "0"},
            {"file": "f2", "dimension": "1,0"},
        ]
        assert utils_module.generate_input_structure(files) == [0, 1]


def test_get_item_requests_the_raw_frame():
    """mwfr_from_input needs the raw frame; an embedded MetaWorkflow would not parse."""
    metaworkflow = load_synthetic_metaworkflow()
    with patch_context(
        utils_module.ff_utils, "get_metadata", return_value=metaworkflow
    ) as mocked:
        utils_module.mwfr_from_input(
            metaworkflow["uuid"],
            [
                {
                    "argument_name": "a_file",
                    "argument_type": "file",
                    "files": [{"file": "f1", "dimension": "0"}],
                }
            ],
            "a_file",
            AUTH_KEY,
        )
    assert mocked.call_args == mock.call(
        metaworkflow["uuid"], add_on="frame=raw&datastore=database", key=AUTH_KEY
    )
