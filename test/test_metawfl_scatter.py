#################################################################
#   Libraries
#################################################################
import copy
import json
import pytest

from magma import metawfl as wfl

#################################################################
#   Vars
#################################################################


def arg_(name, **kwargs):
    arg = {"argument_name": name, "argument_type": "file"}
    arg.update(kwargs)
    return arg


def step_(name, input, **kwargs):
    step = {"name": name, "workflow": f"{name}-uuid", "config": {}, "input": input}
    step.update(kwargs)
    return step


def metawf_(*steps):
    return {
        "accession": "MWF1",
        "app_name": "test",
        "app_version": "1",
        "uuid": "mwf-uuid",
        "input": [],
        "workflows": list(steps),
    }


# A scatters over INPUT_A, B over INPUT_B, C gathers from both.
# A2 and B2 inherit the scatter structure of their producer.
MWF_TWO_SCATTERS = metawf_(
    step_("A", [arg_("INPUT_A", scatter=1)]),
    step_("B", [arg_("INPUT_B", scatter=1)]),
    step_("A2", [arg_("in", source="A", source_argument_name="out_a")]),
    step_("B2", [arg_("in", source="B", source_argument_name="out_b")]),
    step_(
        "C",
        [
            arg_("in_c", source="A2", source_argument_name="out_a2", gather=1),
            arg_("in_c", source="B2", source_argument_name="out_b2", gather=1),
        ],
    ),
)


def shards_(metawf, input_structure, **kwargs):
    """Map step name to its sorted list of shards."""
    run = wfl.MetaWorkflow(copy.deepcopy(metawf)).write_run(input_structure, **kwargs)
    shards = {}
    for workflow_run in run["workflow_runs"]:
        shards.setdefault(workflow_run["name"], []).append(workflow_run["shard"])
    return {name: sorted(s) for name, s in shards.items()}


def dependencies_(metawf, input_structure, name, shard, **kwargs):
    run = wfl.MetaWorkflow(copy.deepcopy(metawf)).write_run(input_structure, **kwargs)
    for workflow_run in run["workflow_runs"]:
        if workflow_run["name"] == name and workflow_run["shard"] == shard:
            return sorted(workflow_run.get("dependencies", []))
    raise AssertionError(f"no workflow run {name}:{shard}")


#################################################################
#   Tests
#################################################################
def test_write_run_input_structures_is_optional():
    """Passing no input_structures is the same as passing structures that all
    match the input structure with maximum scatter.
    """
    structure = [0, 1, 2]
    metawf = metawf_(
        step_("A", [arg_("INPUT_A", scatter=1)]),
        step_("B", [arg_("INPUT_B", scatter=1)]),
        step_("C", [arg_("in_c", source="A", source_argument_name="out_a", gather=1)]),
    )
    without = wfl.MetaWorkflow(copy.deepcopy(metawf)).write_run(structure)
    with_ = wfl.MetaWorkflow(copy.deepcopy(metawf)).write_run(
        structure, input_structures={"INPUT_A": [0, 1, 2], "INPUT_B": [0, 1, 2]}
    )
    assert sorted(
        (w["name"], w["shard"], tuple(sorted(w.get("dependencies", []))))
        for w in without["workflow_runs"]
    ) == sorted(
        (w["name"], w["shard"], tuple(sorted(w.get("dependencies", []))))
        for w in with_["workflow_runs"]
    )


@pytest.mark.parametrize("n_files_b", [1, 2, 5])
def test_write_run_two_independent_scatters(n_files_b):
    """A scatters over its 3 files, B over its own files, and the shared
    downstream step C gathers from both.
    """
    structures = {"INPUT_A": [0, 1, 2], "INPUT_B": list(range(n_files_b))}
    shards = shards_(MWF_TWO_SCATTERS, [0, 1, 2], input_structures=structures)

    assert shards["A"] == ["0", "1", "2"]
    assert shards["B"] == [str(i) for i in range(n_files_b)]
    # A2 and B2 inherit the shards of their producer
    assert shards["A2"] == ["0", "1", "2"]
    assert shards["B2"] == [str(i) for i in range(n_files_b)]
    assert shards["C"] == ["0"]

    assert dependencies_(
        MWF_TWO_SCATTERS, [0, 1, 2], "C", "0", input_structures=structures
    ) == ["A2:0", "A2:1", "A2:2"] + [f"B2:{i}" for i in range(n_files_b)]


def test_write_run_scatter_argument_2d():
    """The input structure of a scattered argument can have 2 dimensions
    while another argument is scattered on 1 dimension.
    """
    metawf = metawf_(
        step_("A", [arg_("INPUT_A", scatter=2)]),
        step_("B", [arg_("INPUT_B", scatter=1)]),
    )
    shards = shards_(
        metawf,
        [[0, 1], [0, 1, 2]],
        input_structures={"INPUT_A": [[0, 1], [0, 1, 2]], "INPUT_B": [0, 1, 2, 3]},
    )
    assert shards["A"] == ["0:0", "0:1", "1:0", "1:1", "1:2"]
    assert shards["B"] == ["0", "1", "2", "3"]


def test_write_run_fixed_shards_take_precedence():
    """A step with fixed shards keeps them, the input structure of the argument
    it scatters over is not used.
    """
    metawf = metawf_(
        step_("A", [arg_("INPUT_A", scatter=1)]),
        step_("B", [arg_("INPUT_B", scatter=1)], shards=[["0"], ["1"]]),
    )
    shards = shards_(
        metawf,
        [0, 1, 2],
        input_structures={"INPUT_A": [0, 1, 2], "INPUT_B": [0, 1, 2, 3, 4]},
    )
    assert shards["A"] == ["0", "1", "2"]
    assert shards["B"] == ["0", "1"]


@pytest.mark.parametrize(
    "input,structures",
    [
        # Scatter over a parameter, no input structure to use
        ([{"argument_name": "PARAM", "argument_type": "parameter", "scatter": 1}], {}),
        # Scatter over the output of a previous step
        (None, {"INPUT_B": [0, 1]}),
        # The scattered argument is not in input_structures
        ([{"argument_name": "INPUT_B", "argument_type": "file", "scatter": 1}], {"OTHER": [0, 1]}),
    ],
)
def test_write_run_falls_back_to_input_structure(input, structures):
    """Steps that don't scatter over an input argument with its own structure
    use the input structure with maximum scatter.
    """
    if input is None:
        input = [
            arg_("in_b", source="A", source_argument_name="out_a", gather=1, scatter=1)
        ]
    metawf = metawf_(
        step_("A", [arg_("INPUT_A", scatter=1)]),
        step_("B", input),
    )
    structures = dict(structures)
    structures.setdefault("INPUT_A", [0, 1, 2])
    shards = shards_(metawf, [0, 1, 2], input_structures=structures)
    assert shards["A"] == ["0", "1", "2"]
    assert shards["B"] == ["0", "1", "2"]


def test_write_run_error_scatter_over_conflicting_arguments():
    """A step cannot scatter over two arguments with different structures."""
    metawf = metawf_(
        step_("A", [arg_("INPUT_A", scatter=1), arg_("INPUT_B", scatter=1)]),
    )
    with pytest.raises(ValueError, match="scatters over arguments with different"):
        shards_(
            metawf,
            [0, 1, 2],
            input_structures={"INPUT_A": [0, 1, 2], "INPUT_B": [0, 1]},
        )


def test_write_run_error_shared_consumer_without_gather():
    """A shared downstream step that does not gather cannot combine steps
    that are scattered differently.
    """
    metawf = metawf_(
        step_("A", [arg_("INPUT_A", scatter=1)]),
        step_("B", [arg_("INPUT_B", scatter=1)]),
        step_(
            "C",
            [
                arg_("in_c", source="A", source_argument_name="out_a"),
                arg_("in_c", source="B", source_argument_name="out_b"),
            ],
        ),
    )
    with pytest.raises(ValueError, match="depends on steps with different shards"):
        shards_(
            metawf,
            [0, 1, 2],
            input_structures={"INPUT_A": [0, 1, 2], "INPUT_B": [0, 1]},
        )


def test_write_run_error_scattered_consumer_of_differently_scattered_step():
    """A step that is scattered over its own argument cannot depend on a step
    with a different number of shards without gathering from it.
    """
    metawf = metawf_(
        step_("A", [arg_("INPUT_A", scatter=1)]),
        step_(
            "B",
            [
                arg_("INPUT_B", scatter=1),
                arg_("in_b", source="A", source_argument_name="out_a"),
            ],
        ),
    )
    with pytest.raises(ValueError, match="has no matching shard"):
        shards_(
            metawf,
            [0, 1, 2],
            input_structures={"INPUT_A": [0, 1, 2], "INPUT_B": [0, 1, 2, 3]},
        )


def test_write_run_error_partial_gather_from_differently_scattered_step():
    """The shards of a partial gather are calculated from the input structure
    with maximum scatter, which does not match a producer that is scattered on
    a structure of its own.
    """
    metawf = metawf_(
        step_("B", [arg_("INPUT_B", scatter=2)]),
        step_("C", [arg_("in_c", source="B", source_argument_name="out_b", gather=1)]),
    )
    with pytest.raises(ValueError, match="don't match the shards"):
        shards_(
            metawf,
            [[0, 1], [0, 1]],
            input_structures={"INPUT_B": [[0, 1], [0, 1], [0]]},
        )


def test_write_run_3d_fixture_unchanged():
    """The 3D fixture combines a partial gather from a 3 dimensional step with
    a full gather from a 1 dimensional step, which must keep working.
    """
    with open("test/files/test_METAWFL_3D.json") as json_file:
        data = json.load(json_file)
    input_structure = [[["1", "2"], ["3"]], [["4", "5"]]]
    expected = shards_(data, input_structure)
    # Passing the same structure per argument changes nothing
    assert (
        shards_(data, input_structure, input_structures={"input_a": input_structure})
        == expected
    )
