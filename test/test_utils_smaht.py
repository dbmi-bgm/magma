#################################################################
#   Libraries
#################################################################
import copy
import pytest

from magma_smaht import utils
from magma_smaht.utils import (
    generate_input_structure,
    get_input_structures,
    get_scattered_argument_names,
    mwfr_from_input,
)

#################################################################
#   Vars
#################################################################


def files_(dimensions):
    """Build an input argument file list from the given dimensions."""
    return [
        {"file": f"uuid-{i}", "dimension": dimension}
        for i, dimension in enumerate(dimensions)
    ]


# Input structures for the file lists that the producers in create_metawfr.py
# create. These are unchanged from the implementation that inferred the
# dimensionality of the input structure from the first file alone.
PRODUCER_INPUT = [
    # 1D, single file, e.g. create_metawfr.py::mwfr_bamqc
    (files_(["0"]), [0]),
    # 1D, e.g. create_metawfr.py::get_core_alignment_mwfr_input
    (files_(["0", "1", "2"]), [0, 1, 2]),
    (files_([f"{i}" for i in range(80)]), list(range(80))),
    # 2D, single R1/R2 pair
    (files_(["0,0", "1,0"]), [[0], [0]]),
    # 2D, create_metawfr.py::mwfr_fastqc after sorting by dimension string
    (files_(["0,0", "0,1", "1,0", "1,1", "1,2"]), [[0, 1], [0, 1, 2]]),
    # 2D, create_metawfr.py::mwfr_fastqc before sorting, i.e. the R2 files
    # (dimension 1) are encountered first. The order of the indices within the
    # sublists must be preserved.
    (files_(["1,0", "0,0", "1,1", "0,1", "1,2"]), [[0, 1], [0, 1, 2]]),
]


#################################################################
#   Tests
#################################################################
@pytest.mark.parametrize("files,expected", PRODUCER_INPUT)
def test_generate_input_structure(files, expected):
    """The input structures of the file lists built by the producers."""
    assert generate_input_structure(files) == expected


@pytest.mark.parametrize(
    "files,expected",
    [
        # Dimensions given as int instead of str (old: AttributeError)
        ([{"file": "uuid-0", "dimension": 0}], [0]),
        ([{"file": "uuid-0", "dimension": 0}, {"file": "uuid-1", "dimension": 1}], [0, 1]),
        # Surrounding whitespace (old: ValueError from int() in 2D)
        (files_([" 0 , 0 ", "1,0"]), [[0], [0]]),
        # Single file without a dimension (old: KeyError). This is used by the
        # single file MetaWorkflowRuns on the snv_calling branch.
        ([{"file": "uuid-0"}], [0]),
        ([{"file": "uuid-0", "dimension": None}], [0]),
    ],
)
def test_generate_input_structure_tolerated_input(files, expected):
    """Input that is not built by any producer, but that describes an
    unambiguous structure.
    """
    assert generate_input_structure(files) == expected


def test_generate_input_structure_missing_dimensions_warn(capsys):
    """Several files without a dimension are treated as a positional 1D list,
    with a warning.
    """
    files = [{"file": "uuid-0"}, {"file": "uuid-1"}]
    assert generate_input_structure(files) == [0, 1]
    assert "WARNING" in capsys.readouterr().out


def test_generate_input_structure_empty():
    """An empty file list is reported instead of raising an IndexError."""
    with pytest.raises(ValueError, match="empty list of files"):
        generate_input_structure([])


@pytest.mark.parametrize(
    "dimensions",
    [
        # 1D first file, 2D remaining files. Inferring the dimensionality from
        # the first file alone would flatten the 2D structure to a 1D list.
        ["0", "0,1", "1,0", "1,1"],
        # 2D first file, 1D remaining files
        ["0,0", "3"],
    ],
)
def test_generate_input_structure_mixed_dimensions(dimensions):
    """All files of an input argument must have the same dimensionality."""
    with pytest.raises(ValueError, match="Inconsistent dimensions"):
        generate_input_structure(files_(dimensions))


@pytest.mark.parametrize(
    "dimensions,match",
    [
        # 1D duplicates and gaps
        (["0", "0"], "input files"),
        (["0", "1", "3"], "input files"),
        (["1", "2"], "input files"),
        # 2D gaps and duplicates in the first dimension
        (["0,0", "2,0"], "first dimension"),
        (["1,0", "2,0"], "first dimension"),
        # 2D gaps and duplicates in the second dimension
        (["0,0", "0,2"], "second dimension of index 0"),
        (["0,0", "0,0"], "second dimension of index 0"),
        (["0,0", "1,1"], "second dimension of index 1"),
    ],
)
def test_generate_input_structure_incomplete_dimensions(dimensions, match):
    """Duplicate and gapped dimensions are rejected, they would result in an
    input structure that does not match the given dimensions.
    """
    with pytest.raises(ValueError, match=match):
        generate_input_structure(files_(dimensions))


@pytest.mark.parametrize(
    "dimensions",
    [
        ["a"],
        ["0", "x"],
        ["0,x"],
        ["-1"],
        ["0,-1"],
        ["0,"],
        ["1.0"],
    ],
)
def test_generate_input_structure_invalid_dimensions(dimensions):
    """Dimensions must be comma separated non-negative integers."""
    with pytest.raises(ValueError, match="Invalid dimension component"):
        generate_input_structure(files_(dimensions))


def test_generate_input_structure_empty_dimension():
    """An empty dimension is treated like a missing one."""
    assert generate_input_structure(files_([""])) == [0]
    with pytest.raises(ValueError, match="Inconsistent dimensions"):
        generate_input_structure(files_(["0", ""]))


def test_generate_input_structure_three_dimensions():
    """More than 2 dimensions raise instead of terminating the interpreter."""
    with pytest.raises(ValueError, match="more than 2 dimensions"):
        generate_input_structure(files_(["0,0,0", "0,0,1"]))


@pytest.mark.parametrize("files,expected", PRODUCER_INPUT)
def test_generate_input_structure_does_not_mutate(files, expected):
    """The input files are posted as part of the MetaWorkflowRun and must not
    be modified.
    """
    files_before = copy.deepcopy(files)
    generate_input_structure(files)
    assert files == files_before


#################################################################
#   Vars for the per argument input structures
#################################################################
METAWF = {
    "accession": "MWF1",
    "app_name": "test",
    "app_version": "1",
    "uuid": "mwf-uuid",
    "input": [],
    "workflows": [
        {
            "name": "A",
            "workflow": "wa-uuid",
            "config": {},
            "input": [
                {"argument_name": "INPUT_A", "argument_type": "file", "scatter": 1},
                {"argument_name": "SAMPLE_NAME", "argument_type": "parameter"},
            ],
        },
        {
            "name": "B",
            "workflow": "wb-uuid",
            "config": {},
            "input": [
                # scattered under a different name in the MetaWorkflowRun input
                {
                    "argument_name": "input_b",
                    "source_argument_name": "INPUT_B",
                    "argument_type": "file",
                    "scatter": 1,
                },
            ],
        },
        {
            "name": "C",
            "workflow": "wc-uuid",
            "config": {},
            "input": [
                {
                    "argument_name": "in_c",
                    "argument_type": "file",
                    "source": "A",
                    "source_argument_name": "out_a",
                    "gather": 1,
                },
                {
                    "argument_name": "in_c",
                    "argument_type": "file",
                    "source": "B",
                    "source_argument_name": "out_b",
                    "gather": 1,
                },
                # not scattered, the same reference files for every shard
                {"argument_name": "REFERENCE_FILES", "argument_type": "file"},
            ],
        },
    ],
}


def input_(n_files_a, n_files_b, **kwargs):
    input = [
        {
            "argument_name": "INPUT_A",
            "argument_type": "file",
            "files": files_([f"{i}" for i in range(n_files_a)]),
        },
        {
            "argument_name": "INPUT_B",
            "argument_type": "file",
            "files": files_([f"{i}" for i in range(n_files_b)]),
        },
        {
            "argument_name": "REFERENCE_FILES",
            "argument_type": "file",
            "files": files_(["0", "1"]),
        },
        {"argument_name": "SAMPLE_NAME", "argument_type": "parameter", "value": "S1"},
    ]
    for argument_name, files in kwargs.items():
        input.append(
            {"argument_name": argument_name, "argument_type": "file", "files": files}
        )
    return input


#################################################################
#   Tests for the per argument input structures
#################################################################
def test_get_scattered_argument_names():
    """Only file arguments that are scattered over and matched to the input of
    the MetaWorkflowRun are returned, under their input argument name.
    """
    assert get_scattered_argument_names(METAWF) == ["INPUT_A", "INPUT_B"]


def test_get_input_structures():
    """A structure is calculated for every scattered file argument, and only
    for those.
    """
    assert get_input_structures(METAWF, input_(3, 2)) == {
        "INPUT_A": [0, 1, 2],
        "INPUT_B": [0, 1],
    }


def test_get_input_structures_ignores_invalid_unscattered_argument():
    """A file argument that no workflow scatters over is not validated."""
    input = input_(3, 2)
    # REFERENCE_FILES has duplicate dimensions, but is not scattered over
    input[2]["files"] = files_(["0", "0"])
    assert get_input_structures(METAWF, input) == {
        "INPUT_A": [0, 1, 2],
        "INPUT_B": [0, 1],
    }


def test_get_input_structures_invalid_scattered_argument():
    """An invalid structure of a scattered argument is reported with the name
    of the argument.
    """
    input = input_(3, 2)
    input[1]["files"] = files_(["0", "0,1"])
    with pytest.raises(ValueError, match="scattered input argument INPUT_B"):
        get_input_structures(METAWF, input)


@pytest.mark.parametrize("n_files_b", [1, 2, 5])
def test_mwfr_from_input_two_scatters(monkeypatch, n_files_b):
    """The MetaWorkflowRun scatters each workflow over its own input argument."""
    monkeypatch.setattr(utils, "get_item", lambda *args, **kwargs: copy.deepcopy(METAWF))
    mwfr = mwfr_from_input("mwf-uuid", input_(3, n_files_b), "INPUT_A", {})

    shards = {}
    for workflow_run in mwfr["workflow_runs"]:
        shards.setdefault(workflow_run["name"], []).append(workflow_run["shard"])
    assert sorted(shards["A"]) == ["0", "1", "2"]
    assert sorted(shards["B"]) == [str(i) for i in range(n_files_b)]
    assert shards["C"] == ["0"]

    dependencies = sorted(
        w["dependencies"] for w in mwfr["workflow_runs"] if w["name"] == "C"
    )[0]
    assert sorted(dependencies) == ["A:0", "A:1", "A:2"] + [
        f"B:{i}" for i in range(n_files_b)
    ]


def test_mwfr_from_input_unknown_input_arg(monkeypatch):
    """An input_arg that is not in the input is reported instead of raising a
    NameError on an unbound input structure.
    """
    monkeypatch.setattr(utils, "get_item", lambda *args, **kwargs: copy.deepcopy(METAWF))
    with pytest.raises(ValueError, match="no input argument INPUT_MISSING"):
        mwfr_from_input("mwf-uuid", input_(3, 2), "INPUT_MISSING", {})
