import mock
import pytest
from typing import Any, Dict

from magma_ff.status_metawfr import (
    evaluate_quality_metrics,
    evaluate_workflow_run_quality_metrics,
    get_recently_completed_workflow_runs,
    is_final_status_completed,
)


META_WORKFLOW_RUN_COMPLETED = {"final_status": "completed"}
META_WORKFLOW_RUN_RUNNING = {"final_status": "running"}
WORKFLOW_RUN_UUID = "some_uuid"
WORKFLOW_RUN_COMPLETED = {"run_status": "completed", "workflow_run": WORKFLOW_RUN_UUID}
WORKFLOW_RUN_RUNNING = {"run_status": "running", "workflow_run": WORKFLOW_RUN_UUID}
WORKFLOW_RUN_ERROR = {"run_status": "failed", "workflow_run": WORKFLOW_RUN_UUID}
WORKFLOW_RUN_PASSING = {
    "output_files": [
        {"no_qc_here": "foo"},
        {"value_qc": {"overall_quality_status": "PASS"}},
    ]
}
WORKFLOW_RUN_WARNING = {
    "output_files": [
        {"no_qc_here": "foo"},
        {"value_qc": {"overall_quality_status": "PASS"}},
        {"value_qc": {"overall_quality_status": "WARN"}},
    ]
}
WORKFLOW_RUN_FAILING = {
    "output_files": [
        {"no_qc_here": "foo"},
        {"value_qc": {"overall_quality_status": "PASS"}},
        {"value_qc": {"overall_quality_status": "FAIL"}},
    ]
}


def make_meta_workflow_run(workflow_runs):
    """Create dict with specified WorkflowRuns."""
    return {"workflow_runs": workflow_runs}


@pytest.mark.parametrize(
    "meta_workflow_run,updated_properties,error,expected",
    [
        ({}, {}, False, []),
        (
            make_meta_workflow_run([WORKFLOW_RUN_COMPLETED]),
            {},
            True,
            None,
        ),
        (
            {},
            make_meta_workflow_run([WORKFLOW_RUN_COMPLETED]),
            True,
            None,
        ),
        (
            make_meta_workflow_run([WORKFLOW_RUN_COMPLETED]),
            make_meta_workflow_run([WORKFLOW_RUN_COMPLETED]),
            False,
            [],
        ),
        (
            make_meta_workflow_run([WORKFLOW_RUN_RUNNING]),
            make_meta_workflow_run([WORKFLOW_RUN_COMPLETED]),
            False,
            [WORKFLOW_RUN_UUID],
        ),
        (
            make_meta_workflow_run([WORKFLOW_RUN_COMPLETED]),
            make_meta_workflow_run([WORKFLOW_RUN_RUNNING]),
            False,
            [],
        ),
    ],
)
def test_get_recently_completed_workflow_runs(
    meta_workflow_run, updated_properties, error, expected
):
    """Test retrieval of newly complete WorkflowRuns from updated PATCH
    body.
    """
    if error:
        with pytest.raises(ValueError):
            get_recently_completed_workflow_runs(meta_workflow_run, updated_properties)
    else:
        result = get_recently_completed_workflow_runs(
            meta_workflow_run, updated_properties
        )
        assert result == expected


@pytest.mark.parametrize(
    "embed_response,expected",
    [
        ([], False),
        ([WORKFLOW_RUN_PASSING], False),
        ([WORKFLOW_RUN_PASSING, WORKFLOW_RUN_WARNING], False),
        ([WORKFLOW_RUN_PASSING, WORKFLOW_RUN_FAILING], True),
    ],
)
def test_evaluate_quality_metrics(embed_response, expected):
    """Test evaluating whether any WorkflowRuns' output QualityMetrics
    have failed.
    """
    with mock.patch(
        "magma_ff.status_metawfr.make_embed_request", return_value=embed_response
    ) as mocked_request:
        result = evaluate_quality_metrics("mocked", "mocked")
        mocked_request.assert_called_once()
        assert result == expected


@pytest.mark.parametrize(
    "workflow_run,expected",
    [
        ({}, False),
        ({"foo": "bar"}, False),
        (WORKFLOW_RUN_PASSING, False),
        (WORKFLOW_RUN_WARNING, False),
        (WORKFLOW_RUN_FAILING, True),
    ],
)
def test_evaluate_workflow_run_quality_metrics(workflow_run, expected):
    """Test evaluating whether any of a given WorkflowRun's output
    QualityMetrics have failed.
    """
    result = evaluate_workflow_run_quality_metrics(workflow_run)
    assert result == expected


@pytest.mark.parametrize(
    "meta_workflow_run,expected",
    [
        ({}, False),
        (META_WORKFLOW_RUN_COMPLETED, True),
        (META_WORKFLOW_RUN_RUNNING, False),
    ]
)
def test_is_final_status_completed(
    meta_workflow_run: Dict[str, Any], expected: bool
) -> None:
    result = is_final_status_completed(meta_workflow_run)
    assert result == expected
