from contextlib import contextmanager
from test.utils import patch_context
from typing import Iterator, List, Any, Optional

import mock
import pytest


from magma_ff.utils import JsonObject

import magma_ff.run_metawflrun_handler as run_metaworkflow_run_handler_module
from magma_ff.run_metawflrun_handler import (
    ExecuteMetaWorkflowRunHandler,
    execute_metawflrun_handler,
)

from magma_ff.create_metawfr import (
    MetaWorkflowRunCreationError,
)

from test.meta_workflow_run_handler_constants import *


META_WORKFLOW_RUN_HANDLER_UUID = "meta_workflow_run_handler_tester_uuid"
AUTH_KEY = {"server": "some_server"}


@contextmanager
def patch_patch_metadata(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch ff_utils.patch_metadata call within execute_metawflrun_handler function."""
    with patch_context(
        run_metaworkflow_run_handler_module.ff_utils, "patch_metadata", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_check_status(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch utils.check_status call within execute_metawflrun_handler function."""
    with patch_context(
        run_metaworkflow_run_handler_module, "check_status", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_make_embed_request(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch utils.make_embed_request call within make_embed_request function."""
    with patch_context(
        run_metaworkflow_run_handler_module, "make_embed_request", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_create_meta_workflow_run(**kwargs) -> Iterator[mock.MagicMock]:
    """
    Patch magma_ff.create_metawfr.create_meta_workflow_run call
    within ExecuteMetaWorkflowRunHandler class.
    """
    with patch_context(
        run_metaworkflow_run_handler_module, "create_meta_workflow_run", **kwargs
    ) as mock_item:
        yield mock_item


class TestExecuteMetaWorkflowRunHandler:
    """Tests for methods/properties for ExecuteMetaWorkflowRunHandler class."""

    @pytest.mark.parametrize(
        "run_handler, pending_step_name, expected_result",
        [
            (HANDLER_PENDING, "B", True),
            (HANDLER_PENDING, "A", False),
            (HANDLER_STEPS_RUNNING, "A", False),
            (HANDLER_STEPS_RUNNING, "D", False),
            (HANDLER_STEPS_RUNNING_2, "A", False),
            (HANDLER_FAILED, "D", False),
            (HANDLER_STOPPED, "D", False),
        ],
    )
    def test_check_pending_step_dependencies(
        self, run_handler: JsonObject, pending_step_name: str, expected_result: bool
    ) -> None:
        """
        Tests the check of a mwfr step's dependencies,
        and whether they are completed or not (checking status).
        """
        execution_generator = ExecuteMetaWorkflowRunHandler(run_handler, AUTH_KEY)
        result = execution_generator._check_pending_step_dependencies(pending_step_name)
        assert result == expected_result

    @pytest.mark.parametrize(
        "run_handler, pending_step_name, exception_expected",
        [(HANDLER_PENDING, "B", False), (HANDLER_PENDING, "B", True)],
    )
    def test_create_and_update_meta_workflow_run_step(
        self, run_handler: JsonObject, pending_step_name: str, exception_expected: bool
    ) -> None:
        """Tests creation (and updates) of new metaworkflow run steps"""
        with patch_create_meta_workflow_run() as mock_create_mwfr:
            execution_generator = ExecuteMetaWorkflowRunHandler(run_handler, AUTH_KEY)
            if not exception_expected:
                mock_create_mwfr.return_value = TEST_MWFR_SIMPLE_GET_OUTPUT
                execution_generator._create_and_update_meta_workflow_run_step(
                    pending_step_name
                )
                assert (
                    execution_generator.handler.get_meta_workflow_run_step_attr(
                        pending_step_name, META_WORKFLOW_RUN
                    )
                    == TEST_MWFR_SIMPLE_GET_OUTPUT[UUID]
                )
                assert (
                    execution_generator.handler.get_meta_workflow_run_step_attr(
                        pending_step_name, STATUS
                    )
                    == RUNNING
                )
            else:
                mock_create_mwfr.side_effect = MetaWorkflowRunCreationError("oops")
                execution_generator._create_and_update_meta_workflow_run_step(
                    pending_step_name
                )
                assert (
                    execution_generator.handler.get_meta_workflow_run_step_attr(
                        pending_step_name, ERROR
                    )
                    == "oops"
                )
                assert (
                    execution_generator.handler.get_meta_workflow_run_step_attr(
                        pending_step_name, STATUS
                    )
                    == FAILED
                )

    @pytest.mark.parametrize(
        "run_handler, orig_final_status, yielded_statuses, yielded_mwf_run_arrays",
        [
            (
                HANDLER_PENDING,
                PENDING,
                [RUNNING, RUNNING, RUNNING, RUNNING],
                [
                    FIRST_STEP_RUNNING_ARRAY,
                    RUNNING_MWFR_ARRAY,
                    RUNNING_MWFR_ARRAY,
                    RUNNING_MWFR_ARRAY,
                ],
            ),
            (
                HANDLER_STEPS_RUNNING,
                RUNNING,
                [RUNNING, RUNNING],
                [RUNNING_MWFR_ARRAY, RUNNING_MWFR_ARRAY],
            ),
            (
                HANDLER_STEPS_RUNNING_2,
                RUNNING,
                [RUNNING, RUNNING],
                [RUNNING_MWFR_ARRAY, RUNNING_MWFR_ARRAY],
            ),
            (HANDLER_FAILED, FAILED, [FAILED], [HALFWAY_DONE_N_FAIL_ARRAY]),
            (HANDLER_FAILED_2, FAILED, [], []),
            (HANDLER_STOPPED, STOPPED, [STOPPED], [HALFWAY_DONE_N_STOPPED_ARRAY]),
            (HANDLER_COMPLETED, COMPLETED, [], []),
        ],
    )
    def test_generator_of_created_meta_workflow_run_steps(
        self,
        run_handler: JsonObject,
        orig_final_status: str,
        yielded_statuses: List[str],
        yielded_mwf_run_arrays: List[List[Any]],
    ) -> None:
        """
        Tests generator of dictionaries used to PATCH created MetaWorkflow Runs
        and the final status of the overall MetaWorkflow Run Handler.
        """
        with patch_create_meta_workflow_run(return_value=TEST_MWFR_SIMPLE_GET_OUTPUT):
            execution_generator = ExecuteMetaWorkflowRunHandler(run_handler, AUTH_KEY)
            assert (
                getattr(execution_generator.handler, FINAL_STATUS) == orig_final_status
            )
            patch_dict_generator = (
                execution_generator.generator_of_created_meta_workflow_run_steps()
            )
            assert len(yielded_statuses) == len(list(patch_dict_generator))
            for idx, step in enumerate(patch_dict_generator):
                assert step[FINAL_STATUS] == yielded_statuses[idx]
                assert step[META_WORKFLOW_RUNS] == yielded_mwf_run_arrays[idx]


@pytest.mark.parametrize(
    "run_handler_json, value_err_expected, status_valid, patch_metadata_calls",
    [
        (None, True, True, 0),
        (HANDLER_PENDING_COPY, False, False, 0),
        (HANDLER_PENDING_COPY, False, True, 4),
    ],
)
def test_execute_metawflrun_handler(
    run_handler_json: Optional[JsonObject],
    value_err_expected: bool,
    status_valid: bool,
    patch_metadata_calls: int,
) -> None:
    """
    Tests wrapper function of generator of dictionaries used to PATCH
    the Run Handler final status and created MetaWorkflow Runs.
    Includes additional CGAP portal status checks.
    """
    with patch_make_embed_request() as mock_embed_request:
        with patch_check_status() as mock_check_status:
            with patch_patch_metadata() as mock_patch_metadata:
                with patch_create_meta_workflow_run(
                    return_value=TEST_MWFR_SIMPLE_GET_OUTPUT
                ):
                    if value_err_expected:
                        mock_embed_request.return_value = None
                        with pytest.raises(ValueError) as val_err:
                            execute_metawflrun_handler(TESTER_UUID, AUTH_KEY)
                            assert TESTER_UUID in val_err
                            assert (
                                mock_patch_metadata.call_count == patch_metadata_calls
                            )
                    else:
                        mock_embed_request.return_value = run_handler_json
                        if not status_valid:
                            mock_check_status.return_value = False
                        else:
                            mock_check_status.return_value = True
                        execute_metawflrun_handler(TESTER_UUID, AUTH_KEY)
                        assert mock_patch_metadata.call_count == patch_metadata_calls
