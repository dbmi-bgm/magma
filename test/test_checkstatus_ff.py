from contextlib import contextmanager
from test.utils import patch_context
from typing import Iterator, List, Any

import json
import mock
import pytest

import magma_ff.checkstatus as checkstatus_module
from magma_ff.checkstatus import CheckStatusFF, CheckStatusRunHandlerFF
from magma_ff import metawflrun as run_ff

from magma_ff.utils import JsonObject

from test.meta_workflow_run_handler_constants import *

class TestCheckStatusFF:
    def test_CheckStatusFF(self):
        """This check does not actually connect to the portal.
        It uses mocks for get_status and get_output
        """
        with open('test/files/CGAP_WGS_trio_scatter_ff.run.json') as json_file:
            data_wflrun = json.load(json_file)

        # fake that the first one is running
        data_wflrun['workflow_runs'][0]['status'] = 'running'
        data_wflrun['workflow_runs'][0]['jobid'] = 'somejobid'

        # Create MetaWorkflowRun object and check_running generator
        wflrun_obj = run_ff.MetaWorkflowRun(data_wflrun)
        cs = CheckStatusFF(wflrun_obj)
        cr = cs.check_running()

        # mock get_status and get_output
        with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_status', return_value='complete'):
            with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_output',
                            return_value=[{'argument_name': 'raw_bam', 'files': 'abc'}]):
                with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_uuid', return_value='run_uuid'):
                    res = next(cr)

        # check yielded result
        assert len(res['workflow_runs']) == len(data_wflrun['workflow_runs'])  # same as original
        assert res['workflow_runs'][0] == {'name': 'workflow_bwa-mem_no_unzip-check',
                        'workflow_run': 'run_uuid',
                        'shard': '0:0',
                        'jobid': 'somejobid',
                        'status': 'completed',  # changed from running to completed
                        'output': [{'argument_name': 'raw_bam', 'files': 'abc'}]}  # output is filled in
        assert 'failed_jobs' not in res # if nothing failed, '' failed_jobs should not be in the patch dict


    def test_CheckStatusFF_failed(self):
        """This check does not actually connect to the portal.
        It uses mocks for get_status and get_output
        """
        with open('test/files/CGAP_WGS_trio_scatter_ff.run.json') as json_file:
            data_wflrun = json.load(json_file)

        # fake that the first one is running
        data_wflrun['workflow_runs'][0]['status'] = 'running'
        data_wflrun['workflow_runs'][0]['jobid'] = 'somejobid'

        # Create MetaWorkflowRun object and check_running generator
        wflrun_obj = run_ff.MetaWorkflowRun(data_wflrun)
        cs = CheckStatusFF(wflrun_obj)
        cr = cs.check_running()

        # mock get_status and get_output
        with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_status', return_value='error'):
            with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_output',
                            return_value=[{'argument_name': 'raw_bam', 'files': 'abc'}]):
                with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_uuid', return_value='run_uuid'):
                    res = next(cr)

        # check yielded result
        assert len(res['workflow_runs']) == len(data_wflrun['workflow_runs'])  # same as original
        assert res['workflow_runs'][0] == {'name': 'workflow_bwa-mem_no_unzip-check',
                        'workflow_run': 'run_uuid',
                        'shard': '0:0',
                        'jobid': 'somejobid',
                        'status': 'failed'}  # changed from running to failed, no output
        assert res['failed_jobs'] == ['somejobid']


    def test_CheckStatusFF_running(self):
        """This check does not actually connect to the portal.
        It uses mocks for get_status and get_output
        """
        with open('test/files/CGAP_WGS_trio_scatter_ff.run.json') as json_file:
            data_wflrun = json.load(json_file)
        # fake that the first one is running
        data_wflrun['workflow_runs'][0]['status'] = 'running'
        data_wflrun['workflow_runs'][0]['jobid'] = 'somejobid'
        # Create MetaWorkflowRun object and check_running generator
        wflrun_obj = run_ff.MetaWorkflowRun(data_wflrun)
        cs = CheckStatusFF(wflrun_obj)
        cr = cs.check_running()
        # Mock WorkflowRun with "started" status
        with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_status', return_value='started'):
            with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_output',
                            return_value=[{'argument_name': 'raw_bam', 'files': 'abc'}]):
                with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_uuid', return_value='run_uuid'):
                    result = list(cr)
        assert result == []

        cr = cs.check_running()
        # Mock WorkflowRun with "complete" status
        with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_status', return_value='complete'):
            with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_output',
                            return_value=[{'argument_name': 'raw_bam', 'files': 'abc'}]):
                with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_uuid', return_value='run_uuid'):
                    result = list(cr)
        assert len(result) == 1

##################################################################
AUTH_KEY = {"server": "some_server"}

@contextmanager
def patch_get_meta_workflow_run_status(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch _meta_workflow_runs_cache property within FFMetaWfrUtils class."""
    with patch_context(
        checkstatus_module.FFMetaWfrUtils,
        "get_meta_workflow_run_status",
        # new_callable=mock.PropertyMock,
        **kwargs
    ) as mock_item:
        yield mock_item

class TestCheckStatusRunHandlerFF:
    """Testing for customized CheckStatus class for MetaWorkflow Run Handler (CGAP portal)."""

    @pytest.mark.parametrize(
        "portal_run_status, expected_value",
        [
            (PENDING, PENDING),
            (RUNNING, RUNNING),
            (COMPLETED, COMPLETED),
            (FAILED, FAILED),
            (INACTIVE, PENDING),
            (STOPPED, STOPPED),
            (QC_FAIL, FAILED),
        ],
    )
    def test_get_meta_workflow_run_step_status(
        self, portal_run_status: str, expected_value: str
    ) -> None:
        """
        Tests retrieval of MetaWorkflow Run status from portal, and status mapping to magma.
        """
        with patch_get_meta_workflow_run_status() as mock_get_status:
            mock_get_status.return_value = portal_run_status
            returned_step_status = CheckStatusRunHandlerFF(HANDLER_PENDING, AUTH_KEY).get_meta_workflow_run_step_status("tester")
            assert returned_step_status == expected_value

    @pytest.mark.parametrize(
        "run_handler, orig_final_status, yielded_statuses, yielded_mwf_run_arrays",
        [
            (
                HANDLER_STEPS_RUNNING,
                RUNNING,
                [COMPLETED],
                [FIRST_STEP_COMPLETED_ARRAY],
            )
        ],
    )
    def test_update_running_steps(
        self,
        run_handler: JsonObject,
        orig_final_status: str,
        yielded_statuses: List[str],
        yielded_mwf_run_arrays: List[List[Any]],
    ) -> None:
        """
        Tests generator of dictionaries used to PATCH running MetaWorkflow Runs
        and the final status of the overall MetaWorkflow Run Handler.
        """
        status_checker = CheckStatusRunHandlerFF(run_handler, AUTH_KEY)
        assert (
            getattr(status_checker.handler, FINAL_STATUS) == orig_final_status
        )

        with patch_get_meta_workflow_run_status() as mock_get_status:
            mock_get_status.side_effect = yielded_statuses
            patch_dict_generator = (
                status_checker.update_running_steps()
            )
        import pdb; pdb.set_trace()
        assert len(yielded_statuses) == len(list(patch_dict_generator))
        for idx, step in enumerate(patch_dict_generator):
            assert step[FINAL_STATUS] == yielded_statuses[idx]
            assert step[META_WORKFLOW_RUNS] == yielded_mwf_run_arrays[idx]
