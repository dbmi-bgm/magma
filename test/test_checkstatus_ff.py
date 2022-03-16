import mock
import pytest
import json
from magma_ff import checkstatus
#from magma import metawflrun as run
from magma_ff import metawflrun as run_ff


# tests requiring connection are marked 'portaltest'.
# to perform tests without connection, use pytest -v -m "not portaltest" test_utils.py


def test_CheckStatusFF():
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
    cs = checkstatus.CheckStatusFF(wflrun_obj)
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


def test_CheckStatusFF_failed():
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
    cs = checkstatus.CheckStatusFF(wflrun_obj)
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


def test_CheckStatusFF_running():
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
    cs = checkstatus.CheckStatusFF(wflrun_obj)
    cr = cs.check_running()

    # mock get_status and get_output
    with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_status', return_value='started'):
        with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_output',
                        return_value=[{'argument_name': 'raw_bam', 'files': 'abc'}]):
            with mock.patch('magma_ff.checkstatus.CheckStatusFF.get_uuid', return_value='run_uuid'):
                res = next(cr)

    # check yielded result
    assert res is None
