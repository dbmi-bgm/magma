import mock
import pytest
import json
from magma import check_status_ff
from magma import run


# tests requiring connection are marked 'portaltest'.
# to perform tests without connection, use pytest -v -m "not portaltest" test_utils.py


def test_CheckStatusFF():
    """This check does not actually connect to the portal.
    It uses mocks for get_status and get_output
    """
    with open('test/files/CGAP_WGS_trio_scatter.run.json') as json_file:
        data_wflrun = json.load(json_file)

    # fake that the first one is running
    data_wflrun['workflow_runs'][0]['status'] = 'running'
    data_wflrun['workflow_runs'][0]['jobid'] = 'somejobid'

    # Create MetaWorkflowRun object and check_running generator
    wflrun_obj = run.MetaWorkflowRun(data_wflrun)
    cs = check_status_ff.CheckStatusFF(wflrun_obj)
    cr = cs.check_running()

    # mock get_status and get_output
    with mock.patch('magma.check_status_ff.CheckStatusFF.get_status', return_value='complete'):
        with mock.patch('magma.check_status_ff.CheckStatusFF.get_output',
                        return_value=[{'argument_name': 'raw_bam', 'files': 'abc'}]):
            res = next(cr)

    # check yielded result
    assert len(res['workflow_runs']) == len(data_wflrun['workflow_runs'])  # same as original
    assert res['workflow_runs'][0] == {'name': 'workflow_bwa-mem_no_unzip-check',
                      'shard': '0:0',
                      'jobid': 'somejobid',
                      'status': 'completed',  # changed from running to completed
                      'output': [{'argument_name': 'raw_bam', 'files': 'abc'}]}  # output is filled in


def test_CheckStatusFF_failed():
    """This check does not actually connect to the portal.
    It uses mocks for get_status and get_output
    """
    with open('test/files/CGAP_WGS_trio_scatter.run.json') as json_file:
        data_wflrun = json.load(json_file)

    # fake that the first one is running
    data_wflrun['workflow_runs'][0]['status'] = 'running'
    data_wflrun['workflow_runs'][0]['jobid'] = 'somejobid'

    # Create MetaWorkflowRun object and check_running generator
    wflrun_obj = run.MetaWorkflowRun(data_wflrun)
    cs = check_status_ff.CheckStatusFF(wflrun_obj)
    cr = cs.check_running()

    # mock get_status and get_output
    with mock.patch('magma.check_status_ff.CheckStatusFF.get_status', return_value='error'):
        with mock.patch('magma.check_status_ff.CheckStatusFF.get_output',
                        return_value=[{'argument_name': 'raw_bam', 'files': 'abc'}]):
            res = next(cr)

    # check yielded result
    assert len(res['workflow_runs']) == len(data_wflrun['workflow_runs'])  # same as original
    assert res['workflow_runs'][0] == {'name': 'workflow_bwa-mem_no_unzip-check',
                      'shard': '0:0',
                      'jobid': 'somejobid',
                      'status': 'failed'}  # changed from running to failed, no output.


def test_CheckStatusFF_running():
    """This check does not actually connect to the portal.
    It uses mocks for get_status and get_output
    """
    with open('test/files/CGAP_WGS_trio_scatter.run.json') as json_file:
        data_wflrun = json.load(json_file)

    # fake that the first one is running
    data_wflrun['workflow_runs'][0]['status'] = 'running'
    data_wflrun['workflow_runs'][0]['jobid'] = 'somejobid'

    # Create MetaWorkflowRun object and check_running generator
    wflrun_obj = run.MetaWorkflowRun(data_wflrun)
    cs = check_status_ff.CheckStatusFF(wflrun_obj)
    cr = cs.check_running()

    # mock get_status and get_output
    with mock.patch('magma.check_status_ff.CheckStatusFF.get_status', return_value='started'):
        with mock.patch('magma.check_status_ff.CheckStatusFF.get_output',
                        return_value=[{'argument_name': 'raw_bam', 'files': 'abc'}]):
            res = next(cr)

    # check yielded result
    assert res is None


@pytest.mark.portaltest
def test_CheckStatusFF_real_failed():
    """check status for a real job 'c5TzfqljUygR' (errored run) on cgapwolf"""
    small_wflrun = {'meta_workflow': 'somemwfuuid',
                    'input': {},
                    'final_status': 'pending',
                    'workflow_runs': [{'jobid': 'c5TzfqljUygR',
                                       'status': 'running',
                                       'name': 'workflow_bwa-mem_no_unzip-check',
                                       'shard': '0:0'}]}
    wflrun_obj = run.MetaWorkflowRun(small_wflrun)

    # use cgapwolf by specifying env
    cs = check_status_ff.CheckStatusFF(wflrun_obj, env='fourfront-cgapwolf')
    cr = cs.check_running()
    res = next(cr)
    assert res['workflow_runs'] == [{'jobid': 'c5TzfqljUygR',
                    'name': 'workflow_bwa-mem_no_unzip-check',
                    'shard': '0:0',
                    'status': 'failed'}]  # add failed status, not adding output


@pytest.mark.portaltest
def test_CheckStatusFF_real_completed():
    """check status for a real job 'RCYui9haX4Ea' (successful run) on cgapwolf"""
    small_wflrun = {'meta_workflow': 'somemwfuuid',
                    'input': {},
                    'final_status': 'pending',
                    'workflow_runs': [{'jobid': 'RCYui9haX4Ea',
                                       'status': 'running',
                                       'name': 'workflow_bwa-mem_no_unzip-check',
                                       'shard': '0:0'}]}
    wflrun_obj = run.MetaWorkflowRun(small_wflrun)

    # use cgapwolf by specifying env
    cs = check_status_ff.CheckStatusFF(wflrun_obj, env='fourfront-cgapwolf')
    cr = cs.check_running()
    res = next(cr)
    assert res['workflow_runs'] == [{'jobid': 'RCYui9haX4Ea',
                    'name': 'workflow_bwa-mem_no_unzip-check',
                    'shard': '0:0',
                    # add status and output
                    'status': 'completed',
                    'output': [{'argument_name': 'raw_bam',
                                'files': '59939d48-1c7e-4b9d-a644-fdcaff8610be'}]}]
