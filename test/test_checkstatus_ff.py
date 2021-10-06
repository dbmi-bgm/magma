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
    wflrun_obj = run_ff.MetaWorkflowRun(small_wflrun)

    # use cgapwolf by specifying env
    cs = checkstatus.CheckStatusFF(wflrun_obj, env='fourfront-cgapwolf')
    cr = cs.check_running()
    res = next(cr)
    assert res['workflow_runs'] == [{'jobid': 'c5TzfqljUygR',
                    'workflow_run': '750851fd-00ad-49d9-98db-468cbdd552b5',
                    'name': 'workflow_bwa-mem_no_unzip-check',
                    'shard': '0:0',
                    'status': 'failed'}]  # add failed status, not adding output
    assert res['failed_jobs'] == ['c5TzfqljUygR']
    assert 'cost' not in res # since the meta workflow run failes, cost should be  computed


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
    wflrun_obj = run_ff.MetaWorkflowRun(small_wflrun)

    # use cgapwolf by specifying env
    cs = checkstatus.CheckStatusFF(wflrun_obj, env='fourfront-cgapwolf')
    cr = cs.check_running()
    res = next(cr)
    assert res['workflow_runs'] == [{'jobid': 'RCYui9haX4Ea',
                    'workflow_run': 'aee412ea-02ad-4dc7-b470-011fdbacf60f',
                    'name': 'workflow_bwa-mem_no_unzip-check',
                    'shard': '0:0',
                    # add status and output
                    'status': 'completed',
                    'output': [{'argument_name': 'raw_bam',
                                'file': '59939d48-1c7e-4b9d-a644-fdcaff8610be'}]}]

@pytest.mark.portaltest
def test_CheckStatusFF_real_completed_with_cost():
    """check status for two real jobs 'gjpZKLQ4R6M3' and 'npFSgRy9cllw' (successful run) on cgapwolf"""
    small_wflrun = {'meta_workflow': 'somemwfuuid',
                    'input': {},
                    'final_status': 'pending',
                    'workflow_runs': [{'jobid': 'gjpZKLQ4R6M3',
                                       'status': 'running',
                                       'name': 'workflow_manta_vcf-check',
                                       'shard': '0'},
                                       {'jobid': 'npFSgRy9cllw',
                                       'status': 'running',
                                       'name': 'workflow_annotateSV_sansa_vep_vcf-check',
                                       'shard': '0'}]}
    wflrun_obj = run_ff.MetaWorkflowRun(small_wflrun)

    # use cgapwolf by specifying env
    cs = checkstatus.CheckStatusFF(wflrun_obj, env='fourfront-cgapwolf')
    cr = cs.check_running()
    res = next(cr)
    assert 'cost' not in res
    res = next(cr)
    assert 'cost' in res # we only expect cost to be there, when the whole MetaWorflowRun completed
    assert res['cost'] > 0.0
    cost_without_failed = res['cost']

    # Repeat the above, where we add the jobs to failed_jobs as well. Not realistic, but tests the cost estimation
    small_wflrun = {'meta_workflow': 'somemwfuuid',
                    'input': {},
                    'final_status': 'pending',
                    'failed_jobs': ['gjpZKLQ4R6M3','npFSgRy9cllw'],
                    'workflow_runs': [{'jobid': 'gjpZKLQ4R6M3',
                                       'status': 'running',
                                       'name': 'workflow_manta_vcf-check',
                                       'shard': '0'},
                                       {'jobid': 'npFSgRy9cllw',
                                       'status': 'running',
                                       'name': 'workflow_annotateSV_sansa_vep_vcf-check',
                                       'shard': '0'}]}
    wflrun_obj = run_ff.MetaWorkflowRun(small_wflrun)
    # use cgapwolf by specifying env
    cs = checkstatus.CheckStatusFF(wflrun_obj, env='fourfront-cgapwolf')
    cr = cs.check_running()
    res = next(cr)
    assert 'cost' not in res
    res = next(cr)
    assert 'cost' in res # we only expect cost to be there, when the whole MetaWorflowRun completed
    assert res['cost'] == 2 * cost_without_failed


