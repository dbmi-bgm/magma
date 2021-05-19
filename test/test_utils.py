#################################################################
#   Libraries
#################################################################
import sys, os
import pytest
import mock
import json

from magma import wfl
from magma import run
from magma import utils
# from tibanna_cgap.zebra_utils import ZebraInput


# tests requiring connection are marked 'portaltest'.
# to perform tests without connection, use pytest -v -m "not portaltest" test_utils.py


# ZebraInput(**dict)
#################################################################
#   Tests
#################################################################
def test_inputgen_WGS_trio_scatter():
    # Results expected
    result = [
        {'app_name': 'workflow_bwa-mem_no_unzip-check', 'workflow_uuid': '50e75343-2e00-471d-a667-4acb083287d8',
        'config': {
            "log_bucket": "tibanna-output"
            },
        'jobid': 'JOBID',
        'parameters': {},
        'input_files': [
            {'workflow_argument_name': 'fastq_R1', 'uuid': 'A1', 'mount': True},
            {'workflow_argument_name': 'fastq_R2', 'uuid': 'A2', 'mount': True},
            {'workflow_argument_name': 'reference', 'uuid': 'b24ed5ed-a037-48a0-a938-3fecfb90d0cf', 'mount': True}]},

        {'app_name': 'workflow_bwa-mem_no_unzip-check', 'workflow_uuid': '50e75343-2e00-471d-a667-4acb083287d8',
        'config': {
            "log_bucket": "tibanna-output"
            },
        'jobid': 'JOBID',
        'parameters': {},
        'input_files': [
            {'workflow_argument_name': 'fastq_R1', 'uuid': 'C1', 'mount': True},
            {'workflow_argument_name': 'fastq_R2', 'uuid': 'C2', 'mount': True},
            {'workflow_argument_name': 'reference', 'uuid': 'b24ed5ed-a037-48a0-a938-3fecfb90d0cf', 'mount': True}]},

        {'app_name': 'workflow_bwa-mem_no_unzip-check', 'workflow_uuid': '50e75343-2e00-471d-a667-4acb083287d8',
        'config': {
            "log_bucket": "tibanna-output"
            },
        'jobid': 'JOBID',
        'parameters': {},
        'input_files': [
            {'workflow_argument_name': 'fastq_R1', 'uuid': 'D1', 'mount': True},
            {'workflow_argument_name': 'fastq_R2', 'uuid': 'D2', 'mount': True},
            {'workflow_argument_name': 'reference', 'uuid': 'b24ed5ed-a037-48a0-a938-3fecfb90d0cf', 'mount': True}]},

        {'app_name': 'workflow_merge-bam-check', 'workflow_uuid': '4853a03a-8c0c-4624-a45d-c5206a72907b',
        'config': {
            "log_bucket": "tibanna-output"
            },
        'jobid': 'JOBID',
        'parameters': {},
        'input_files': [
            {'workflow_argument_name': 'input_bams', 'uuid': ['uuid-bam_w_readgroups-2:0', 'uuid-bam_w_readgroups-2:1', 'uuid-bam_w_readgroups-2:2'], 'mount': True}]}
        ]
    shard = [
        'workflow_bwa-mem_no_unzip-check:0:0',
        'workflow_bwa-mem_no_unzip-check:1:0',
        'workflow_bwa-mem_no_unzip-check:1:1',
        'workflow_merge-bam-check:2'
    ]
    # Read input
    with open('test/files/CGAP_WGS_trio.json') as json_file:
        data_wfl = json.load(json_file)
    with open('test/files/CGAP_WGS_trio_scatter.run.json') as json_file:
        data_wflrun = json.load(json_file)
    # Create MetaWorkflow and MetaWorkflowRun objects
    wfl_obj = wfl.MetaWorkflow(data_wfl)
    wflrun_obj = run.MetaWorkflowRun(data_wflrun)
    # Run test
    ingen_obj = utils.InputGenerator(wfl_obj, wflrun_obj)
    ingen = ingen_obj.input_generator()
    # Test results
    for i, (input_json, workflow_runs) in enumerate(ingen):
        if 'jobid' in input_json:
            input_json['jobid'] = 'JOBID'
        else:
            assert False
        #end if
        assert input_json == result[i]
        for wflrun in workflow_runs:
            if wflrun['name'] + ':' + wflrun['shard'] == shard[i]:
                assert wflrun['status'] == 'running'
            #end if
            assert wflrun_obj.runs[shard[i]].status == 'running'
        #end for
    #end for
#end def


def test_CheckStatus():
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
    cs = utils.CheckStatus(wflrun_obj)
    cr = cs.check_running()

    # mock get_status and get_output
    with mock.patch('magma.utils.CheckStatus.get_status', return_value='complete'):
        with mock.patch('magma.utils.CheckStatus.get_output',
                        return_value=[{'workflow_argument_name': 'raw_bam', 'uuid': 'abc'}]):
            res = next(cr)

    # check yielded result
    assert len(res) == len(data_wflrun['workflow_runs'])  # same as original
    assert res[0] == {'name': 'workflow_bwa-mem_no_unzip-check',
                      'shard': '0:0',
                      'shard_name': 'workflow_bwa-mem_no_unzip-check:0:0',
                      'jobid': 'somejobid',
                      'status': 'completed',  # changed from running to completed
                      'output': [{'workflow_argument_name': 'raw_bam', 'uuid': 'abc'}]}  # output is filled in


def test_CheckStatus_failed():
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
    cs = utils.CheckStatus(wflrun_obj)
    cr = cs.check_running()

    # mock get_status and get_output
    with mock.patch('magma.utils.CheckStatus.get_status', return_value='error'):
        with mock.patch('magma.utils.CheckStatus.get_output',
                        return_value=[{'workflow_argument_name': 'raw_bam', 'uuid': 'abc'}]):
            res = next(cr)

    # check yielded result
    assert len(res) == len(data_wflrun['workflow_runs'])  # same as original
    assert res[0] == {'name': 'workflow_bwa-mem_no_unzip-check',
                      'shard': '0:0',
                      'shard_name': 'workflow_bwa-mem_no_unzip-check:0:0',
                      'jobid': 'somejobid',
                      'status': 'failed'}  # changed from running to failed, no output.


def test_CheckStatus_running():
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
    cs = utils.CheckStatus(wflrun_obj)
    cr = cs.check_running()

    # mock get_status and get_output
    with mock.patch('magma.utils.CheckStatus.get_status', return_value='started'):
        with mock.patch('magma.utils.CheckStatus.get_output',
                        return_value=[{'workflow_argument_name': 'raw_bam', 'uuid': 'abc'}]):
            res = next(cr)

    # check yielded result
    assert res is None


@pytest.mark.portaltest
def test_CheckStatus_real_failed():
    """check status for a real job 'c5TzfqljUygR' (errored run) on cgapwolf"""
    small_wflrun = {'meta_workflow_uuid': 'somemwfuuid',
                    'input': {},
                    'workflow_runs': [{'jobid': 'c5TzfqljUygR',
                                       'status': 'running',
                                       'name': 'workflow_bwa-mem_no_unzip-check',
                                       'shard': '0:0'}]}
    wflrun_obj = run.MetaWorkflowRun(small_wflrun)

    # use cgapwolf by specifying env
    cs = utils.CheckStatus(wflrun_obj, env='fourfront-cgapwolf')
    cr = cs.check_running()
    res = next(cr)
    assert res == [{'jobid': 'c5TzfqljUygR',
                    'name': 'workflow_bwa-mem_no_unzip-check',
                    'shard': '0:0',
                    'shard_name': 'workflow_bwa-mem_no_unzip-check:0:0',
                    'status': 'failed'}]  # add failed status, not adding output


@pytest.mark.portaltest
def test_CheckStatus_real_completed():
    """check status for a real job 'RCYui9haX4Ea' (successful run) on cgapwolf"""
    small_wflrun = {'meta_workflow_uuid': 'somemwfuuid',
                    'input': {},
                    'workflow_runs': [{'jobid': 'RCYui9haX4Ea',
                                       'status': 'running',
                                       'name': 'workflow_bwa-mem_no_unzip-check',
                                       'shard': '0:0'}]}
    wflrun_obj = run.MetaWorkflowRun(small_wflrun)

    # use cgapwolf by specifying env
    cs = utils.CheckStatus(wflrun_obj, env='fourfront-cgapwolf')
    cr = cs.check_running()
    res = next(cr)
    assert res == [{'jobid': 'RCYui9haX4Ea',
                    'name': 'workflow_bwa-mem_no_unzip-check',
                    'shard': '0:0',
                    'shard_name': 'workflow_bwa-mem_no_unzip-check:0:0',
                    # add status and output
                    'status': 'completed',
                    'output': [{'workflow_argument_name': 'raw_bam',
                                'uuid': '59939d48-1c7e-4b9d-a644-fdcaff8610be'}]}]
