#################################################################
#   Libraries
#################################################################
import sys, os
import pytest
import json

from magma import wfl
from magma import run
from magma import utils
# from tibanna_cgap.zebra_utils import ZebraInput


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
