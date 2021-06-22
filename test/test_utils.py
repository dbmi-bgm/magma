#################################################################
#   Libraries
#################################################################
import sys, os
import pytest
import json

from magma import metawfl as wfl
from magma import metawflrun as run
from magma_ff import inputgenerator as ingen
from magma import runupdate as runupd

#################################################################
#   Vars
#################################################################
completed = {
    'workflow_bwa-mem_no_unzip-check:2:0': {'name': 'workflow_bwa-mem_no_unzip-check',
      'output': [{'argument_name': 'raw_bam', 'file': 'uuid-raw_bam-2:0'}],
      'status': 'completed',
      'shard': '2:0',
      'jobid': 'a1b2c3d'},
    'workflow_bwa-mem_no_unzip-check:2:1': {'name': 'workflow_bwa-mem_no_unzip-check',
     'output': [{'argument_name': 'raw_bam', 'file': 'uuid-raw_bam-2:1'}],
     'status': 'completed',
     'shard': '2:1',
     'jobid': 'e1f2g3h'},
    'workflow_bwa-mem_no_unzip-check:2:2': {'name': 'workflow_bwa-mem_no_unzip-check',
     'output': [{'argument_name': 'raw_bam', 'file': 'uuid-raw_bam-2:2'}],
     'status': 'completed',
     'shard': '2:2',
     'jobid': 'AAAAAAa'},
    'workflow_add-readgroups-check:2:0': {'name': 'workflow_add-readgroups-check',
      'output': [{'argument_name': 'bam_w_readgroups',
        'file': 'uuid-bam_w_readgroups-2:0'}],
      'status': 'completed',
      'dependencies': ['workflow_bwa-mem_no_unzip-check:2:0'],
      'shard': '2:0'},
    'workflow_add-readgroups-check:2:1': {'name': 'workflow_add-readgroups-check',
      'output': [{'argument_name': 'bam_w_readgroups',
        'file': 'uuid-bam_w_readgroups-2:1'}],
      'status': 'completed',
      'dependencies': ['workflow_bwa-mem_no_unzip-check:2:1'],
      'shard': '2:1'},
    'workflow_add-readgroups-check:2:2': {'name': 'workflow_add-readgroups-check',
      'output': [{'argument_name': 'bam_w_readgroups',
        'file': 'uuid-bam_w_readgroups-2:2'}],
      'status': 'completed',
      'dependencies': ['workflow_bwa-mem_no_unzip-check:2:2'],
      'shard': '2:2'}
}

pending_shards = { # pending only
                   #    workflow_bwa-mem_no_unzip-check:2:1,
                   #    workflow_add-readgroups-check:2:0
                   #    workflow_add-readgroups-check:2:2
    'workflow_bwa-mem_no_unzip-check:2:0': {
      'name': 'workflow_bwa-mem_no_unzip-check',
      'output': [{'argument_name': 'raw_bam', 'file': 'uuid-raw_bam-2:0'}],
      'status': 'completed',
      'shard': '2:0',
      'jobid': 'a1b2c3d'},
    'workflow_bwa-mem_no_unzip-check:2:1': {
      'name': 'workflow_bwa-mem_no_unzip-check',
      'status': 'pending',
      'shard': '2:1'},
    'workflow_bwa-mem_no_unzip-check:2:2': {
      'name': 'workflow_bwa-mem_no_unzip-check',
      'output': [{'argument_name': 'raw_bam', 'file': 'uuid-raw_bam-2:2'}],
      'status': 'completed',
      'shard': '2:2',
      'jobid': 'AAAAAAa'},
    'workflow_add-readgroups-check:2:0': {
      'name': 'workflow_add-readgroups-check',
      'status': 'pending',
      'dependencies': ['workflow_bwa-mem_no_unzip-check:2:0'],
      'shard': '2:0'},
    'workflow_add-readgroups-check:2:1': {
      'name': 'workflow_add-readgroups-check',
      'output': [{'argument_name': 'bam_w_readgroups',
        'file': 'uuid-bam_w_readgroups-2:1'}],
      'status': 'completed',
      'dependencies': ['workflow_bwa-mem_no_unzip-check:2:1'],
      'shard': '2:1'},
    'workflow_add-readgroups-check:2:2': {
      'name': 'workflow_add-readgroups-check',
      'status': 'pending',
      'dependencies': ['workflow_bwa-mem_no_unzip-check:2:2'],
      'shard': '2:2'}
}

pending = {
    'workflow_bwa-mem_no_unzip-check:2:0': {
        'name': 'workflow_bwa-mem_no_unzip-check',
        'status': 'pending',
        'shard': '2:0'},
    'workflow_bwa-mem_no_unzip-check:2:1': {
        'name': 'workflow_bwa-mem_no_unzip-check',
        'status': 'pending',
        'shard': '2:1'},
    'workflow_bwa-mem_no_unzip-check:2:2': {
        'name': 'workflow_bwa-mem_no_unzip-check',
        'status': 'pending',
        'shard': '2:2'},
    'workflow_add-readgroups-check:2:0': {
        'name': 'workflow_add-readgroups-check',
        'status': 'pending',
        'dependencies': ['workflow_bwa-mem_no_unzip-check:2:0'],
        'shard': '2:0'},
    'workflow_add-readgroups-check:2:1': {
        'name': 'workflow_add-readgroups-check',
        'status': 'pending',
        'dependencies': ['workflow_bwa-mem_no_unzip-check:2:1'],
        'shard': '2:1'},
    'workflow_add-readgroups-check:2:2': {
        'name': 'workflow_add-readgroups-check',
        'status': 'pending',
        'dependencies': ['workflow_bwa-mem_no_unzip-check:2:2'],
        'shard': '2:2'}
}

#################################################################
#   Tests
#################################################################
def test_inputgen_WGS_trio_scatter():
    # Results expected
    result = [
        {'app_name': 'workflow_bwa-mem_no_unzip-check', 'workflow_uuid': '50e75343-2e00-471d-a667-4acb083287d8',
        'config': {
            'log_bucket': 'tibanna-output'
            },
        'jobid': 'JOBID',
        'parameters': {},
        '_tibanna': {
          'run_type': 'workflow_bwa-mem_no_unzip-check',
          'env': 'env'
        },
        'input_files': [
            {'workflow_argument_name': 'fastq_R1', 'uuid': 'A1', 'mount': True},
            {'workflow_argument_name': 'fastq_R2', 'uuid': 'A2', 'mount': True},
            {'workflow_argument_name': 'reference', 'uuid': 'b24ed5ed-a037-48a0-a938-3fecfb90d0cf', 'mount': True}]},

        {'app_name': 'workflow_bwa-mem_no_unzip-check', 'workflow_uuid': '50e75343-2e00-471d-a667-4acb083287d8',
        'config': {
            'log_bucket': 'tibanna-output'
            },
        'jobid': 'JOBID',
        'parameters': {},
        '_tibanna': {
          'run_type': 'workflow_bwa-mem_no_unzip-check',
          'env': 'env'
        },
        'input_files': [
            {'workflow_argument_name': 'fastq_R1', 'uuid': 'C1', 'mount': True},
            {'workflow_argument_name': 'fastq_R2', 'uuid': 'C2', 'mount': True},
            {'workflow_argument_name': 'reference', 'uuid': 'b24ed5ed-a037-48a0-a938-3fecfb90d0cf', 'mount': True}]},

        {'app_name': 'workflow_bwa-mem_no_unzip-check', 'workflow_uuid': '50e75343-2e00-471d-a667-4acb083287d8',
        'config': {
            'log_bucket': 'tibanna-output'
            },
        'jobid': 'JOBID',
        'parameters': {},
        '_tibanna': {
          'run_type': 'workflow_bwa-mem_no_unzip-check',
          'env': 'env'
        },
        'input_files': [
            {'workflow_argument_name': 'fastq_R1', 'uuid': 'D1', 'mount': True},
            {'workflow_argument_name': 'fastq_R2', 'uuid': 'D2', 'mount': True},
            {'workflow_argument_name': 'reference', 'uuid': 'b24ed5ed-a037-48a0-a938-3fecfb90d0cf', 'mount': True}]},

        {'app_name': 'workflow_merge-bam-check', 'workflow_uuid': '4853a03a-8c0c-4624-a45d-c5206a72907b',
        'config': {
            'log_bucket': 'tibanna-output'
            },
        'jobid': 'JOBID',
        'parameters': {},
        '_tibanna': {
          'run_type': 'workflow_merge-bam-check',
          'env': 'env'
        },
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
    ingen_obj = ingen.InputGenerator(wfl_obj, wflrun_obj)
    in_gen = ingen_obj.input_generator()
    # Test results
    for i, (input_json, dict_out) in enumerate(in_gen):
        if 'jobid' in input_json:
            input_json['jobid'] = 'JOBID'
        else:
            assert False
        #end if
        assert input_json == result[i]
        for wflrun in dict_out['workflow_runs']:
            if wflrun['name'] + ':' + wflrun['shard'] == shard[i]:
                assert wflrun['status'] == 'running'
            #end if
            assert wflrun_obj.runs[shard[i]].status == 'running'
        #end for
    #end for
#end def

def test_runupdate_reset_steps():
    # Results
    initial = completed
    final = pending
    # Read input
    with open('test/files/CGAP_WGS_trio_scatter.run.json') as json_file:
        data_wflrun = json.load(json_file)
    # Create MetaWorkflowRun object
    wflrun_obj = run.MetaWorkflowRun(data_wflrun)
    # Run test and test result
    for workflow_run in wflrun_obj.runs_to_json():
        shard_name = workflow_run['name'] + ':' + workflow_run['shard']
        if shard_name in initial:
            assert initial[shard_name] == workflow_run
        #end if
    #end for
    runupdate = runupd.RunUpdate(wflrun_obj)
    x = runupdate.reset_steps(['workflow_bwa-mem_no_unzip-check', 'workflow_add-readgroups-check'])
    for workflow_run in x['workflow_runs']:
        shard_name = workflow_run['name'] + ':' + workflow_run['shard']
        if shard_name in final:
            assert final[shard_name] == workflow_run
        #end if
    #end for
#end def

def test_runupdate_reset_shards():
    # Results
    initial = completed
    final = pending_shards
    # Read input
    with open('test/files/CGAP_WGS_trio_scatter.run.json') as json_file:
        data_wflrun = json.load(json_file)
    # Create MetaWorkflowRun object
    wflrun_obj = run.MetaWorkflowRun(data_wflrun)
    # Run test and test result
    for workflow_run in wflrun_obj.runs_to_json():
        shard_name = workflow_run['name'] + ':' + workflow_run['shard']
        if shard_name in initial:
            assert initial[shard_name] == workflow_run
        #end if
    #end for
    runupdate = runupd.RunUpdate(wflrun_obj)
    x = runupdate.reset_shards(['workflow_bwa-mem_no_unzip-check:2:1', 'workflow_add-readgroups-check:2:0', 'workflow_add-readgroups-check:2:2'])
    for workflow_run in x['workflow_runs']:
        shard_name = workflow_run['name'] + ':' + workflow_run['shard']
        if shard_name in final:
            assert final[shard_name] == workflow_run
        #end if
    #end for
#end def

def test_runupdate_import_step():
    # Results
    input = [
      {
        'argument_name': 'sample_name',
        'argument_type': 'parameter',
        'value': 'AVALUE'
      },
      {
        'argument_name': 'fastq_R1',
        'argument_type': 'file',
        'files': [['A1'], ['C1', 'D1'], ['B1', 'E1', 'F1']]
      },
      {
        'argument_name': 'fastq_R2',
        'argument_type': 'file',
        'files': [['A2'], ['C2', 'D2'], ['B2', 'E2', 'F2']]
      }
    ]
    initial = pending
    final = completed
    # Add FOO step to expected results
    final.setdefault(
    'FOO:0',
    {'name': 'FOO',
        'status': 'pending',
        'dependencies': ['workflow_picard-MarkDuplicates-check:0'],
        'shard': '0'})
    final.setdefault(
    'FOO:1',
    {'name': 'FOO',
        'status': 'pending',
        'dependencies': ['workflow_picard-MarkDuplicates-check:1'],
        'shard': '1'})
    final.setdefault(
    'FOO:2',
    {'name': 'FOO',
        'status': 'pending',
        'dependencies': ['workflow_picard-MarkDuplicates-check:2'],
        'shard': '2'})
    # Read input
    with open('test/files/CGAP_WGS_trio_scatter.run.json') as json_file:
        data_wflrun = json.load(json_file)
    with open('test/files/CGAP_WGS_trio_scatter_import_ExtraShard.run.json') as json_file:
        data_wflrun_i = json.load(json_file)
    # Create MetaWorkflowRun object
    wflrun_obj = run.MetaWorkflowRun(data_wflrun)
    wflrun_i_obj = run.MetaWorkflowRun(data_wflrun_i)
    # Run test and test result
    assert wflrun_i_obj.input == []
    for workflow_run in wflrun_i_obj.runs_to_json():
        shard_name = workflow_run['name'] + ':' + workflow_run['shard']
        if shard_name in initial:
            assert initial[shard_name] == workflow_run
        #end if
    #end for
    runupdate = runupd.RunUpdate(wflrun_i_obj)
    x = runupdate.import_steps(wflrun_obj, ['workflow_add-readgroups-check'])
    assert wflrun_i_obj.input == input
    assert x['meta_workflow'] == 'UUID_NEW'
    for workflow_run in x['workflow_runs']:
        shard_name = workflow_run['name'] + ':' + workflow_run['shard']
        if shard_name in final:
            assert final[shard_name] == workflow_run
        #end if
    #end for
#end def

def test_inputgen_formula_eval():
    # meta-worfklow-run json
    input_wflrun = {
      'meta_workflow': 'DIUU',
      'workflow_runs' : [
            {
              'name': 'Foo',
              'status': 'pending',
              'shard': '0',
            }],
      'input': [
        {
          'argument_name': 'pAram_x',
          'argument_type': 'parameter',
          'value': 10
        },
        {
          'argument_name': 'y',
          'argument_type': 'parameter',
          'value': 2
        },
        {
          'argument_name': 'RNAME',
          'argument_type': 'parameter',
          'value': ['I', 'm', 'rename']
        }],
        'final_status': 'pending'
    }
    # meta-workflow json
    input_wfl = {
          'name': 'ANAME',
          'uuid': 'UUDI',
          'input': [
            {
              'argument_name': 'a_global_file',
              'argument_type': 'file',
              'files': 'a_global_file-UUID'
            }
          ],
          'workflows': [
            {
              'name': 'Foo',
              'workflow': 'Foo-UUID',
              'config': {
                'instance_type': 'm5.large',
                'ebs_size': 'formula:pAram_x+10* y/(pAram_x)',
                'EBS_optimized': True,
                'spot_instance': False,
                'log_bucket': 'tibanna-output',
                'run_name': 'run_Foo'
              },
              'input': [
                {
                  'argument_name': 'a_local_file',
                  'argument_type': 'file',
                  'source_argument_name': 'a_global_file',
                  'rename': 'formula:RNAME'
                },
                {
                  'argument_name': 'a_local_file',
                  'argument_type': 'file',
                  'source_argument_name': 'a_global_file',
                  'rename': ['foobar']
                }
              ]
            }
          ]
        }
    # Results expected
    result = [
        {'app_name': 'Foo', 'workflow_uuid': 'Foo-UUID',
        'config': {
            'instance_type': 'm5.large',
            'ebs_size': 12,
            'EBS_optimized': True,
            'spot_instance': False,
            'log_bucket': 'tibanna-output',
            'run_name': 'run_Foo'
            },
        'jobid': 'JOBID',
        'parameters': {},
        '_tibanna': {
          'run_type': 'Foo',
          'env': 'env'
        },
        'input_files': [
            {'workflow_argument_name': 'a_local_file', 'uuid': 'a_global_file-UUID',
            'rename': ['I', 'm', 'rename']},
            {'workflow_argument_name': 'a_local_file', 'uuid': 'a_global_file-UUID',
            'rename': ['foobar']}
        ]}
    ]
    # Create MetaWorkflow and MetaWorkflowRun objects
    wfl_obj = wfl.MetaWorkflow(input_wfl)
    wflrun_obj = run.MetaWorkflowRun(input_wflrun)
    # Run test
    ingen_obj = ingen.InputGenerator(wfl_obj, wflrun_obj)
    in_gen = ingen_obj.input_generator()
    # Test results
    for i, (input_json, dict_out) in enumerate(in_gen):
        if 'jobid' in input_json:
            input_json['jobid'] = 'JOBID'
        else:
            assert False
        #end if
        assert input_json == result[i]
    #end for
#end def
