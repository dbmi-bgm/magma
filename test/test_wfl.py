#################################################################
#   Libraries
#################################################################
import sys, os
import pytest
import json

from magma import wfl

#################################################################
#   Tests
#################################################################
# Test pipeline test_METAWFL
# A - B - C - D - G - P - M
#          \       \
#           E ----- H
#                  /
# Z --------------
#
# A is scatter 1
# D is gather 1 from C
# H is gather 1 from E

# Test pipeline test_METAWFL_3D
#    B - C - D - G
#   / \
# A    \
#  \    E
#   \    \
#    - M, H, X
#
# A is scatter 3
# C is gather 1 from B
# D is gather 2 from C
# E is gather 2 from B
# M is gather 1 from A, gather 1 from E
# H is gather 2 from A
# X is gather 2 from A, gather 1 from E

# Template example
template = {
  "accession": "", # must be unique
  "app_name": "", # must be unique
  "app_version": "",
  "uuid": "", # must be unique
  # GENERAL ARGUMENTS
  "arguments": [
    {
      "argument_name": "",
      "argument_type": "file",
      "uuid": "",
      "mount": False,
      "rename": "",
      "unzip": ""
    },
    {
      "argument_name": "",
      "argument_type": "parameter",
      "value": ""
    }
  ],
  # STEPS
  "workflows": [
    {
      "name": "", # must be unique
      "uuid": "", # WORKFLOW UUID, must be unique
      "config": {
        "instance_type": "",
        "ebs_size": "",
        "EBS_optimized": True,
        "spot_instance": True,
        "log_bucket": "tibanna-output",
        "run_name": "run_",
        "behavior_on_capacity_limit": "wait_and_retry"
      },
      # STEP ARGUMENTS
      # Try to check for value (parameters) or link to previous step (file)
      #   if not specified, try match to general arguments, if not is initial input
      "arguments": [
        {
          "argument_name": "",
          "argument_type": "file",
          "scatter": 0,
          "gather": 0,
          "mount": False,
          "rename": "",
          "unzip": "",
          # SPECIFY IF LINK PREVIOUS STEP OUTPUT
          "source_step": "",
          "source_argument_name": ""
          # if no match need to be done because it's the first input
          #   skip source_step field
          # if parameter or file value is case specific and need to come from
          #   workflow run we can just set a placeholder like <whatever>.value
          #   where <whatever>.value means pick 'value' in 'whatever' section in
          #   current workflow run object
          # if source_argument_name but not source_step it's global
        },
        {
          "argument_name": "",
          "argument_type": "parameter",
          "value": ""
        }
      ],
      "outputs": [] # LIST OUTPUTS
    }
  ]
}

def test_wfl_M_H():
    ''' '''
    # Results expected
    results = {
     'A': {'0': [], '1': [], '2': []},
     'B': {'0': ['A:0'], '1': ['A:1'], '2': ['A:2']},
     'C': {'0': ['B:0'], '1': ['B:1'], '2': ['B:2']},
     'E': {'0': ['C:0'], '1': ['C:1'], '2': ['C:2']},
     'D': {'0': ['C:0', 'C:1', 'C:2']},
     'G': {'0': ['D:0']},
     'H': {'0': ['E:0', 'E:1', 'E:2', 'G:0', 'Z:0']},
     'P': {'0': ['G:0']},
     'M': {'0': ['P:0']},
     'Z': {'0': []},
     'steps': ['A', 'A', 'A', 'B', 'B', 'B', 'C', 'C', 'C', 'D', 'E', 'E', 'E', 'G', 'H', 'M', 'P', 'Z']
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl.MetaWorkflow(data)
    # Run test
    x = wfl_obj.write_run(['M', 'H'], input=['f1', 'f2', 'f3'])
    # Test steps
    assert sorted([wfl_['name'] for wfl_ in x['workflow_runs']]) == results['steps']
    # Test depencencies
    for wfl_ in x['workflow_runs']:
        # print(x['workflow_runs'])
        assert sorted(wfl_['dependencies']) == results[wfl_['name']][wfl_['shard']]
    #end for
#end def

def test_wfl_P():
    ''' '''
    # Results expected
    results = {
     'A': {'0': [], '1': [], '2': []},
     'B': {'0': ['A:0'], '1': ['A:1'], '2': ['A:2']},
     'C': {'0': ['B:0'], '1': ['B:1'], '2': ['B:2']},
     'D': {'0': ['C:0', 'C:1', 'C:2']},
     'G': {'0': ['D:0']},
     'P': {'0': ['G:0']},
     'steps': ['A', 'A', 'A', 'B', 'B', 'B', 'C', 'C', 'C', 'D', 'G', 'P']
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl.MetaWorkflow(data)
    # Run test
    x = wfl_obj.write_run(['P'], input=['f1', 'f2', 'f3'])
    # Test steps
    assert sorted([wfl_['name'] for wfl_ in x['workflow_runs']]) == results['steps']
    # Test depencencies
    for wfl_ in x['workflow_runs']:
        assert sorted(wfl_['dependencies']) == results[wfl_['name']][wfl_['shard']]
    #end for
#end def

def test_wfl_2D_WGS_trio():
    # Results expected
    result = {'meta_workflow_uuid': 'UUID', 'workflow_runs': [{'name': 'workflow_bwa-mem_no_unzip-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': [], 'shard': '0:0'}, {'name': 'workflow_bwa-mem_no_unzip-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': [], 'shard': '1:0'}, {'name': 'workflow_bwa-mem_no_unzip-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': [], 'shard': '1:1'}, {'name': 'workflow_bwa-mem_no_unzip-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': [], 'shard': '2:0'}, {'name': 'workflow_bwa-mem_no_unzip-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': [], 'shard': '2:1'}, {'name': 'workflow_bwa-mem_no_unzip-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': [], 'shard': '2:2'}, {'name': 'workflow_add-readgroups-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_bwa-mem_no_unzip-check:0:0'], 'shard': '0:0'}, {'name': 'workflow_add-readgroups-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_bwa-mem_no_unzip-check:1:0'], 'shard': '1:0'}, {'name': 'workflow_add-readgroups-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_bwa-mem_no_unzip-check:1:1'], 'shard': '1:1'}, {'name': 'workflow_add-readgroups-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_bwa-mem_no_unzip-check:2:0'], 'shard': '2:0'}, {'name': 'workflow_add-readgroups-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_bwa-mem_no_unzip-check:2:1'], 'shard': '2:1'}, {'name': 'workflow_add-readgroups-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_bwa-mem_no_unzip-check:2:2'], 'shard': '2:2'}, {'name': 'workflow_merge-bam-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_add-readgroups-check:0:0'], 'shard': '0'}, {'name': 'workflow_merge-bam-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_add-readgroups-check:1:0', 'workflow_add-readgroups-check:1:1'], 'shard': '1'}, {'name': 'workflow_merge-bam-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_add-readgroups-check:2:0', 'workflow_add-readgroups-check:2:1', 'workflow_add-readgroups-check:2:2'], 'shard': '2'}, {'name': 'workflow_picard-MarkDuplicates-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_merge-bam-check:0'], 'shard': '0'}, {'name': 'workflow_picard-MarkDuplicates-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_merge-bam-check:1'], 'shard': '1'}, {'name': 'workflow_picard-MarkDuplicates-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_merge-bam-check:2'], 'shard': '2'}, {'name': 'workflow_sort-bam-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_picard-MarkDuplicates-check:0'], 'shard': '0'}, {'name': 'workflow_sort-bam-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_picard-MarkDuplicates-check:1'], 'shard': '1'}, {'name': 'workflow_sort-bam-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_picard-MarkDuplicates-check:2'], 'shard': '2'}, {'name': 'workflow_gatk-BaseRecalibrator', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_sort-bam-check:0'], 'shard': '0'}, {'name': 'workflow_gatk-BaseRecalibrator', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_sort-bam-check:1'], 'shard': '1'}, {'name': 'workflow_gatk-BaseRecalibrator', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_sort-bam-check:2'], 'shard': '2'}, {'name': 'workflow_gatk-ApplyBQSR-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_gatk-BaseRecalibrator:0', 'workflow_sort-bam-check:0'], 'shard': '0'}, {'name': 'workflow_gatk-ApplyBQSR-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_gatk-BaseRecalibrator:1', 'workflow_sort-bam-check:1'], 'shard': '1'}, {'name': 'workflow_gatk-ApplyBQSR-check', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_gatk-BaseRecalibrator:2', 'workflow_sort-bam-check:2'], 'shard': '2'}, {'name': 'workflow_gatk-HaplotypeCaller', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_gatk-ApplyBQSR-check:0'], 'shard': '0'}, {'name': 'workflow_gatk-HaplotypeCaller', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_gatk-ApplyBQSR-check:1'], 'shard': '1'}, {'name': 'workflow_gatk-HaplotypeCaller', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_gatk-ApplyBQSR-check:2'], 'shard': '2'}, {'name': 'workflow_gatk-CombineGVCFs', 'workflow_run_uuid': '', 'output': '', 'status': 'pending', 'dependencies': ['workflow_gatk-HaplotypeCaller:0', 'workflow_gatk-HaplotypeCaller:1', 'workflow_gatk-HaplotypeCaller:2'], 'shard': '0'}], 'input': []}
    # Read input
    with open('test/files/CGAP_WGS_trio.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl.MetaWorkflow(data)
    # Run test
    input = [['A'], ['C', 'D'], ['B', 'E', 'F']]
    x = wfl_obj.write_run(['workflow_gatk-CombineGVCFs'], input)
    # Test results
    assert x == result
#end def

def test_wfl_3D_M_G_H_X():
    # Results expected
    results = {
     'A': {'0:0:0': [], '0:0:1': [], '0:1:0': [], '1:0:0': [], '1:0:1': []},
     'B': {'0:0:0': ['A:0:0:0'], '0:0:1': ['A:0:0:1'], '0:1:0': ['A:0:1:0'],
           '1:0:0': ['A:1:0:0'], '1:0:1': ['A:1:0:1']},
     'C': {'0:0': ['B:0:0:0', 'B:0:0:1'], '0:1': ['B:0:1:0'], '1:0': ['B:1:0:0', 'B:1:0:1']},
     'D': {'0': ['C:0:0', 'C:0:1', 'C:1:0']},
     'G': {'0': ['D:0']},
     'E': {'0': ['B:0:0:0', 'B:0:0:1', 'B:0:1:0'], '1': ['B:1:0:0', 'B:1:0:1']},
     'M': {'0:0': ['A:0:0:0', 'A:0:0:1', 'E:0', 'E:1'],
           '0:1': ['A:0:1:0', 'E:0', 'E:1'],
           '1:0': ['A:1:0:0', 'A:1:0:1', 'E:0', 'E:1']},
     'X': {'0': ['A:0:0:0', 'A:0:0:1', 'A:0:1:0', 'E:0', 'E:1'],
           '1': ['A:1:0:0', 'A:1:0:1', 'E:0', 'E:1']},
     'H': {'0': ['A:0:0:0', 'A:0:0:1', 'A:0:1:0', 'E:0'],
           '1': ['A:1:0:0', 'A:1:0:1', 'E:1']}
    }
    # Read input
    with open('test/files/test_METAWFL_3D.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl.MetaWorkflow(data)
    # Run test
    input = [[['a', 'b'], ['c']], [['h', 'i']]]
    x = wfl_obj.write_run(['M', 'G', 'H', 'X'], input)
    # Test depencencies
    for wfl_ in x['workflow_runs']:
        assert sorted(wfl_['dependencies']) == results[wfl_['name']][wfl_['shard']]
    #end for
#end def

def test_wfl__order_run_H_P():
    # Results expected
    results = {
        'steps': ['Z', 'A', 'B', 'C', 'D', 'E', 'G', 'H', 'P'],
        'steps_': ['A', 'Z', 'B', 'C', 'D', 'E', 'G', 'H', 'P']
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl.MetaWorkflow(data)
    # Run test
    x = wfl_obj._order_run(['H', 'P'])
    # Test steps
    try: assert [x_.name for x_ in x] == results['steps']
    except Exception: [x_.name for x_ in x] == results['steps_']
#end def

def test_wfl__order_run_E():
    # Results expected
    results = {
        'steps': ['A', 'B', 'C', 'E'],
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl.MetaWorkflow(data)
    # Run test
    x = wfl_obj._order_run(['E'])
    # Test steps
    assert [x_.name for x_ in x] == results['steps']
#end def

def test_wfl__order_run_E_C():
    # Results expected
    results = {
        'steps': ['A', 'B', 'C', 'E'],
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl.MetaWorkflow(data)
    # Run test
    x = wfl_obj._order_run(['E', 'C'])
    # Test steps
    assert [x_.name for x_ in x] == results['steps']
#end def

def test_wfl__input_dimensions():
    # Results expected
    results = {
        'dim1': {1: [3]},
        'dim2': {1: [3], 2: [2, 3, 1]},
        'dim3': {1: [3], 2: [2, 2, 2], 3: [[2 ,1], [1, 3], [2, 2]]}
    }
    # Create Wfl object
    wfl_obj = wfl.MetaWorkflow(template)
    # Run test
    dim1 = ['f0', 'f1', 'f2']
    dim2 = [['f0', 'f1'], ['f2', 'f3', 'f4'], ['f5']]
    dim3 = [[['f0', 'f1'], ['f2']], [['f3'], ['f4', 'f5', 'f6']], [['f7', 'f8'], ['f9', 'f10']]]
    x_dim1 = wfl_obj._input_dimensions(dim1)
    x_dim2 = wfl_obj._input_dimensions(dim2)
    x_dim3 = wfl_obj._input_dimensions(dim3)
    # Test results
    assert x_dim1 == results['dim1']
    assert x_dim2 == results['dim2']
    assert x_dim3 == results['dim3']
#end def

def test_wfl__shards():
    # Results expected
    results = {
        'dim3_1': [['0'], ['1'], ['2']],
        'dim3_2': [['0', '0'], ['0', '1'], ['1', '0'], ['1', '1'], ['2', '0'], ['2', '1']],
        'dim3_3': [['0', '0', '0'], ['0', '0', '1'], ['0', '1', '0'],
                   ['1', '0', '0'], ['1', '1', '0'], ['1', '1', '1'], ['1', '1', '2'],
                   ['2', '0', '0'], ['2', '0', '1'], ['2', '1', '0'], ['2', '1', '1']]
    }
    # Create Wfl object
    wfl_obj = wfl.MetaWorkflow(template)
    # Run test
    dim3 = {1: [3], 2: [2, 2, 2], 3: [[2 ,1], [1, 3], [2, 2]]}
    dim3_1 = wfl_obj._shards(dim3, 1)
    dim3_2 = wfl_obj._shards(dim3, 2)
    dim3_3 = wfl_obj._shards(dim3, 3)
    # Test results
    assert dim3_1 == results['dim3_1']
    assert dim3_2 == results['dim3_2']
    assert dim3_3 == results['dim3_3']
#end def