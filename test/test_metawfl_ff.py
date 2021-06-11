#################################################################
#   Libraries
#################################################################
import sys, os
import pytest
import json

from magma_ff import metawfl as wfl_ff

#################################################################
#   Tests
#################################################################
def test_wfl_ff_M_H():
    ''' '''
    # Results expected
    results = {
     # A no dependencies
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
    input_parsed = [{
          'argument_name': 'no_file',
          'argument_type': 'file'
        },
        {
          'argument_name': 'no_parameter',
          'argument_type': 'parameter'
        },
        {
          'argument_name': 'a_file',
          'argument_type': 'file',
          'files': 'AB'
        },
        {
          'argument_name': 'a_file_3D',
          'argument_type': 'file',
          'files': [[['a', 'b'], ['c']], [['d', 'e'], ['g', 'f']], [['h']]]
        }]
    # Read input
    with open('test/files/test_METAWFL_ff.json') as json_file:
        data = json.load(json_file)
    # Create MetaWorkflow object
    wfl_obj = wfl_ff.MetaWorkflow(data)
    # Run test
    x = wfl_obj.write_run(['f1', 'f2', 'f3'], ['M', 'H'])
    # Test steps
    assert sorted([wfl_['name'] for wfl_ in x['workflow_runs']]) == results['steps']
    # Test depencencies
    for wfl_ in x['workflow_runs']:
        # print(x['workflow_runs'])
        if 'dependencies' in wfl_:
            assert sorted(wfl_['dependencies']) == results[wfl_['name']][wfl_['shard']]
        #end if
    #end for
    assert wfl_obj.input == input_parsed
#end def
