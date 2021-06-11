#################################################################
#   Libraries
#################################################################
import sys, os
import pytest
import json

from magma_ff import metawflrun as run_ff

#################################################################
#   Tests
#################################################################
def test_run_ff():
    # meta-worfklow-run json
    input_wflrun = {
      'meta_workflow': 'AAID',
      'workflow_runs' : [
            {
              'name': 'Foo',
              'status': 'completed',
              'shard': '0'
            },
            {
              'name': 'Bar',
              'status': 'running',
              'shard': '0'
            },
            {
              'name': 'Poo',
              'status': 'pending',
              'shard': '0'
            }],
      'input': [
        {
          'argument_name': 'no_parameter',
          'argument_type': 'parameter'
        },
        {
          'argument_name': 'a_file_3D',
          'argument_type': 'file',
          'files': [
                  {'file': 'a', 'dimension': '0,0,0'},
                  {'file': 'b', 'dimension': '0,0,1'},
                  {'file': 'c', 'dimension': '0,1,0'},
                  {'file': 'd', 'dimension': '1,0,0'},
                  {'file': 'e', 'dimension': '1,0,1'},
                  {'file': 'f', 'dimension': '1,1,1'},
                  {'file': 'g', 'dimension': '1,1,0'},
                  {'file': 'h', 'dimension': '2,0,0'}]
        },
        {
          'argument_name': 'a_json',
          'argument_type': 'parameter',
          'value': '["a", "b", ["c"], {"d": 1, "e": 2}]',
          'value_type': 'json'
        },
        {
          'argument_name': 'a_integer',
          'argument_type': 'parameter',
          'value': '12',
          'value_type': 'integer'
        },
        {
          'argument_name': 'a_float',
          'argument_type': 'parameter',
          'value': '2.5',
          'value_type': 'float'
        },
        {
          'argument_name': 'a_string',
          'argument_type': 'parameter',
          'value': 'STRINGA',
          'value_type': 'string'
        },
        {
          'argument_name': 'a_boolean',
          'argument_type': 'parameter',
          'value': 'TrUe',
          'value_type': 'boolean'
        }
      ],
      'final_status': 'pending'
    }

    input_parser = [
        {
          'argument_name': 'no_parameter',
          'argument_type': 'parameter'
        },
        {
          'argument_name': 'a_file_3D',
          'argument_type': 'file',
          'files': [[['a', 'b'], ['c']], [['d', 'e'], ['g', 'f']], [['h']]]
        },
        {
          'argument_name': 'a_json',
          'argument_type': 'parameter',
          'value': ["a", "b", ["c"], {"d": 1, "e": 2}]
        },
        {
          'argument_name': 'a_integer',
          'argument_type': 'parameter',
          'value': 12
        },
        {
          'argument_name': 'a_float',
          'argument_type': 'parameter',
          'value': 2.5
        },
        {
          'argument_name': 'a_string',
          'argument_type': 'parameter',
          'value': 'STRINGA'
        },
        {
          'argument_name': 'a_boolean',
          'argument_type': 'parameter',
          'value': True
        }
    ]
    # Create object
    wflrun_obj = run_ff.MetaWorkflowRun(input_wflrun)
    # Test no-change
    wflrun_obj.update_status()
    assert wflrun_obj.final_status == 'running'
    # Test update and completed
    for shard_name in wflrun_obj.runs:
        wflrun_obj.update_attribute(shard_name, 'status', 'completed')
    #end for
    wflrun_obj.update_status() #now all step are completed
    assert wflrun_obj.final_status == 'completed'
    assert wflrun_obj.input == input_parser
#end def
