#################################################################
#   Libraries
#################################################################
import sys, os
import pytest

from magma_ff import parser

#################################################################
#   Tests
#################################################################
input_json = {}

input_wfl =     {
      'name': 'NAME',
      'uuid': 'UUID',
      'input': [
        {
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
          'files': [{'file': 'AB'}]
        },
        {
          'argument_name': 'a_file_1D',
          'argument_type': 'file',
          'files': [
              {'file': 'A', 'dimension': '2'},
              {'file': 'B', 'dimension': '0'},
              {'file': 'C', 'dimension': '1'}]
        },
        {
          'argument_name': 'a_file_2D',
          'argument_type': 'file',
          'files': [
                  {'file': 'a', 'dimension': '0,1'},
                  {'file': 'b', 'dimension': '0,0'},
                  {'file': 'c', 'dimension': '1,0'},
                  {'file': 'd', 'dimension': '1,1'}]
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
      'workflows': [
        {
          'name': 'name',
          'workflow': 'WORKFLOW',
          'config': {},
          'input': [
            {
              'argument_name': 'no_file_wfl',
              'argument_type': 'file'
            },
            {
              'argument_name': 'no_parameter',
              'argument_type': 'parameter'
            }
          ]
        },
        {
          'name': 'name_1',
          'workflow': 'WORKFLOW_1',
          'config': {},
          'input': [
            {
              'argument_name': 'a_float',
              'argument_type': 'parameter',
              'value': '4.2',
              'value_type': 'float'
            }
          ]
        }
      ]
    }

input_wfl_out =     {
      'name': 'NAME',
      'uuid': 'UUID',
      'input': [
        {
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
          'argument_name': 'a_file_1D',
          'argument_type': 'file',
          'files': [ 'B', 'C', 'A']
        },
        {
          'argument_name': 'a_file_2D',
          'argument_type': 'file',
          'files': [['b', 'a'], ['c', 'd']]
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
      ],
      'workflows': [
        {
          'name': 'name',
          'workflow': 'WORKFLOW',
          'config': {},
          'input': [
            {
              'argument_name': 'no_file_wfl',
              'argument_type': 'file'
            },
            {
              'argument_name': 'no_parameter',
              'argument_type': 'parameter'
            }
          ]
        },
        {
          'name': 'name_1',
          'workflow': 'WORKFLOW_1',
          'config': {},
          'input': [
            {
              'argument_name': 'a_float',
              'argument_type': 'parameter',
              'value': 4.2
            }
          ]
        }
      ]
    }

#################################################################
#   Tests
#################################################################
def test__files_to_json_0D():
    # Input
    files = [
        {
            'file': 'AB'
        }]
    # Results
    files_out = 'AB'
    # Test
    pff_obj = parser.ParserFF(input_json)
    assert pff_obj._files_to_json(files) == files_out
#end def

def test__files_to_json_1D():
    # Input
    files_1 = [
        {
            'file': 'A',
            'dimension': '2'
        },
        {
            'file': 'B',
            'dimension': '0'
        },
        {
            'file': 'C',
            'dimension': '1'
        }]
    files_2 = [
        {
            'file': 'A',
            'dimension': '1'
        },
        {
            'file': 'B',
            'dimension': '2'
        },
        {
            'file': 'C',
            'dimension': '0'
        }]
    # Results
    files_1_out = ['B', 'C', 'A']
    files_2_out = ['C', 'A', 'B']
    # Test
    pff_obj = parser.ParserFF(input_json)
    assert pff_obj._files_to_json(files_1) == files_1_out
    assert pff_obj._files_to_json(files_2) == files_2_out
#end def

def test__files_to_json_2D():
    # Input
    files = [
            {
                'file': 'a',
                'dimension': '0,1'
            },
            {
                'file': 'b',
                'dimension': '0,0'
            },
            {
                'file': 'c',
                'dimension': '1,0'
            },
            {
                'file': 'd',
                'dimension': '1,1'
            }]
    # Results
    files_out = [['b', 'a'], ['c', 'd']]
    # Test
    pff_obj = parser.ParserFF(input_json)
    assert pff_obj._files_to_json(files) == files_out
#end def

def test__files_to_json_3D():
    # Input
    files = [
            {
                'file': 'a',
                'dimension': '0,0,0'
            },
            {
                'file': 'b',
                'dimension': '0,0,1'
            },
            {
                'file': 'c',
                'dimension': '0,1,0'
            },
            {
                'file': 'd',
                'dimension': '1,0,0'
            },
            {
                'file': 'e',
                'dimension': '1,0,1'
            },
            {
                'file': 'f',
                'dimension': '1,1,1'
            },
            {
                'file': 'g',
                'dimension': '1,1,0'
            },
            {
                'file': 'h',
                'dimension': '2,0,0'
            }]
    # Results
    files_out = [[['a', 'b'], ['c']], [['d', 'e'], ['g', 'f']], [['h']]]
    # Test
    pff_obj = parser.ParserFF(input_json)
    assert pff_obj._files_to_json(files) == files_out
#end def

def test_arguments_to_json_wfl():
    # Test
    pff_obj = parser.ParserFF(input_wfl)
    res = pff_obj.arguments_to_json()
    assert res == input_wfl_out
#end def

def test_arguments_to_json_metawfl():
    input_wfl = {
        'meta_workflow': '',
        'workflow_runs' : [],
        'final_status': '',
        'common_fields': {},
        'input': [
          {
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
            'files': [{'file': 'AB'}]
          },
          {
            'argument_name': 'a_file_1D',
            'argument_type': 'file',
            'files': [
                {'file': 'A', 'dimension': '2'},
                {'file': 'B', 'dimension': '0'},
                {'file': 'C', 'dimension': '1'}]
          },
          {
            'argument_name': 'a_file_2D',
            'argument_type': 'file',
            'files': [
                    {'file': 'a', 'dimension': '0,1'},
                    {'file': 'b', 'dimension': '0,0'},
                    {'file': 'c', 'dimension': '1,0'},
                    {'file': 'd', 'dimension': '1,1'}]
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
            'value': 'fALSE',
            'value_type': 'boolean'
          }
        ]
    }

    input_wfl_out = {
        'meta_workflow': '',
        'workflow_runs' : [],
        'final_status': '',
        'common_fields': {},
        'input': [
            {
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
              'argument_name': 'a_file_1D',
              'argument_type': 'file',
              'files': [ 'B', 'C', 'A']
            },
            {
              'argument_name': 'a_file_2D',
              'argument_type': 'file',
              'files': [['b', 'a'], ['c', 'd']]
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
              'value': False
            }
        ]
    }
    # Test
    pff_obj = parser.ParserFF(input_wfl)
    res = pff_obj.arguments_to_json()
    assert res == input_wfl_out
#end def
