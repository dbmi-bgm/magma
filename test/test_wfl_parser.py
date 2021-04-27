#################################################################
#   Libraries
#################################################################
import sys, os
import pytest
import json

from wfl_utils import wfl_parser

#################################################################
#   Tests
#################################################################
# Test pipeline
# A - B - C - D - G - P - M
#          \       \
#           E ----- H
#                  /
# Z --------------
#
# A is scatter
# D is gather from C
# H is gather from E

def test_wfl_parser_M_H():
    ''' '''
    # Results expected
    results = {
     'A': {0: [], 1: [], 2: []},
     'B': {0: ['A:0'], 1: ['A:1'], 2: ['A:2']},
     'C': {0: ['B:0'], 1: ['B:1'], 2: ['B:2']},
     'E': {0: ['C:0'], 1: ['C:1'], 2: ['C:2']},
     'D': {0: ['C:0', 'C:1', 'C:2']},
     'G': {0: ['D:0']},
     'H': {0: ['E:0', 'E:1', 'E:2', 'G:0', 'Z:0']},
     'P': {0: ['G:0']},
     'M': {0: ['P:0']},
     'Z': {0: []},
     'steps': ['A', 'A', 'A', 'B', 'B', 'B', 'C', 'C', 'C', 'D', 'E', 'E', 'E', 'G', 'H', 'M', 'P', 'Z']
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl_parser.Wfl(data)
    # Run test
    x = wfl_obj.write_wfl_run(['M', 'H'])
    # Test steps
    assert sorted([wfl_['name'] for wfl_ in x['workflow_runs']]) == results['steps']
    # Test depencencies
    for wfl_ in x['workflow_runs']:
        assert sorted(wfl_['dependencies']) == results[wfl_['name']][wfl_['shard']]
    #end for
#end def

def test_wfl_parser_P():
    ''' '''
    # Results expected
    results = {
     'A': {0: [], 1: [], 2: []},
     'B': {0: ['A:0'], 1: ['A:1'], 2: ['A:2']},
     'C': {0: ['B:0'], 1: ['B:1'], 2: ['B:2']},
     'D': {0: ['C:0', 'C:1', 'C:2']},
     'G': {0: ['D:0']},
     'P': {0: ['G:0']},
     'steps': ['A', 'A', 'A', 'B', 'B', 'B', 'C', 'C', 'C', 'D', 'G', 'P']
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl_parser.Wfl(data)
    # Run test
    x = wfl_obj.write_wfl_run(['P'])
    # Test steps
    assert sorted([wfl_['name'] for wfl_ in x['workflow_runs']]) == results['steps']
    # Test depencencies
    for wfl_ in x['workflow_runs']:
        assert sorted(wfl_['dependencies']) == results[wfl_['name']][wfl_['shard']]
    #end for
#end def

def test_wfl_parser__order_wfl_run_H_P():
    # Results expected
    results = {
        'steps': ['Z', 'A', 'B', 'C', 'D', 'E', 'G', 'H', 'P'],
        'steps_': ['A', 'Z', 'B', 'C', 'D', 'E', 'G', 'H', 'P']
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl_parser.Wfl(data)
    # Run test
    x = wfl_obj._order_wfl_run(['H', 'P'])
    try: assert [x_.name for x_ in x] == results['steps']
    except Exception: [x_.name for x_ in x] == results['steps_']
#end def

def test_wfl_parser__order_wfl_run_E():
    # Results expected
    results = {
        'steps': ['A', 'B', 'C', 'E'],
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl_parser.Wfl(data)
    # Run test
    x = wfl_obj._order_wfl_run(['E'])
    assert [x_.name for x_ in x] == results['steps']
#end def

def test_wfl_parser__order_wfl_run_E_C():
    # Results expected
    results = {
        'steps': ['A', 'B', 'C', 'E'],
    }
    # Read input
    with open('test/files/test_METAWFL.json') as json_file:
        data = json.load(json_file)
    # Create Wfl object
    wfl_obj = wfl_parser.Wfl(data)
    # Run test
    x = wfl_obj._order_wfl_run(['E', 'C'])
    assert [x_.name for x_ in x] == results['steps']
#end def
