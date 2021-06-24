#################################################################
#   Libraries
#################################################################
import sys, os
import pytest
import json

from magma import metawflrun as run

#################################################################
#   Tests
#################################################################
def test_update_functs():
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
      'input': [],
      'final_status': 'pending'
    }
    # Create object
    wflrun_obj = run.MetaWorkflowRun(input_wflrun)
    # Test no-change
    wflrun_obj.update_status()
    assert wflrun_obj.final_status == 'running'
    # Test update and completed
    for shard_name in wflrun_obj.runs:
        wflrun_obj.update_attribute(shard_name, 'status', 'completed')
    #end for
    wflrun_obj.update_status() #now all step are completed
    assert wflrun_obj.final_status == 'completed'
#end def

def test_update_functs_2():
    # meta-worfklow-run json
    input_wflrun = {
      'meta_workflow': 'AAID',
      'workflow_runs' : [
            {
              'name': 'Foo',
              'status': 'running',
              'shard': '0'
            },
            {
              'name': 'Bar',
              'status': 'failed',
              'shard': '0'
            },
            {
              'name': 'Poo',
              'status': 'completed',
              'shard': '0'
            }],
      'input': [],
      'final_status': 'pending'
    }
    # Create object
    wflrun_obj = run.MetaWorkflowRun(input_wflrun)
    # Test no-change
    wflrun_obj.update_status()
    assert wflrun_obj.final_status == 'failed'
    # Test update and completed
    for shard_name in wflrun_obj.runs:
        wflrun_obj.update_attribute(shard_name, 'status', 'completed')
    #end for
    wflrun_obj.update_status() #now all step are completed
    assert wflrun_obj.final_status == 'completed'
    # Test update and pending
    for shard_name in wflrun_obj.runs:
        wflrun_obj.update_attribute(shard_name, 'status', 'pending')
    #end for
    wflrun_obj.update_status() #now all step are pending
    assert wflrun_obj.final_status == 'pending'
#end def

def test_update_functs_3():
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
              'status': 'pending',
              'shard': '0'
            },
            {
              'name': 'Poo',
              'status': 'completed',
              'shard': '0'
            }],
      'input': [],
      'final_status': 'pending'
    }
    # Create object
    wflrun_obj = run.MetaWorkflowRun(input_wflrun)
    # Test update and inactive
    wflrun_obj.update_status()
    assert wflrun_obj.final_status == 'inactive'
#end def
