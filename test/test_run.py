#################################################################
#   Libraries
#################################################################
import sys, os
import pytest
import json

from magma import run

#################################################################
#   Tests
#################################################################
def test_update_functs():
    # meta-worfklow-run json
    input_wflrun = {
      'meta_workflow_uuid': 'AAID',
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
      'status': 'pending'
    }
    # Create object
    wflrun_obj = run.MetaWorkflowRun(input_wflrun)
    # Test no-change
    wflrun_obj.update_status()
    assert wflrun_obj.status == 'pending'
    # Test update and completed
    for shard_name in wflrun_obj.runs:
        wflrun_obj.update_attribute(shard_name, 'status', 'completed')
    #end for
    wflrun_obj.update_status() #now all step are completed
    assert wflrun_obj.status == 'completed'
#end def
