#!/usr/bin/env python3

################################################
#
#   MetaWorkflowRun ff
#
################################################

################################################
#   Libraries
################################################
import sys, os
import copy

# magma
from magma.metawflrun import MetaWorkflowRun as MetaWorkflowRunFromMagma
from magma_ff.parser import ParserFF

################################################
#   MetaWorkflowRun
################################################
class MetaWorkflowRun(MetaWorkflowRunFromMagma):

    def __init__(self, input_json):
        """
        """
        input_json_ = copy.deepcopy(input_json)
        ParserFF(input_json_).arguments_to_json()

        super().__init__(input_json_)
    #end def

    def _reset_run(self, shard_name):
        """
            reset attributes value for WorkflowRun object in runs

                shard_name, is the name of the workflow-run to reset
        """
        run_obj = self.runs[shard_name]
        # Reset run_obj
        run_obj.output = []
        run_obj.status = 'pending'
        if getattr(run_obj, 'jobid', None):
            delattr(run_obj, 'jobid')
        #end if
        if getattr(run_obj, 'workflow_run', None):
            delattr(run_obj, 'workflow_run')
        #end if
    #end def

#end class
