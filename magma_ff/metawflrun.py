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
from tibanna_ffcommon.core import API

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

    def update_failed_jobs(self): 
        """
            checks status for all WorkflowRun objects
            and adds failed runs to the MetaWorflowRun object
            return failed_jobs
        """
        if not hasattr(self, 'failed_jobs'):
            self.failed_jobs = []
        #end if
        for _, run_obj in self.runs.items():
            if run_obj.status == 'failed':
                self.failed_jobs.append(run_obj.jobid)
                self.failed_jobs = list(set(self.failed_jobs)) # Remove duplicates
            #end if
        #end for
        return self.failed_jobs
    #end def

    def update_cost(self): 
        """
            computes an estimated cost of the MetaWorkflowRun. Includes failed and completed jobs.
            return cost
        """
        self.cost = 0.0
        #end if
        for jobid in self.failed_jobs:
            try:
                run_cost, _ = API().cost_estimate(job_id=jobid, force=True)
                self.cost = self.cost + run_cost
            except Exception:
                return None
        #end for
        for _, run_obj in self.runs.items():
            # only look for completed jobs as failed ones are in self.failed_jobs
            if run_obj.status == 'completed':
                try:
                    run_cost, _ = API().cost_estimate(job_id=run_obj.jobid, force=True)
                    self.cost = self.cost + run_cost
                except Exception:
                    # This can happen, if e.g. prices from AWS can't be retrieved
                    return None
            #end if
        #end for
        return self.cost
    #end def

#end class
