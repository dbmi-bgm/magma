#!/usr/bin/env python3

################################################
#
#   Object to update
#       meta-workflow-run and workflow-run objects
#
#   Michele Berselli
#   berselli.michele@gmail.com
#
################################################

################################################
#   Libraries
################################################
import sys, os

################################################
#   RunUpdate
################################################
class RunUpdate(object):
    """
        object to handle MetaWorkflowRun and WorkflowRun update
    """

    def __init__(self, wflrun_obj):
        """
            initialize RunUpdate object

                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
        """
        # Basic attributes
        self.wflrun_obj = wflrun_obj
    #end def

    def reset_steps(self, steps_name):
        """
            reset WorkflowRun objects with name in steps_name
            return updated workflow-runs and final_status information as json

                steps_name, list of names for step-workflows that need to be reset
        """
        for name in steps_name:
            self.wflrun_obj.reset_step(name)
        #end for
        return {'final_status':  self.wflrun_obj.update_status(),
                'workflow_runs': self.wflrun_obj.runs_to_json()}
    #end def

    def reset_shards(self, shards_name):
        """
            reset WorkflowRun objects with shard_name in shards_name
            return updated workflow-runs and final_status information as json

                shards_name, list of names for workflow-runs that need to be reset
        """
        for name in shards_name:
            self.wflrun_obj.reset_shard(name)
        #end for
        return {'final_status':  self.wflrun_obj.update_status(),
                'workflow_runs': self.wflrun_obj.runs_to_json()}
    #end def

    def import_steps(self, wflrun_obj, steps_name, import_input=True):
        """
            update current MetaWorkflowRun object information
            import and use information from specified wflrun_obj
            update WorkflowRun objects up to steps specified by steps_name
            return updated meta-workflow-run as json

                wflrun_obj, MetaWorkflowRun object to get information from
                steps_name, list of names for step-workflows to fill in information from wflrun_obj
        """
        ## Import input
        if import_input:
            self.wflrun_obj.input = wflrun_obj.input
        #end if
        ## Import WorkflowRun objects
        for name in steps_name:
            queue = [] # queue of steps to import
                       #    name step and its dependencies
            # Get workflow-runs corresponding to name step
            for shard_name, run_obj in self.wflrun_obj.runs.items():
                if name == shard_name.split(':')[0]:
                    queue.append(run_obj)
                #end if
            #end for
            # Iterate queue, get dependencies and import workflow-runs
            while queue:
                run_obj = queue.pop(0)
                shard_name = run_obj.shard_name
                dependencies = run_obj.dependencies
                try:
                    self.wflrun_obj.runs[shard_name] = wflrun_obj.runs[shard_name]
                except KeyError as e:
                    # raise ValueError('JSON content error, missing information for workflow-run "{0}"\n'
                    #                     .format(e.args[0]))
                    continue
                #end try
                for dependency in dependencies:
                    queue.append(self.wflrun_obj.runs[dependency])
                #end for
            #end while
        #end for
        # Update final_status
        self.wflrun_obj.update_status()

        return self.wflrun_obj.to_json()
    #end def

#end class
