#!/usr/bin/env python3

################################################
#   Libraries
################################################
import sys, os

################################################
#   UpdateHandler
################################################
class UpdateHandler(object):
    """
    Class to handle MetaWorkflowRunHandler and MetaWorkflowRun object updates.
    """

    def __init__(self, run_handler_obj):
        """
        Constructor method.
        Initialize object and attributes.

        :param run_handler_obj: MetaWorkflowRunHandler magma object, representing a MetaWorkflowRunHandler JSON from CGAP portal
        :type run_handler_obj: object
        """
        # Basic attributes
        self.run_handler_obj = run_handler_obj

    def reset_steps(self, step_names):
        """
        Reset MetaWorkflowRun object in step_names list.

        :param step_names: List of names for MetaWorkflowRun steps to be reset
        :type step_names: list[str]
        :return: Updated meta_workflow_runs and handler final_status information
        :rtype: dict
        """
        for name in step_names:
            self.wflrun_obj.reset_step(name)

        # used later to PATCH onto the portal
        return {'final_status':  self.run_handler_obj.update_final_status(),
                'workflow_runs': self.wflrun_obj.runs_to_json()}

    # def import_steps(self, wflrun_obj, steps_name, import_input=True):
    #     """Update current MetaWorkflowRun[obj] information.
    #     Import and use information from specified wflrun_obj.
    #     Update WorkflowRun[obj] up to steps specified by steps_name

    #     :param wflrun_obj: MetaWorkflowRun[obj] to import information from
    #     :type wflrun_obj: object
    #     :param steps_name: List of names for steps to import
    #     :type steps_name: list(str)
    #     :return: MetaWorkflowRun[json]
    #     :rtype: dict
    #     """
    #     ## Import input
    #     if import_input:
    #         self.wflrun_obj.input = wflrun_obj.input
    #     ## Import WorkflowRun objects
    #     for name in steps_name:
    #         queue = [] # queue of steps to import
    #                    #    name step and its dependencies
    #         # Get workflow-runs corresponding to name step
    #         for shard_name, run_obj in self.wflrun_obj.runs.items():
    #             if name == shard_name.split(':')[0]:
    #                 queue.append(run_obj)
    #         # Iterate queue, get dependencies and import workflow-runs
    #         while queue:
    #             run_obj = queue.pop(0)
    #             shard_name = run_obj.shard_name
    #             dependencies = run_obj.dependencies
    #             try:
    #                 self.wflrun_obj.runs[shard_name] = wflrun_obj.runs[shard_name]
    #             except KeyError as e:
    #                 # raise ValueError('JSON content error, missing information for workflow-run "{0}"\n'
    #                 #                     .format(e.args[0]))
    #                 continue
    #             for dependency in dependencies:
    #                 queue.append(self.wflrun_obj.runs[dependency])
    #     # Update final_status
    #     self.wflrun_obj.update_status()

    #     return self.wflrun_obj.to_json()
