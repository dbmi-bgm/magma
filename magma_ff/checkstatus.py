#!/usr/bin/env python3

################################################
#
#   dcicutils wrapper
#
################################################

################################################
#   Libraries
################################################

# magma
from magma.checkstatus import AbstractCheckStatus
from magma_ff.wfrutils import FFWfrUtils, FFMetaWfrUtils
from magma_ff.metawflrun_handler import MetaWorkflowRunHandler

################################################
#   CheckStatusFF
################################################
class CheckStatusFF(AbstractCheckStatus):
    """Customized CheckStatus class for the portal.
    """

    def __init__(self, wflrun_obj, env=None):
        """Initialize the object and set all attributes.

        :param wflrun_obj: MetaWorkflowRun[obj]
        :type wflrun_obj: object
        :param env: Name of the environment to use (e.g. fourfront-cgap)
        :type env: str
        """
        super().__init__(wflrun_obj)

        # Portal-related attributes
        self._env = env
        # For FFWfrUtils object
        self._ff = None
    #end def

    @property
    def status_map(self):
        """Mapping from get_status output to magma status.
        """
        return {
            'started': 'running',
            'complete': 'completed',
            'error': 'failed'
        }

    def check_running(self):
        """
        """
        for patch_dict in super().check_running():
            if patch_dict:
                failed_jobs = self.wflrun_obj.update_failed_jobs()
                if len(failed_jobs) > 0:
                    patch_dict['failed_jobs'] = failed_jobs
                cost = self.wflrun_obj.update_cost()
                if cost is not None and cost > 0:
                    patch_dict['cost'] = cost
                yield patch_dict

    # The following three functions are for portal (cgap / 4dn)
    def get_uuid(self, jobid):
        """
        """
        return self._ff.wfr_run_uuid(jobid)

    def get_status(self, jobid):
        """
        """
        return self._ff.wfr_run_status(jobid)

    def get_output(self, jobid):
        """
        """
        return self._ff.get_minimal_processed_output(jobid)

    @property
    def ff(self):
        """Internal property used for get_status, get_output for portal.
        """
        if not self._ff:
            self._ff = FFWfrUtils(self._env)
        return self._ff

#end class


################################################
#   CheckStatusRunHandlerFF
################################################
#TODO: not using an abstract class -- will check on this later
class CheckStatusRunHandlerFF(object):
    """
    Customized CheckStatus class for MetaWorkflow Run Handler for the CGAP portal.
    """

    def __init__(self, mwfr_handler_input_dict, env=None):
        """
        Initialize object and attributes.

        :param mwfr_handler_input_dict: MetaWorkflowRunHandler input dict
        :type mwfr_handler_input_dict: dict
        :param env: Name of the environment to use (e.g. fourfront-cgap)
        :type env: str
        """
        # Basic attributes
        #TODO: may do this outside of this class for consistency
        self.mwfr_handler_obj = MetaWorkflowRunHandler(mwfr_handler_input_dict)

        # Used for searching CGAP portal-related attributes
        self._env = env

        # For FFMetaWfrUtils object
        self._ff = FFMetaWfrUtils(self._env)

    @property
    def status_map(self):
        """
        Mapping from MWFR portal final_status output to magma final_status.
        """
        return {
            "pending": "pending",
            "running": "running",
            "completed": "completed",
            "failed": "failed",
            "inactive": "pending",
            "stopped": "stopped",
            "quality metric failed": "failed"
        }


    def check_running_mwfr_steps(self):
        """
        Check the currently running MetaWorkflowRun steps and update
        statuses accordingly.
        Returns a generator. clever.
        """
        # Iterate through list of running MetaWorkflow Run steps (array of objects)
        for running_mwfr_step_name in self.mwfr_handler_obj.running_steps():

            # Get run uuid
            run_uuid = self.mwfr_handler_obj.get_step_attr(running_mwfr_step_name, uuid)

            # Check current status from MWF run name
            status = self.status_map[self.get_mwfr_status(run_uuid)]

            # Update run status no matter what
            self.mwfr_handler_obj.update_meta_workflow_run_step(running_mwfr_step_name, "status",  status)

            # Update run uuid regardless of the status
            # if run_uuid:  # some failed runs don't have run uuid
            #     self.wflrun_obj.update_attribute(run_obj.shard_name, 'workflow_run', run_uuid)
            # TODO: what's good w a mwfr that failed and may not have uuid??


            if status == 'running':
                yield None  # yield None so that it doesn't terminate iteration
                continue
            # TODO: what about when failed? add to error attr (ik originally for just creation error but still)

            # TODO: add part cost check/calculation here? tbd -- rn no, only checks running
            # but actually that may work

            # Return the json to PATCH meta_workflow_runs and final_status in handler
            yield {'final_status':  self.mwfr_handler_obj.update_final_status(),
                   'meta_workflow_runs': self.mwfr_handler_obj.update_meta_workflows_array()}


    def get_mwfr_status(self, mwfr_uuid):
        """
        using portal, gets final_status of given mwfr
        """
        return self._ff.get_meta_wfr_current_status(mwfr_uuid)

    def get_mwfr_cost(self, mwfr_uuid):
        """
        using portal, gets cost of given mwfr
        """
        return self._ff.get_meta_wfr_cost(mwfr_uuid)