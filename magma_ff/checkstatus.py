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
#TODO: not using an abstract class 
class CheckStatusRunHandlerFF(object):
    """
    Customized CheckStatus class for MetaWorkflow Run Handler for the CGAP portal.
    """

    def __init__(self, mwfr_handler_obj, env=None):
        """
        Initialize object and attributes.

        :param mwfr_handler_obj: MetaWorkflowRunHandler[obj] representing a MetaWorkflowRunHandler[json]
        :type mwfr_handler_obj: object
        :param env: Name of the environment to use (e.g. fourfront-cgap)
        :type env: str
        """
        # Basic attributes
        self.mwfr_handler_obj = mwfr_handler_obj

        # Used for searching CGAP portal-related attributes
        self._env = env
        # For FFMetaWfrUtils object, to search CGAP portal-related attributes
        self._ff = None

    @property
    def status_map(self):
        """Mapping from get_status output to magma status.
        Set to property so that inherited classes can overwrite it.
        """
        return {
            'pending': 'pending',
            'running': 'running',
            'completed': 'completed',
            'failed' : 'failed'
        }

    # @property
    # def status_map(self):
    #     """Mapping from get_status output to magma status.
    #     """
    #     return {
    #         'started': 'running',
    #         'complete': 'completed',
    #         'error': 'failed'
    #     }

    #         return {
#             'pending': 'pending',
#             'running': 'running',
#             'completed': 'completed',
#             'failed' : 'failed'
#         }

# "pending",
#                 "running",
#                 "completed",
#                 "failed",
#         //        "inactive",
#                 "stopped",
#          //       "quality metric failed"

# Handler"pending",
#                 "running",
#                 "completed",
#                 "failed",
#                 "stopped"

    def check_running_mwfr_steps(self):
        """
        Check the currently running MetaWorkflowRun steps and update
        statuses accordingly.
        """
        # Iterate through list of running MetaWorkflow Run steps (array of objects)
        for mwfr_step in self.mwfr_handler_obj.running_steps():

            # Check current status from MWF run name
            status_ = self.get_status(run_obj.jobid)
            status = self.status_map[status_]

            # Update run status no matter what
            self.wflrun_obj.update_attribute(run_obj.shard_name, 'status', status)

            # Get run uuid
            run_uuid = self.get_uuid(run_obj.jobid)

            # Update run uuid regardless of the status
            if run_uuid:  # some failed runs don't have run uuid
                self.wflrun_obj.update_attribute(run_obj.shard_name, 'workflow_run', run_uuid)

            if status == 'completed':

                # Get formatted output
                output = self.get_output(run_obj.jobid)

                # Update output
                if output:
                    self.wflrun_obj.update_attribute(run_obj.shard_name, 'output', output)

            elif status == 'running':
                yield None  # yield None so that it doesn't terminate iteration
                continue
            else:  # failed
                # handle error status - anything to do before yielding the updated json
                self.handle_error(run_obj)
            #end if

            # Return the json to patch workflow_runs for both completed and failed
            #   and keep going so that it can continue updating status for other runs
            yield {'final_status':  self.wflrun_obj.update_status(),
                   'workflow_runs': self.wflrun_obj.runs_to_json()}

        for patch_dict in super().check_running():
            if patch_dict:
                failed_jobs = self.wflrun_obj.update_failed_jobs()
                if len(failed_jobs) > 0:
                    patch_dict['failed_jobs'] = failed_jobs
                cost = self.wflrun_obj.update_cost()
                if cost is not None and cost > 0:
                    patch_dict['cost'] = cost
                yield patch_dict

    def get_status(self, jobid):
        """
        Returns the status of the given MetaWorkflow Run, from CGAP portal
        """
        return self.ff.wfr_run_status(jobid)

    @property
    def ff(self):
        """
        Internal property used for get_status from CGAP portal for given MetaWorkflow Run
        """
        if not self._ff:
            self._ff = FFMetaWfrUtils(self._env)
        return self._ff