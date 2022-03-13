#!/usr/bin/env python3

################################################
#
#   dcicutils wrapper
#
################################################

################################################
#   Libraries
################################################
import sys, os

# magma
from magma.checkstatus import AbstractCheckStatus
from magma_ff.wfrutils import FFWfrUtils

################################################
#   CheckStatusFF
################################################
class CheckStatusFF(AbstractCheckStatus):
    """
        CheckStatus object for CGAP/4dn Portal (tibanna-ff-based)
    """

    def __init__(self, wflrun_obj, env=None):
        """

                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
                env, portal env name e.g. fourfront-cgap (required to actually check status)
        """
        super().__init__(wflrun_obj)

        # Portal-related attributes
        self._env = env
        # Cache for FFWfrUtils object
        self._ff = None
    #end def

    @property
    def status_map(self):
        """
            mapping from get_status output (e.g. portal WFR run status) to Magma status
        """
        return {
            'started': 'running',
            'complete': 'completed',
            'error': 'failed'
        }

    def check_running(self):
        for patch_dict in super().check_running():
            if patch_dict:
                failed_jobs = self.wflrun_obj.update_failed_jobs()
                if len(failed_jobs) > 0:
                    patch_dict['failed_jobs'] = failed_jobs
                # Currently, we are only calculating the cost when the MetaWorkflow completed, 
                # as computing the estimated cost for each workflow is rather expensive.
                if patch_dict['final_status'] == 'completed':
                    cost = self.wflrun_obj.update_cost()
                    if cost is not None and cost > 0:
                        patch_dict['cost'] = cost
                yield patch_dict

    # The following three functions are for portal (cgap / 4dn)
    def get_uuid(self, jobid):
        return self.ff.wfr_run_uuid(jobid)

    def get_status(self, jobid):
        return self.ff.wfr_run_status(jobid)

    def get_output(self, jobid):
        return self.ff.get_minimal_processed_output(jobid)

    @property
    def ff(self):
        """
            internal property used for get_status, get_output for portal
        """
        if not self._ff:
            self._ff = FFWfrUtils(self._env)
        return self._ff

#end class
