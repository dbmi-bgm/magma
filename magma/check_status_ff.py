# dcicutils wrapper
from .check_status import AbstractCheckStatus
from .ff_wfr_utils import FFWfrUtils


class CheckStatusFF(AbstractCheckStatus):
    """
    CheckStatus Class for CGAP/4dn Portal (tibanna-ff-based)
    """

    def __init__(self, wflrun_obj, env=None):
        """

                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
                env, portal env name e.g. fourfront-cgap (required to actually check status)
        """
        super().__init__(wflrun_obj)

        # portal-related attributes
        self._env = env
        # cache for FFWfrUtils object
        self._ff = None
    #end def

    @property
    def status_map(self):
        """Mapping from get_status output (e.g. portal WFR run status) to Magma status"""
        return {
            'started': 'running',
            'complete': 'completed',
            'error': 'failed'
        }

    # the following three functions are for portal (cgap / 4dn)
    def get_status(self, jobid):
        return self.ff.wfr_run_status(jobid)

    def get_output(self, jobid):
        return self.ff.get_minimal_processed_output(jobid)

    @property
    def ff(self):
        """internal property used for get_status, get_output for portal
        """
        if not self._ff:
            self._ff = FFWfrUtils(self._env)
        return self._ff
#end class
