from magma_ff.wfrutils import FFWfrUtils, FFMetaWfrUtils
from magma_ff.utils import JsonObject
from magma_ff.metawflrun_handler import MetaWorkflowRunHandler
from magma.checkstatus import AbstractCheckStatus

from typing import List, Dict, Union, Any, Optional
from functools import cached_property

from magma.magma_constants import *


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
        return self.ff.wfr_run_uuid(jobid)

    def get_status(self, jobid):
        """
        """
        return self.ff.wfr_run_status(jobid)

    def get_output(self, jobid):
        """
        """
        return self.ff.get_minimal_processed_output(jobid)

    @property
    def ff(self):
        """Internal property used for get_status, get_output for portal.
        """
        if not self._ff:
            self._ff = FFWfrUtils(self._env)
        return self._ff

#end class



class CheckStatusRunHandlerFF(object):
    """
    Customized CheckStatus class for MetaWorkflow Run Handler from the CGAP portal.
    """

    def __init__(self, meta_workflow_run_handler: JsonObject, auth_key: JsonObject) -> None:
        """
        Initialize CheckStatusRunHandlerFF object.

        :param meta_workflow_run_handler: MetaWorkflowRunHandler input dict
        :param auth_key: Authorization keys for C4 account
        """
        self.meta_workflow_run_handler = meta_workflow_run_handler
        self.auth_key = auth_key


    def update_running_steps(self) -> Optional[Dict[str, Union[str, List[Any]]]]:
        """
        For each running MetaWorkflow Run Step:
        - updates status of that MetaWorkflow Run Step to its current portal output
        - generates updated meta_workflow_runs array and final_status (of handler)
            for MetaWorkflowRunHandler instance, yielded as
            {final_status, meta_workflow_runs} for PATCHing
        """
        # Iterate through list of running MetaWorkflow Run steps (array of objects)
        for running_step_name in self.handler.running_steps():

            # Get run uuid
            run_step_uuid = self.handler.get_meta_workflow_run_step_attr(running_step_name, UUID)

            # Check current status of this MetaWorkflow Run step
            curr_status = self.get_meta_workflow_run_step_status(run_step_uuid)

            # TODO: is there any case where a uuid of a "running" step doesn't exist?
            # I don't think so but check with Doug

            # TODO: is there any way to catch traceback from Tibanna of a failed job?
            # if so, can add attr to run handler schema to save these, otherwise it is
            # manually searched/inspected (I imagine it is the latter)

            # TODO: worry about other attrs at all (like uuid?)

            if curr_status == RUNNING:
                yield None  # yield None so iteration isn't terminated
                continue

            # Update run status
            self.handler.update_meta_workflow_run_step_obj(running_step_name, STATUS, curr_status)

            # Return the json to PATCH meta_workflow_runs and final_status in handler
            yield {FINAL_STATUS: self.handler.update_final_status(),
                   META_WORKFLOW_RUNS: self.handler.update_meta_workflow_runs_array()}

    def updated_run_handler_cost(self) -> Dict[str, float]:
        """
        For each running MetaWorkflow Run Step:
        - retrieve its Tibanna cost from CGAP portal. Returns 0 if it doesn't have this attribute
        - add this step's cost to the overall run handler cost
        - once loop is completed, generates updated cost for MetaWorkflowRunHandler instance,
            yielded as a dict for PATCHing on CGAP portal
        """
        curr_cost = float(0)
        for run_step_name in self.handler.meta_workflow_run_steps_dict:
            # Get run uuid
            run_step_uuid = self.handler.get_meta_workflow_run_step_attr(run_step_name, UUID)
            # Get its cost and add to overall handler cost
            run_step_cost = self.portal_run_attr_getter.get_meta_workflow_run_cost(run_step_uuid)
            curr_cost += run_step_cost
        # Return the json to PATCH cost attribute in handler
        return {COST: curr_cost}
        # TODO: is there actually any case where we don't need to check non-running
        # steps for cost? other than when initializing cost of a newly created handler to 0...

    def get_meta_workflow_run_step_status(self, meta_workflow_run_identifier: str) -> str:
        """
        Using the CGAP portal, gets the current status of given MetaWorkflow Run step.

        :param meta_workflow_run_identifier: Identifier (e.g. UUID, @id) for
            MetaWorkflow Run to be searched
        :return: the status of the specified MetaWorkflow Run
        """
        current_status = self.portal_run_attr_getter.get_meta_workflow_run_status(meta_workflow_run_identifier)
        return self.run_status_mapping[current_status]

    @property
    def run_status_mapping(self) -> dict:
        """
        Mapping from possible CGAP portal final_status value for a MetaWorkflow Run,
        to possible status values for a MetaWorkflow Run step within a Run Handler,
        according to CGAP schema for a Run Handler.
        """
        #TODO: add this to constants
        return {
            PENDING: PENDING,
            RUNNING: RUNNING,
            COMPLETED: COMPLETED,
            FAILED: FAILED,
            INACTIVE: PENDING,
            STOPPED: STOPPED,
            QC_FAIL: FAILED
        }

    @cached_property
    def portal_run_attr_getter(self):
        """Used for accessing status and cost attributes of MetaWorkflow Runs from CGAP portal."""
        return FFMetaWfrUtils(self.auth_key)

    @cached_property
    def handler(self):
        """Using JSON object of Run Handler from CGAP portal, create magma_ff MetaWorkflowRunHandler instance."""
        return MetaWorkflowRunHandler(self.meta_workflow_run_handler)