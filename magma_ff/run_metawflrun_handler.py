from typing import Optional, List, Dict, Union, Any
from functools import cached_property
from dcicutils import ff_utils

from magma_ff.metawflrun_handler import MetaWorkflowRunHandler
from magma_ff.utils import JsonObject, make_embed_request, check_status
from magma_ff.create_metawfr import (
    create_meta_workflow_run,
    MetaWorkflowRunCreationError,
)

from magma.magma_constants import *


def execute_metawflrun_handler(
    meta_workflow_run_handler_id: str,
    auth_key: JsonObject,
    valid_final_status: Optional[List[str]] = None,
    verbose: bool = False,
) -> None:
    """
    Wrapper function to ExecuteMetaWorkflowRunHandler class method calls.
    Executes the Run Handler for the given MetaWorkflow Run Handler ID.
    Checks pending MetaWorkflow Run steps and, if dependencies are completed,
    creates a corresponding MetaWorkflow Run, updates the step's status
    and handler status, and PATCHes to CGAP portal.

    :param meta_workflow_run_handler_id: Identifier for the MetaWorkflow Run Handler
        (e.g. UUID, @id) to be executed
    :param auth_key: Authorization keys for C4 account
    :param verbose: Whether to print the PATCH response(s)
    :param valid_final_status: Run Handler final status(es) considered valid in CGAP portal
    """
    # Retrieve Run Handler portal JSON from CGAP portal
    fields_to_embed = [
        "*",
        "meta_workflow_runs.*",
    ]  # TODO: double check this with integrated testing
    meta_workflow_run_handler = make_embed_request(
        meta_workflow_run_handler_id, fields_to_embed, auth_key, single_item=True
    )
    if not meta_workflow_run_handler:
        raise ValueError(
            f"No MetaWorkflow Run Handler found for given identifier: {meta_workflow_run_handler_id}"
        )

    # Check that status of Run Handler retrieved is valid
    perform_action = check_status(meta_workflow_run_handler, valid_final_status)

    # Start executing this Run Handler is its status is considered valid, PATCHing MWFRs as they're created
    if perform_action:
        newly_running_meta_workflow_runs = ExecuteMetaWorkflowRunHandler(
            meta_workflow_run_handler, auth_key
        ).generator_of_created_meta_workflow_run_steps()
        for patch_dict in newly_running_meta_workflow_runs:
            response_from_patch = ff_utils.patch_metadata(
                patch_dict, meta_workflow_run_handler_id, key=auth_key
            )
            if verbose:
                print(response_from_patch)
                # TODO: add patch to the associated item list of metaworkflow runs?


class ExecuteMetaWorkflowRunHandler:
    """
    Class that generates updated dictionaries for PATCHing a MetaWorkflow Run Handler,
    as each MetaWorkflow Run Step is executed in order, based on user-defined dependencies.
    """

    def __init__(
        self, meta_workflow_run_handler: JsonObject, auth_key: JsonObject
    ) -> None:
        """
        Initialize the ExecuteMetaWorkflowRunHandler object, set basic attributes.

        :param meta_workflow_run_handler: JSON object of MetaWorkflowRun Handler,
            retrieved from CGAP portal
        :param auth_key: Portal authorization key
        """
        self.auth_key = auth_key
        self.meta_workflow_run_handler = meta_workflow_run_handler

    def generator_of_created_meta_workflow_run_steps(
        self,
    ) -> Dict[str, Union[str, List[Any]]]:
        """
        For each pending (ready to run) MetaWorkflow Run Step, if all dependencies are complete:
        - updates status of that MetaWorkflow Run Step to "running"
        - creates a corresponding MetaWorkflow Run
        - generates updated meta_workflow_runs array and final_status
            for MetaWorkflowRunHandler instance, yielded as
            {final_status, meta_workflow_runs} for PATCHing
        """
        # going through all steps that are ready to run (pending)
        for pending_meta_workflow_run_name in self.handler.pending_steps():
            # current_pending_meta_workflow_run_step = self.meta_workflow_run_handler_instance.retrieve_meta_workflow_run_step_obj_by_name(pending_meta_workflow_run_name)

            dependencies_completed = self._check_pending_step_dependencies(
                pending_meta_workflow_run_name
            )

            # if all dependencies for this pending step have run to completion
            if dependencies_completed:
                # Create this MetaWorkflow Run and POST to portal
                # set this step's status to running too
                self._create_and_update_meta_workflow_run_step(
                    pending_meta_workflow_run_name
                )

            # update final status & meta_workflow_runs array of the handler, yield for PATCHING
            yield {
                FINAL_STATUS: self.handler.update_final_status(),
                META_WORKFLOW_RUNS: self.handler.update_meta_workflow_runs_array(),
            }

    def _check_pending_step_dependencies(self, pending_step_name: str) -> bool:
        """
        Given the name of a pending MetaWorkflowRun Step, check if all the Run Steps it is
        dependent on are completed.

        :param pending_step_name: name of the pending MetaWorkflowRun Step
        :returns: True if all dependencies are completed, otherwise False
        """

        current_dependencies = self.handler.get_meta_workflow_run_step_attr(
            pending_step_name, DEPENDENCIES
        )

        for dependency_name in current_dependencies:
            dependency_step_status = self.handler.get_meta_workflow_run_step_attr(
                dependency_name, STATUS
            )
            if dependency_step_status != COMPLETED:
                return False

        return True

    def _create_and_update_meta_workflow_run_step(self, pending_step_name: str) -> None:
        """
        For a given pending MetaWorkflow Run step name within a Run Handler,
        create its corresponding MetaWorkflow Run and update appropriate attributes (status & MetaWorkflow Run LinkTo).
        If there is any error in creation of the Run, update the error attribute.

        :param pending_step_name: name of MetaWorkflow Run to be created and updated
        :raises MetaWorkflowRunCreationError: if the MetaWorkflow Run for the given name can't be created
        """
        try:
            # TODO: iterate through all items for creation,
            meta_workflow_run_portal_object = create_meta_workflow_run(
                self.handler.get_meta_workflow_run_step_attr(
                    pending_step_name, ITEMS_FOR_CREATION
                ),
                self.handler.get_meta_workflow_run_step_attr(
                    pending_step_name, META_WORKFLOW
                ),
                self.auth_key,
            )  # TODO: !!! have to add run_uuid attr to schema!! arrray? to match items_for_creation
            # TODO: will this be the actual output of this function or do i have to parse more?

            # update the meta_workflow_run linkTo
            self.handler.update_meta_workflow_run_step_obj(
                pending_step_name,
                META_WORKFLOW_RUN,
                meta_workflow_run_portal_object[UUID],
            )
            # update the run step's status to running
            self.handler.update_meta_workflow_run_step_obj(
                pending_step_name, STATUS, RUNNING
            )
        # if there is any error in creation of the MetaWorkflowRun
        except MetaWorkflowRunCreationError as err:
            # update error attr
            self.handler.update_meta_workflow_run_step_obj(
                pending_step_name, ERROR, str(err)
            )
            # update run step's status to failed
            self.handler.update_meta_workflow_run_step_obj(
                pending_step_name, STATUS, FAILED
            )

    @cached_property
    def handler(self):
        """Using JSON object of Run Handler from CGAP portal, create MetaWorkflowRunHandler instance."""
        return MetaWorkflowRunHandler(self.meta_workflow_run_handler)
