#!/usr/bin/env python3

################################################
#   Libraries
################################################
from dcicutils import ff_utils

# magma
# from magma_ff.metawfl_handler import MetaWorkflowHandler

from magma_ff.metawflrun_handler import MetaWorkflowRunHandler
from magma_ff.utils import make_embed_request, check_status
from magma_ff.create_metawfr import create_meta_workflow_run, MetaWorkflowRunCreationError
# from magma_ff.run_metawfr import run_metawfr

################################################
#   MetaWorkflowRunStep Generator Class
################################################
class MetaWorkflowRunStepGenerator:
    def __init__(self, mwfr_handler_input_dict, auth_key):
        self.auth_key = auth_key
        self.mwfr_handler_obj = MetaWorkflowRunHandler(mwfr_handler_input_dict)

    def run_step_generator(self):
        """this goes through pending steps
        if all dependencies are complete, creates mwfr and runs it"""
        # going through all steps that are ready to run (pending)
        for pending_mwfr_step_name in self.mwfr_handler_obj.pending_steps():
            curr_pending_step_obj = self.mwfr_handler_obj.retrieve_meta_workflow_run_step_by_name(pending_mwfr_step_name)

            # check that all dependencies are completed before running current step
            curr_dependencies = getattr(curr_pending_step_obj, "dependencies", [])
            deps_completed = True
            for dependency_name in curr_dependencies:
                dependency_step_status = self.mwfr_handler_obj.get_step_attr(dependency_name, "status")
                if dependency_step_status != "completed":
                    deps_completed = False #TODO: add break here maybe

            # if all dependencies have run to completion
            if deps_completed:
                # create the metaworkflow run
                #TODO: iterate through all items for creation,
                # and use handler method instead of getattr? error catching n all dat
                # oh a good idea is to add method to step class....
                try: 
                    meta_workflow_run_portal_obj = create_meta_workflow_run(
                        getattr(curr_pending_step_obj, "items_for_creation"),
                        getattr(curr_pending_step_obj, "meta_workflow"),
                        self.auth_key
                    ) #TODO: !!! have to add run_uuid attr to schema!! arrray? to match items_for_creation
                    
                    # update the meta_workflow_run/run_uuid linkTo
                    setattr(curr_pending_step_obj, "run_uuid", meta_workflow_run_portal_obj["uuid"])
                    # update the status to running
                    setattr(curr_pending_step_obj, "status", "running")
                except MetaWorkflowRunCreationError as err:
                    # update error attr
                    setattr(curr_pending_step_obj, "error", err)
                    # update status to failed
                    setattr(curr_pending_step_obj, "status", "failed")

                

            # update final status & mwfr array of overall handler and yield for PATCHING
            yield {'final_status':  self.mwfr_handler_obj.update_final_status(),
                    'meta_workflow_runs': self.mwfr_handler_obj.update_meta_workflows_array()}
        

################################################
#   Running Function: 
#   Calls MWFR creation/run fxns and patches handler
################################################
def run_metawflrun_handler(
    metawfr_handler_uuid,
    auth_key,
    verbose=False,
    # sfn="tibanna_zebra", #TODO: just copying -- keeps option open
    # env="fourfront-cgap",
    # maxcount=None, # TODO: remnant of run metawfr -- no limit on mwfr steps per handler? -- this 
    valid_final_status=None
):
    fields_to_embed = ["*", "meta_workflow_runs.*"] #TODO: double check this with integrated testing
    mwfr_handler_input_dict = make_embed_request(
        metawfr_handler_uuid, fields_to_embed, auth_key, single_item=True
    ) #TODO: add error check here
    perform_action = check_status(mwfr_handler_input_dict, valid_final_status)
    if perform_action:
        # this will create handler object which has checking status methods
        mwfr_step_generator = MetaWorkflowRunStepGenerator(mwfr_handler_input_dict, auth_key).run_step_generator()

        for patch_dict in mwfr_step_generator:
            response_from_patch = ff_utils.patch_metadata(patch_dict, metawfr_handler_uuid, key=auth_key)
            if verbose:
                print(response_from_patch)