#!/usr/bin/env python3

################################################
#   Libraries
################################################
from dcicutils import ff_utils

from magma_ff.checkstatus import CheckStatusRunHandlerFF
from magma_ff.utils import check_status

################################################
#   Status Function: 
#   Checks & patches status of MWFR in run handler
################################################
def status_metawfr_handler(
    metawfr_handler_uuid, 
    auth_key,
    env="fourfront-cgap",  
    verbose=False, 
    valid_status=None
):
    perform_action = True
    #TODO: what's good with the add_on here
    run_handler_json = ff_utils.get_metadata(
        metawfr_handler_uuid, add_on="frame=raw&datastore=database", key=auth_key
    )
    perform_action = check_status(run_handler_json, valid_status)
    if perform_action:
        patch_dict = None
        handler_status_check_obj = CheckStatusRunHandlerFF(run_handler_json, env)
        
        # get list of all updates and isolate most recent update
        status_updates = list(handler_status_check_obj.check_running_mwfr_steps())
        if status_updates:
            patch_dict = status_updates[-1]

        if patch_dict:
            response_from_patch = ff_utils.patch_metadata(patch_dict, metawfr_handler_uuid, key=auth_key)
            if verbose:
                print(response_from_patch)