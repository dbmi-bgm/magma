#!/usr/bin/env python3

################################################
#
#   Function to handle the import from old
#       meta-workflow-run
#
################################################

################################################
#   Libraries
################################################
import sys, os
import json

# magma
from magma import metawflrun as run
from magma_ff import runupdate as runupd

# dcicutils
from dcicutils import ff_utils

################################################
#   Functions
################################################
################################################
#   import_metawfr
################################################
def import_metawfr(metawf_uuid, metawfr_uuid, case_uuid, steps_name, create_metawfr, ff_key, post=False, verbose=False):
    """
            metawf_uuid, uuid for meta-workflow
            metawfr_uuid, uuid for old meta-workflow-run to import
            case_uuid, uuid for the case
            steps_name, list of names for step-workflows to import
                        all the dependencies will be included automatically
            create_metawfr, is a function to create a new meta-workflow-run from meta-workflow and case metadata
    """
    # Create new meta-workflow-run json
    run_json = create_metawfr(metawf_uuid, case_uuid, ff_key)
    # Create MetaWorkflowRun object for new meta-workflow-run
    run_obj = run.MetaWorkflowRun(run_json)

    # Get old meta-workflow-run json from the portal
    run_json_toimport = ff_utils.get_metadata(metawfr_uuid, add_on='?frame=raw', key=ff_key)
    # Create MetaWorkflowRun object for old meta-workflow-run
    run_obj_toimport = run.MetaWorkflowRun(run_json_toimport)

    # Create RunUpdate object for new meta-workflow-run
    runupd_obj = runupd.RunUpdate(run_obj)

    # Import information
    run_json_updated = runupd_obj.import_steps(run_obj_toimport, steps_name)

    # Post updated meta-workflow-run
    if post:
        res_post = ff_utils.post_metadata(run_json_updated, 'MetaWorkflowRun', key=ff_key)
        if verbose:
            print(res_post)
        #end if
    #end if
#end def
