#!/usr/bin/env python3

################################################
#
#   Function to check and patch status for running
#       workflow-runs in meta-workflow-run
#
################################################

################################################
#   Libraries
################################################
import sys, os

# magma
from magma_ff.metawflrun import MetaWorkflowRun
from magma_ff import checkstatus

# dcicutils
from dcicutils import ff_utils

################################################
#   Functions
################################################
################################################
#   status_metawfr
################################################
def status_metawfr(metawfr_uuid, ff_key, verbose=False, env='fourfront-cgap'):
    """
            metawfr_uuid, uuid for meta-workflow-run to check status
    """
    # Get meta-workflow-run json from the portal
    run_json = ff_utils.get_metadata(metawfr_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
    # Create MetaWorkflowRun object for meta-workflow-run
    run_obj = MetaWorkflowRun(run_json)

    # Create CheckStatusFF object
    cs_obj = checkstatus.CheckStatusFF(run_obj, env)

    # Create generator to patch_dict for jobs that need update status
    #   if job still running return None
    # Patch jobs
    for patch_dict in cs_obj.check_running():
        if patch_dict:
            res_post = ff_utils.patch_metadata(patch_dict, metawfr_uuid, key=ff_key)
            if verbose:
                print(res_post)
            #end if
        #end if
    #end for
#end def
