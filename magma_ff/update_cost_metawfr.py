#!/usr/bin/env python3

################################################
#
#   Function to compute and patch the cost
#         of a meta-workflow-run
#
################################################

################################################
#   Libraries
################################################
import sys, os

# magma
from magma_ff.metawflrun import MetaWorkflowRun

# dcicutils
from dcicutils import ff_utils

################################################
#   Functions
################################################
################################################
#   update_cost_metawfr
################################################
def update_cost_metawfr(metawfr_uuid, ff_key, verbose=False):
    """
            metawfr_uuid, uuid for meta-workflow-run to update costs
    """
    # Get meta-workflow-run json from the portal
    run_json = ff_utils.get_metadata(metawfr_uuid, add_on='frame=raw&datastore=database', key=ff_key)
    # Create MetaWorkflowRun object for meta-workflow-run
    run_obj = MetaWorkflowRun(run_json)

    cost = run_obj.update_cost()

    if cost is not None and cost > 0:
        patch_dict = {'cost':  cost}
        res_post = ff_utils.patch_metadata(patch_dict, metawfr_uuid, key=ff_key)
        if verbose:
            print(res_post)
        #end if
    #end if
#end def
