#!/usr/bin/env python3

################################################
#
#   Functions to reset workflow-runs status
#       in meta-workflow-run
#
################################################

################################################
#   Libraries
################################################
import sys, os

# magma
from magma_ff.metawflrun import MetaWorkflowRun
from magma_ff.runupdate import RunUpdate

# dcicutils
from dcicutils import ff_utils

################################################
#   Functions
################################################
################################################
#   reset_steps
################################################
def reset_steps(metawfr_uuid, steps_name, ff_key, verbose=False):
    """
            metawfr_uuid, uuid for meta-workflow-run
            steps_name, list of names for step-workflows that need to be reset
    """
    # Get meta-workflow-run json from the portal
    run_json = ff_utils.get_metadata(metawfr_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
    # Create MetaWorkflowRun object for meta-workflow-run
    run_obj = MetaWorkflowRun(run_json)

    # Create RunUpdate object for new meta-workflow-run
    runupd_obj = RunUpdate(run_obj)

    # Reset steps
    patch_dict = runupd_obj.reset_steps(steps_name)

    # Patch
    res_post = ff_utils.patch_metadata(patch_dict, metawfr_uuid, key=ff_key)
    if verbose:
        print(res_post)
    #end if
#end def

################################################
#   reset_shards
################################################
def reset_shards(metawfr_uuid, shards_name, ff_key, verbose=False):
    """
            metawfr_uuid, uuid for meta-workflow-run
            shards_name, list of names for workflow-runs that need to be reset
    """
    # Get meta-workflow-run json from the portal
    run_json = ff_utils.get_metadata(metawfr_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
    # Create MetaWorkflowRun object for meta-workflow-run
    run_obj = MetaWorkflowRun(run_json)

    # Create RunUpdate object for new meta-workflow-run
    runupd_obj = RunUpdate(run_obj)

    # Reset shards
    patch_dict = runupd_obj.reset_shards(shards_name)

    # Patch
    res_post = ff_utils.patch_metadata(patch_dict, metawfr_uuid, key=ff_key)
    if verbose:
        print(res_post)
    #end if
#end def

################################################
#   reset_status
################################################
def reset_status(metawfr_uuid, status, step_name, ff_key, verbose=False):
    """
            metawfr_uuid, uuid for meta-workflow-run
            status, status or list of status to reset
            step_name, name or list of names for step-workflows that need to be reset
    """
    # Make status as list if it is string
    if isinstance(status, str):
        status = [status]
    #end if
    if isinstance(step_name, str):
        step_name = [step_name]
    #end if

    # Get meta-workflow-run json from the portal
    run_json = ff_utils.get_metadata(metawfr_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
    # Create MetaWorkflowRun object for meta-workflow-run
    run_obj = MetaWorkflowRun(run_json)

    # Get shards to reset
    to_reset = []
    for shard_name, obj in run_obj.runs.items():
        if obj.status in status and obj.name in step_name:
            to_reset.append(shard_name)
        #end if
    #end for

    # Create RunUpdate object for new meta-workflow-run
    runupd_obj = RunUpdate(run_obj)

    # Reset shards
    patch_dict = runupd_obj.reset_shards(to_reset)

    # Patch
    res_post = ff_utils.patch_metadata(patch_dict, metawfr_uuid, key=ff_key)
    if verbose:
        print(res_post)
    #end if
#end def
