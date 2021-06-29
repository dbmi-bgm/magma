#!/usr/bin/env python3

################################################
#
#   Function to run meta-workflow-run
#       with tibanna and patch metadata
#
################################################

################################################
#   Libraries
################################################
import sys, os

# magma
from magma_ff.metawfl import MetaWorkflow
from magma_ff.metawflrun import MetaWorkflowRun
from magma_ff import inputgenerator as ingen

# dcicutils
from dcicutils import ff_utils
from tibanna_ffcommon.core import API

################################################
#   Functions
################################################
################################################
#   run_metawfr
################################################
def run_metawfr(metawfr_uuid, ff_key, verbose=False, sfn='tibanna_zebra', env='fourfront-cgap', maxcount=None):
    """
            metawfr_uuid, uuid for meta-workflow-run to run
    """
    # Get meta-workflow-run json from the portal
    run_json = ff_utils.get_metadata(metawfr_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
    # Create MetaWorkflowRun object for meta-workflow-run
    run_obj = MetaWorkflowRun(run_json)

    # Get meta-workflow json from the portal
    metawf_uuid = run_json['meta_workflow']
    wfl_json = ff_utils.get_metadata(metawf_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
    # Create MetaWorkflow object for meta-workflow
    wfl_obj = MetaWorkflow(wfl_json)

    # Create InputGenerator object
    ingen_obj = ingen.InputGenerator(wfl_obj, run_obj)

    # Create generator to (input_json, patch_dict) for job that needs to run
    #   patch_dict
    #       {'final_status':  'status',
    #        'workflow_runs': [{wflrun}, ...]}
    in_gen = ingen_obj.input_generator(env)

    # Start run and patch
    count = 0
    for input_json, patch_dict in in_gen:
        # Start tibanna run
        API().run_workflow(input_json=input_json, sfn=sfn)
        # Patch
        res_post = ff_utils.patch_metadata(patch_dict, metawfr_uuid, key=ff_key)
        if verbose:
            print(res_post)
        #end if
        count += 1
        if maxcount and count >= maxcount:
            break
    #end for
#end def
