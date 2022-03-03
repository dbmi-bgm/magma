#!/usr/bin/env python3

################################################
#
#   Function to check and patch status for running
#       workflow-runs in meta-workflow-run
#
################################################
from magma_ff.metawflrun import MetaWorkflowRun
from magma_ff import checkstatus
from dcicutils import ff_utils

from .utils import make_embed_request


################################################
#   Functions
################################################
def status_metawfr(metawfr_uuid, ff_key, verbose=False, env='fourfront-cgap'):
    """
            metawfr_uuid, uuid for meta-workflow-run to check status
    """
    status_patch_body = None
    run_json = ff_utils.get_metadata(
        metawfr_uuid, add_on='frame=raw&datastore=database', key=ff_key
    )
    ignore_quality_metrics = run_json.get("ignore_output_quality_metrics")
    run_obj = MetaWorkflowRun(run_json)
    cs_obj = checkstatus.CheckStatusFF(run_obj, env)
    status_updates = list(cs_obj.check_running())  # Get all updates
    if status_updates:
        status_patch_body = status_updates[-1]  # Take most updated
    if status_patch_body:
        if not ignore_quality_metrics:
            updated_workflow_runs = get_recently_completed_workflow_runs(
                run_json, status_patch_body
            )
            quality_metrics_passing = evaluate_quality_metrics(updated_workflow_runs, ff_key)
            if not quality_metrics_passing:
                status_patch_body["final_status"] = "quality metric failure"
        patch_response = ff_utils.patch_metadata(
            status_patch_body, metawfr_uuid, key=ff_key
        )
        if verbose:
            print(patch_response)


def get_recently_completed_workflow_runs(meta_workflow_run, patch_body):
    """"""
    result = []
    original_workflow_runs = meta_workflow_run.get("workflow_runs", [])
    updated_workflow_runs = patch_body.get("workflow_runs", [])
    if len(original_workflow_runs) != len(updated_workflow_runs):
        raise Exception(
            "Workflow run length unexpectedly changed during status update."
            "\nOriginal: %s\nUpdated: %s"
            % (original_workflow_runs, updated_workflow_runs)
        )
    for idx in range(len(original_workflow_runs)):
        original_workflow_run = original_workflow_runs[idx]
        updated_workflow_run = updated_workflow_runs[idx]
        original_status = original_workflow_run.get("status")
        updated_status = updated_workflow_run.get("status")
        if updated_status != original_status and updated_status == "completed":
            workflow_run_item = updated_workflow_run.get("workflow_run")
            if workflow_run_item:
                result.append(workflow_run_item)
    return result


def evaluate_quality_metrics(workflow_runs_to_check, ff_key):
    """"""
    result = True
    embed_fields = [
        "output_files.value_qc.overall_quality_status"
    ]
    embed_response = make_embed_request(workflow_runs_to_check, embed_fields, ff_key)
    for workflow_run in embed_response:
        quality_metrics_failed = evaluate_workflow_run_quality_metrics(workflow_run)
        if quality_metrics_failed:
            result = False
            break
    return result


def evaluate_workflow_run_quality_metrics(workflow_run):
    """"""
    result = False
    output_files = workflow_run.get("output_files", [])
    for output_file in output_files:
        quality_metric = output_file.get("value_qc", {})
        if quality_metric:
            quality_metric_status = quality_metric.get("overall_quality_status")
            if quality_metric_status == "FAIL":
                result = True
                break
    return result
