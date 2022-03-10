################################################
#
#   Function to import from old
#       meta-workflow-run
#
################################################
from dcicutils import ff_utils

from magma.metawflrun import MetaWorkflowRun
from magma_ff.create_metawfr import MetaWorkflowRunFromSampleProcessing
from magma_ff.runupdate import RunUpdate


################################################
#   Functions
################################################
def import_metawfr(
    meta_workflow_uuid,
    metawfr_uuid,
    sample_processing_uuid,
    steps_name,
    ff_key,
    post=False,
    verbose=False,
    expect_family_structure=True,
):
    """Create new MetaWorkflowRun from an existing one by utilizing the
    designated existing, completed steps.

    Note: We expect meta_workflow_run_creation_class to have attribute
    meta_workflow_run and accept the given args/kwargs.

    :param meta_workflow_uuid: UUID of MetaWorkflow for new
        MetaWorkflowRun
    :type meta_workflow_uuid: str
    :param metawfr_uuid: UUID of existing MetaWorkflowRun
    :type metawfr_uuid: str
    :param sample_processing_uuid: UUID of SampleProcessing for new
        MetaWorkflowRun (& presumably existing one as well)
    :type sample_processing_uuid: str
    :param steps_name: The Workflow names to reuse from the existing
        MetaWorkflowRun
    :type steps_name: list(str)
    :param ff_key: Fourfront authorization key
    :type ff_key: dict
    :param post: Whether to POST new MetaWorkflowRun
    :type post: bool
    :param verbose: Whether to print the POST response
    :type verbose: bool
    :param expect_family_structure: Whether a family structure is
        expected for the SampleProcessing's Samples
    :type expect_family_structure: bool
    :returns: Updated MetaWorkflowRun properties
    :rtype: dict
    """
    run_json_to_import = ff_utils.get_metadata(
        metawfr_uuid, add_on="frame=raw", key=ff_key
    )
    run_obj_to_import = MetaWorkflowRun(run_json_to_import)
    new_meta_workflow_run = MetaWorkflowRunFromSampleProcessing(
        sample_processing_uuid,
        meta_workflow_uuid,
        ff_key,
        expect_family_structure=expect_family_structure,
    ).meta_workflow_run
    run_obj = MetaWorkflowRun(new_meta_workflow_run)
    runupd_obj = RunUpdate(run_obj)
    run_json_updated = runupd_obj.import_steps(run_obj_to_import, steps_name)
    if post:
        res_post = ff_utils.post_metadata(
            run_json_updated, "MetaWorkflowRun", key=ff_key
        )
        if verbose:
            print(res_post)
    return run_json_updated
