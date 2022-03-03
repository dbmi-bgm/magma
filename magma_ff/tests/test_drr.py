import json

from ..create_metawfr import MetaWorkflowRunFromSampleProcessing


def test_mwfr_creation_for_case():
    keys_file = "/Users/drioux/.cgap-keys.json"
    with open(keys_file, "r") as file_handle:
        keys = json.load(file_handle)
    key = keys["prod"]
    mwf_uuid = "GAPLAFW2J13T"
    sample_processing_uuid = "8dd51b60-2d65-4e74-ac86-08ff9159be90"
    import pdb; pdb.set_trace()
    meta_workflow_run = MetaWorkflowRunFromSampleProcessing(
        sample_processing_uuid, mwf_uuid, key
    )
    meta_workflow_run.post_and_patch()
