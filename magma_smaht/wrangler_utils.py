#!/usr/bin/env python3


import json
from pathlib import Path
from typing import Any, Dict, Sequence, Union
from packaging import version
from dcicutils import ff_utils
import pprint

from .create_metawfr import MWF_NAME_CRAM_TO_FASTQ_PAIRED_END
from .reset_metawfr import reset_failed

JsonObject = Dict[str, Any]

WF_CRAM_TO_FASTQ_PAIRED_END = "cram_to_fastq_paired-end"

SUPPORTED_MWF = [MWF_NAME_CRAM_TO_FASTQ_PAIRED_END]

# Portal constants
COMPLETED = "completed"
UUID = "uuid"


def associate_conversion_output_with_fileset(
    mwfr_identifier: str, smaht_key: dict
) -> None:
    """Patches conversion workflow outputs, so that they can be used for downstream processing.
    It associates fastq pairs with each other and patches them to the Fileset that is on the
    MetaWorkflowRun.

    Args:
        mwfr_identifier (string): (Conversion) MetaWorkflowRun accession or uuid
        smaht_key (dict): SMaHT key
    """

    mwfr_meta = ff_utils.get_metadata(mwfr_identifier, smaht_key)
    if mwfr_meta["final_status"] != COMPLETED:
        print_error_and_exit("MetaWorkflowRun must have final_status 'completed'.")
    if mwfr_meta["meta_workflow"]["name"] not in SUPPORTED_MWF:
        print_error_and_exit(
            f"Metaworkflow {mwfr_meta['meta_workflow']['name']} is not supported."
        )
    file_sets = mwfr_meta.get("file_sets", [])
    file_sets_uuids = list(map(lambda f: f[UUID], file_sets))
    if not file_sets_uuids:
        print_error_and_exit("The MetaWorkflowRun has not associated FileSet. Exiting.")

    for wfr in mwfr_meta["workflow_runs"]:
        output = wfr["output"]
        if wfr["name"] != WF_CRAM_TO_FASTQ_PAIRED_END:
            continue
        if len(output) != 2:
            print_error_and_exit(f"Expected exactly 2 output files in WorkflowRun")

        fastq_1_uuid = output[0]["file"]["uuid"]
        fastq_2_uuid = output[1]["file"]["uuid"]
        associate_paired_fastqs(
            fastq_1_uuid=fastq_1_uuid,
            fastq_2_uuid=fastq_2_uuid,
            force_override=False,
            smaht_key=smaht_key,
        )
        patch_body = {"file_sets": file_sets_uuids}
        try:
            ff_utils.patch_metadata(patch_body, obj_id=fastq_1_uuid, key=smaht_key)
            ff_utils.patch_metadata(patch_body, obj_id=fastq_2_uuid, key=smaht_key)
        except Exception as e:
            raise Exception(f"Item could not be PATCHed: {str(e)}")


def associate_paired_fastqs(
    fastq_1_uuid: str, fastq_2_uuid: str, force_override: bool, smaht_key: dict
):
    """Given an R1 and an R2 fastq file, this function sets the `paired_with` property
    of the R2 fastq to the given R1 fastq. The function will check which fastq is R1 and which is R2

    Args:
        fastq_1_uuid (str): UUID of fastq
        fastq_2_uuid (str): UUID of fastq
        force_override (bool): If the paired_with property is already set, this flag will allow it to be overridden
        smaht_key (dict): credentials
    """
    file_1 = ff_utils.get_metadata(fastq_1_uuid, smaht_key)
    file_2 = ff_utils.get_metadata(fastq_2_uuid, smaht_key)
    file_1_rpn = file_1.get("read_pair_number")
    file_2_rpn = file_2.get("read_pair_number")

    if file_1_rpn == "R1" and file_2_rpn == "R2":
        file_r1 = file_1
        file_r2 = file_2
    elif file_2_rpn == "R1" and file_1_rpn == "R2":
        file_r1 = file_2
        file_r2 = file_1
    else:
        print_error_and_exit(f"Could not identify fastq pair")

    if file_r2.get("paired_with") and not force_override:
        print(
            f"The 'paired_with' property is already set for file {file_r2[UUID]}. Not patching"
        )
        return

    patch_body = {"paired_with": file_r1[UUID]}
    try:
        ff_utils.patch_metadata(patch_body, obj_id=file_r2[UUID], key=smaht_key)
    except Exception as e:
        raise Exception(f"Item could not be PATCHed: {str(e)}")


def reset_failed_mwfrs(mwfr_uuids: list, smaht_key: dict):
    for id in mwfr_uuids:
        print(f"Reset MetaWorkflowRun {id}")
        reset_failed(id, smaht_key)


def reset_all_failed_mwfrs(smaht_key: dict):
    url = "/search/?final_status=failed&type=MetaWorkflowRun"
    results = ff_utils.search_metadata(url, key=smaht_key)
    for item in results:
        print(f"Reset MetaWorkflowRun {item['uuid']}")
        reset_failed(item["uuid"], smaht_key)


def print_error_and_exit(error):
    print(error)
    exit()


def set_property(
    uuid: str,
    prop_key: str,
    prop_value: Any,
    smaht_key: Dict[str, Any]
    ):
    """"Sets a property prop_key to value prop_value for item with uuid."""
    patch_body={
        prop_key: prop_value
    }
    try:
        ff_utils.patch_metadata(patch_body, obj_id=uuid, key=smaht_key)
        print(f"Set item {uuid} property {prop_key} to {prop_value}.")
    except Exception as e:
        raise Exception(f"Item could not be PATCHed: {str(e)}")