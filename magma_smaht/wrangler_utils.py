#!/usr/bin/env python3


import json
from pathlib import Path
from typing import Any, Dict, Sequence, Union
from packaging import version
from dcicutils import ff_utils
import pprint

from .create_metawfr import MWF_NAME_CRAM_TO_FASTQ_PAIRED_END
from .reset_metawfr import reset_failed, reset_all

from magma_smaht.utils import (
    get_file_set,
    get_donors_from_mwfr,
    get_item,
    get_tag_for_sample_identity_check,
    get_wfr_from_mwfr,
    get_latest_somalier_run_for_donor,
)

JsonObject = Dict[str, Any]

WF_CRAM_TO_FASTQ_PAIRED_END = "cram_to_fastq_paired-end"
WF_BAM_TO_FASTQ_PAIRED_END = "bam_to_fastq_paired-end"

SUPPORTED_MWF = [MWF_NAME_CRAM_TO_FASTQ_PAIRED_END, WF_BAM_TO_FASTQ_PAIRED_END]

# Portal constants
COMPLETED = "completed"
UUID = "uuid"
ACCESSION = "accession"


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
        if wfr["name"] not in [WF_CRAM_TO_FASTQ_PAIRED_END, WF_BAM_TO_FASTQ_PAIRED_END]:
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


def reset_mwfrs(mwfr_uuids: list, smaht_key: dict):
    for id in mwfr_uuids:
        print(f"Reset MetaWorkflowRun {id}")
        reset_all(id, smaht_key)


def reset_all_failed_mwfrs(smaht_key: dict):
    url = "/search/?final_status=failed&type=MetaWorkflowRun"
    results = ff_utils.search_metadata(url, key=smaht_key)
    for item in results:
        print(f"Reset MetaWorkflowRun {item['uuid']}")
        reset_failed(item["uuid"], smaht_key)


def merge_qc_items(file_accession: str, mode: str, smaht_key: dict):
    """Merge QC items of a file.
    Mode "keep_oldest" will merge the qc values and patch them to the oldest qc_item. The other qc_items will be removed from the file
    Mode "keep_newest" will merge the qc values and patch them to the newest qc_item. The other qc_items will be removed from the file
    In general, QC values of newer QC items will overwrite existing QC values of older items

    Args:
        file_accession (str): file accession string
        mode (str): "keep_oldest" or "keep_newest"
        smaht_key (dict): Auth key
    """
    qc_values_dict = {}  # This will hold the merged values
    file = ff_utils.get_metadata(file_accession, smaht_key)
    file_uuid = file["uuid"]
    file_qms = file.get("quality_metrics", [])
    if len(file_qms) < 2:
        print(f"ERROR: Not enough QM items present for merging.")
        return

    qm_uuid_to_keep = (
        file_qms[0]["uuid"] if mode == "keep_oldest" else file_qms[-1]["uuid"]
    )

    for qm in file_qms:
        qm_uuid = qm["uuid"]
        qm_item = ff_utils.get_metadata(qm_uuid, smaht_key)
        qc_values = qm_item["qc_values"]
        for qcv in qc_values:
            derived_from = qcv["derived_from"]
            qc_values_dict[derived_from] = qcv

    qc_values_list = list(qc_values_dict.values())

    try:
        patch_body = {"qc_values": qc_values_list}
        ff_utils.patch_metadata(patch_body, obj_id=qm_uuid_to_keep, key=smaht_key)
        patch_body = {"quality_metrics": [qm_uuid_to_keep]}
        ff_utils.patch_metadata(patch_body, obj_id=file_uuid, key=smaht_key)
    except Exception as e:
        raise Exception(f"Item could not be PATCHed: {str(e)}")
    print("Merging done.")


def archive_unaligned_reads(fileset_accession: str, dry_run: bool, smaht_key: dict):
    """Archive (submitted) unaligned reads of a fileset.
    Every submitted unaligned read in the fileset will receive the s3_lifecycle_categor=long_term_archive.

    Args:
        fileset_accession (str): _description_
        smaht_key (dict): _description_
    """
    file_set = get_file_set(fileset_accession, smaht_key)

    search_filter = (
        "?type=UnalignedReads" f"&status=uploaded" f"&file_sets.uuid={file_set[UUID]}"
    )
    unaligned_reads = ff_utils.search_metadata(
        f"/search/{search_filter}", key=smaht_key
    )
    if dry_run:
        print(f" - Patching {len(unaligned_reads)} files. DRY RUN - NOTHING PATCHED")
    else:
        print(f" - Patching {len(unaligned_reads)} files")

    for unaligned_read in unaligned_reads:
        if not dry_run:
            patch_body = {"s3_lifecycle_category": "long_term_archive"}
            ff_utils.patch_metadata(patch_body, obj_id=unaligned_read[UUID], key=smaht_key)
        print(f" - Archived file {unaligned_read['display_title']}")


def sample_identity_check_status(num_files: int, smaht_key: dict):
    """Check output files that are not input of sample_identity_check metaworkflow runs.


    Args:
        num_files (int): Number of files to check
        smaht_key (dict): Auth key
    """

    # Check for sample identity checks in progress. We should only run new sample identity workflows if the prvious ones completed
    search_filter = (
        "?type=MetaWorkflowRun"
        "&meta_workflow.name=sample_identity_check"
        "&final_status=running&final_status=pending"
    )
    current_checks = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)
    if current_checks:
        print(
            f"WARNING: THERE ARE CURRENTLY ACTIVE RUNS. ONLY RUN NEW WORKFLOWS WHEN THE PREVIOUS ONES COMPLETED"
        )

    search_filter = (
        "?type=OutputFile"
        "&sequencing_center.display_title=UWSC+GCC"
        "&sequencing_center.display_title=WASHU+GCC"
        "&sequencing_center.display_title=BROAD+GCC"
        "&sequencing_center.display_title=NYGC+GCC"
        "&sequencing_center.display_title=BCM+GCC"
        "&status=uploaded&status=released"
        "&file_format.display_title=bam"
        "&output_status=Final Output"
        f"&limit={num_files}"
        "&sort=date_created"
        "&meta_workflow_run_inputs.meta_workflow.name%21=sample_identity_check"
    )
    output_files = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)
    donors = {}
    status = {}
    for output in output_files:
        mwfrs = output.get("meta_workflow_run_outputs")
        if not mwfrs:
            print(
                f"Warning: No MetaWorkflowRunOutputs found for file {output['display_title']}"
            )
            continue
        mwfr = mwfrs[0]
        mwfr = get_item(mwfr[UUID], smaht_key, frame="embedded")

        # Only consider files that are outputs for alignment workflows
        if "Alignment" not in mwfr["meta_workflow"]["category"]:
            print(
                f"Warning: File {output['accession']} is not result of an alignment MWF. Skipping."
            )
            continue

        donors_from_mwf = get_donors_from_mwfr(mwfr, smaht_key)
        if len(donors_from_mwf) > 1:
            print(
                f"Warning: Expected 1 donor but found {len(donors_from_mwf)} for file {output['accession']}"
            )
            continue

        if len(donors_from_mwf) == 0:  # Probably HAPMAP
            print(
                f"Warning: No donors found for file {output['accession']} ({output['display_title']}). HAPMAP?"
            )
            continue
        donor = donors_from_mwf[0]
        donor_uuid = donor[UUID]
        donors[donor_uuid] = donor

        if donor_uuid not in status:
            status[donor_uuid] = []
        status[donor_uuid].append(output[ACCESSION])

    print("The Sample Identity Check has not been run on the following files:")
    for donor_uuid in status:
        donor = donors[donor_uuid]
        donor_display_title = donor["display_title"]
        header = f"\nDonor: {donor_display_title} ({donor[ACCESSION]})"
        print("_" * len(header))
        print(header)

        for file in status[donor_uuid]:
            print(f" - {file}")

        latest_run = get_latest_somalier_run_for_donor(donor[ACCESSION], smaht_key)
        if not latest_run:
            print(
                f"\nINFO: Sample identity check has never run for donor {donor_display_title}"
            )
        else:
            latest_run = latest_run[0]
            somalier_relate = get_wfr_from_mwfr(latest_run, "somalier_relate", 0)
            qc_result = somalier_relate["output"][0]["file"]["quality_metrics"][0][
                "overall_quality_status"
            ]
            print(
                f"\nINFO: Latest run result was: {qc_result} (MWFR: {latest_run[ACCESSION]})"
            )

        print(f"\nRun the following command to perform this QC check:")
        print(
            f"create-mwfr-smaht sample-identity-check -e data -d {donor[ACCESSION]} -f {' -f '.join(status[donor_uuid])}\n"
        )


def print_error_and_exit(error):
    print(error)
    exit()


def set_property(uuid: str, prop_key: str, prop_value: Any, smaht_key: Dict[str, Any]):
    """ "Sets a property prop_key to value prop_value for item with uuid."""
    patch_body = {prop_key: prop_value}
    try:
        ff_utils.patch_metadata(patch_body, obj_id=uuid, key=smaht_key)
        print(f"Set item {uuid} property {prop_key} to {prop_value}.")
    except Exception as e:
        raise Exception(f"Item could not be PATCHed: {str(e)}")
