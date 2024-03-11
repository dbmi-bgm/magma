#!/usr/bin/env python3

################################################
#
#   Functions to create a MetaWorkflowRun
#
################################################

################################################
#   Libraries
################################################

import json, uuid
from dcicutils import ff_utils
import pprint

pp = pprint.PrettyPrinter(indent=2)

# magma
from magma_smaht.metawfl import MetaWorkflow
from magma_smaht.utils import (
    get_latest_mwf,
    get_file_set,
    get_library_from_file_set,
    get_sample_from_library,
    get_mwfr_file_input_arg,
    get_mwfr_parameter_input_arg,
)

# MetaWorkflow names are used to get the latest version.
# We assume that they don't change!
MWF_NAME_ILLUMINA = "Illumina_alignment_GRCh38"
MWF_NAME_ONT = "ONT_alignment_GRCh38"
MWF_NAME_PACBIO = "PacBio_alignment_GRCh38"
MWF_NAME_HIC = "Hi-C_alignment_GRCh38"
MWF_NAME_FASTQC = "short_reads_FASTQ_quality_metrics"

# Input argument names
INPUT_FILES_R1_FASTQ_GZ = "input_files_r1_fastq_gz"
INPUT_FILES_R2_FASTQ_GZ = "input_files_r2_fastq_gz"
INPUT_FILES_BAM = "input_files_bam"
INPUT_FILES_FASTQ_GZ = "input_files_fastq_gz"
SAMPLE_NAME = "sample_name"
LENGTH_REQUIRED = "length_required"
LIBRARY_ID = "library_id"


################################################
#   Functions
################################################


def mwfr_illumina_alignment(fileset_accession, length_required, smaht_key):
    """Creates a MetaWorflowRun item in the portal for Illumina alignment of submitted files within a fileset"""

    mwf = get_latest_mwf(MWF_NAME_ILLUMINA, smaht_key)
    print(f"Using MetaWorkflow {mwf['accession']} ({mwf['aliases'][0]})")

    file_set = get_file_set(fileset_accession, smaht_key)
    mwfr_input = get_core_alignment_mwfr_input_from_readpairs(
        file_set, INPUT_FILES_R1_FASTQ_GZ, INPUT_FILES_R2_FASTQ_GZ, smaht_key
    )
    # Illumina specific input
    mwfr_input.append(get_mwfr_parameter_input_arg(LENGTH_REQUIRED, length_required))
    create_and_post_mwfr(mwf["uuid"], file_set, INPUT_FILES_R1_FASTQ_GZ, mwfr_input, smaht_key)


def mwfr_pacbio_alignment(fileset_accession, smaht_key):
    """Creates a MetaWorflowRun item in the portal for PacBio alignment of submitted files within a fileset"""

    mwf = get_latest_mwf(MWF_NAME_PACBIO, smaht_key)
    print(f"Using MetaWorkflow {mwf['accession']} ({mwf['aliases'][0]})")

    file_set = get_file_set(fileset_accession, smaht_key)
    mwfr_input = get_core_alignment_mwfr_input(file_set, INPUT_FILES_BAM, smaht_key)
    create_and_post_mwfr(mwf["uuid"], file_set, INPUT_FILES_BAM, mwfr_input, smaht_key)


def mwfr_hic_alignment(fileset_accession, smaht_key):
    """Creates a MetaWorflowRun item in the portal for HI-C alignment of submitted files within a fileset"""

    mwf = get_latest_mwf(MWF_NAME_HIC, smaht_key)
    print(f"Using MetaWorkflow {mwf['accession']} ({mwf['aliases'][0]})")

    file_set = get_file_set(fileset_accession, smaht_key)
    mwfr_input = get_core_alignment_mwfr_input_from_readpairs(
        file_set, INPUT_FILES_R1_FASTQ_GZ, INPUT_FILES_R2_FASTQ_GZ, smaht_key
    )
    create_and_post_mwfr(mwf["uuid"], file_set, INPUT_FILES_R1_FASTQ_GZ, mwfr_input, smaht_key)


def mwfr_ont_alignment(fileset_accession, smaht_key):
    """Creates a MetaWorflowRun item in the portal for ONT alignment of submitted files within a fileset"""

    mwf = get_latest_mwf(MWF_NAME_ONT, smaht_key)
    print(f"Using MetaWorkflow {mwf['accession']} ({mwf['aliases'][0]})")

    # Collect Input
    file_set = get_file_set(fileset_accession, smaht_key)
    library = get_library_from_file_set(file_set, smaht_key)
    sample = get_sample_from_library(library, smaht_key)

    # We are only retrieving the fastq files and get the bams from the derived_from property
    search_filter = f"?type=UnalignedReads&file_format.display_title=fastq_gz&file_sets.uuid={file_set['uuid']}"
    files_fastq = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)

    # Create files list for input args
    fastqs, bams = [], []
    for dim, file_fastq in enumerate(files_fastq):
        fastqs.append({"file": file_fastq["uuid"], "dimension": f"{dim}"})
        bams.append(
            {"file": file_fastq["derived_from"][0]["uuid"], "dimension": f"{dim}"}
        )

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_FASTQ_GZ, fastqs),
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bams),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, sample["accession"]),
        get_mwfr_parameter_input_arg(LIBRARY_ID, library["accession"]),
    ]

    create_and_post_mwfr(mwf["uuid"], file_set, INPUT_FILES_FASTQ_GZ, mwfr_input, smaht_key)


def mwfr_fastqc(fileset_accession, smaht_key):

    file_set = get_file_set(fileset_accession, smaht_key)
    mwf = get_latest_mwf(MWF_NAME_FASTQC, smaht_key)
    print(f"Using MetaWorkflow {mwf['accession']} ({mwf['aliases'][0]})")

    # Get unaligned reads in the fileset that don't have already QC
    search_filter = f"?file_sets.uuid={file_set['uuid']}&type=UnalignedReads&file_format.display_title=fastq_gz&quality_metrics=No+value"
    files_to_run = ff_utils.search_metadata((f"search/{search_filter}"), key=smaht_key)

    if len(files_to_run) == 0:
        print(f"No files to run for search {search_filter}")
        return

    files_input = []
    for dim, file in enumerate(files_to_run):
        files_input.append({"file": file["uuid"], "dimension": f"{dim}"})

    input_arg = "input_files_fastq_gz"
    mwfr_input = [get_mwfr_file_input_arg(input_arg, files_input)]

    create_and_post_mwfr(mwf["uuid"], file_set, input_arg, mwfr_input, smaht_key)


def get_core_alignment_mwfr_input_from_readpairs(
    file_set, file_input_arg_1, file_input_arg_2, smaht_key
):

    library = get_library_from_file_set(file_set, smaht_key)
    sample = get_sample_from_library(library, smaht_key)

    # We are only retrieving the R2 reads and get the R1 read from the paired_with property
    search_filter = (
        f"?type=UnalignedReads&read_pair_number=R2&file_sets.uuid={file_set['uuid']}"
    )
    reads_r2 = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)

    # Create files list for input args
    files_r1, files_r2 = [], []
    for dim, file_r2 in enumerate(reads_r2):
        files_r2.append({"file": file_r2["uuid"], "dimension": f"{dim}"})
        files_r1.append({"file": file_r2["paired_with"]["uuid"], "dimension": f"{dim}"})

    mwfr_input = [
        get_mwfr_file_input_arg(file_input_arg_1, files_r1),
        get_mwfr_file_input_arg(file_input_arg_2, files_r2),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, sample["accession"]),
        get_mwfr_parameter_input_arg(LIBRARY_ID, library["accession"]),
    ]
    return mwfr_input


def get_core_alignment_mwfr_input(file_set, file_input_arg, smaht_key):

    library = get_library_from_file_set(file_set, smaht_key)
    sample = get_sample_from_library(library, smaht_key)

    search_filter = f"?type=UnalignedReads&file_sets.uuid={file_set['uuid']}"
    search_result = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)

    # Create files list for input args
    files = []
    for dim, file in enumerate(search_result):
        files.append({"file": file["uuid"], "dimension": f"{dim}"})

    mwfr_input = [
        get_mwfr_file_input_arg(file_input_arg, files),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, sample["accession"]),
        get_mwfr_parameter_input_arg(LIBRARY_ID, library["accession"]),
    ]
    return mwfr_input


def create_and_post_mwfr(mwf_uuid, file_set, input_arg, mwfr_input, smaht_key):

    mwfr = mwfr_from_input(mwf_uuid, mwfr_input, input_arg, smaht_key)
    mwfr["file_sets"] = [file_set["uuid"]]
    # mwfr['final_status'] = 'stopped'
    # print(mwfr)
    post_response = ff_utils.post_metadata(mwfr, "MetaWorkflowRun", smaht_key)
    mwfr_accession = post_response["@graph"][0]["accession"]
    print(
        f"Posted MetaWorkflowRun {mwfr_accession} for Fileset {file_set['accession']}."
    )


def filter_list_of_dicts(list_of_dics, property_target, target):
    return [w for w in list_of_dics if w[property_target] == target]


def mwfr_from_input(
    metawf_uuid,
    input,
    input_arg,
    ff_key,
    consortia=["smaht"],
    submission_centers=["smaht_dac"],
):
    """Create a MetaWorkflowRun[json] from the given MetaWorkflow[portal]
    and input arguments.

    :param metawf_uuid: MetaWorkflow[portal] UUID
    :type metawf_uuid: str
    :param input: Input arguments as list, where each argument is a dictionary
    :type list(dict)
    :param input_arg: argument_name of the input argument to use
        to calculate input structure
    :type str
    :param ff_key: Portal authorization key
    :type ff_key: dict

        e.g. input,
            input = [{
                    'argument_name': 'ARG_NAME',
                    'argument_type': 'file',
                    'files':[{'file': 'UUID', 'dimension': str(0)}]
                    }, ...]
    """

    metawf_meta = ff_utils.get_metadata(
        metawf_uuid, add_on="frame=raw&datastore=database", key=ff_key
    )

    for arg in input:
        if arg["argument_name"] == input_arg:
            input_structure = arg["files"]

    mwf = MetaWorkflow(metawf_meta)
    mwfr = mwf.write_run(input_structure)

    mwfr["uuid"] = str(uuid.uuid4())
    mwfr["consortia"] = consortia
    mwfr["submission_centers"] = submission_centers
    mwfr["input"] = input

    return mwfr
