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
MWF_NAME_FASTQC = "Illumina_FASTQ_quality_metrics"
MWF_NAME_FASTQ_LONG_READ = "long_reads_FASTQ_quality_metrics"
MWF_NAME_CRAM_TO_FASTQ_PAIRED_END = "cram_to_fastq_paired-end"
MWF_NAME_BAMQC_SHORT_READ = "paired-end_short_reads_BAM_quality_metrics_GRCh38"
MWF_NAME_ULTRA_LONG_BAMQC = "ultra-long_reads_BAM_quality_metrics_GRCh38"
MWF_NAME_LONG_READ_BAMQC = "long_reads_BAM_quality_metrics_GRCh38"

# Input argument names
INPUT_FILES_R1_FASTQ_GZ = "input_files_r1_fastq_gz"
INPUT_FILES_R2_FASTQ_GZ = "input_files_r2_fastq_gz"
INPUT_FILES_BAM = "input_files_bam"
INPUT_FILES = "input_files"
INPUT_FILES_FASTQ_GZ = "input_files_fastq_gz"
INPUT_FILES_CRAM = "input_files_cram"
GENOME_REFERENCE_FASTA = "genome_reference_fasta"
SAMPLE_NAME = "sample_name"
LENGTH_REQUIRED = "length_required"
LIBRARY_ID = "library_id"

# Schema fields
COMMON_FIELDS = "common_fields"
UUID = "uuid"
SUBMISSION_CENTERS = "submission_centers"
SEQUENCING_CENTER = "sequencing_center"
CONSORTIA = "consortia"
FILE_SETS = "file_sets"
META_WORFLOW_RUN = "MetaWorkflowRun"
ACCESSION = "accession"
ALIASES = "aliases"


################################################
#   Alignemnt MetaWorkflowRuns
################################################


def mwfr_illumina_alignment(fileset_accession, length_required, smaht_key):
    """Creates a MetaWorflowRun item in the portal for Illumina alignment of submitted files within a fileset"""

    mwf = get_latest_mwf(MWF_NAME_ILLUMINA, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    file_set = get_file_set(fileset_accession, smaht_key)
    mwfr_input = get_core_alignment_mwfr_input_from_readpairs(
        file_set, INPUT_FILES_R1_FASTQ_GZ, INPUT_FILES_R2_FASTQ_GZ, smaht_key
    )
    # Illumina specific input
    mwfr_input.append(get_mwfr_parameter_input_arg(LENGTH_REQUIRED, length_required))
    create_and_post_mwfr(
        mwf[UUID], file_set, INPUT_FILES_R1_FASTQ_GZ, mwfr_input, smaht_key
    )


def mwfr_pacbio_alignment(fileset_accession, smaht_key):
    """Creates a MetaWorflowRun item in the portal for PacBio alignment of submitted files within a fileset"""

    mwf = get_latest_mwf(MWF_NAME_PACBIO, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    file_set = get_file_set(fileset_accession, smaht_key)
    mwfr_input = get_core_alignment_mwfr_input(file_set, INPUT_FILES_BAM, smaht_key)
    create_and_post_mwfr(mwf[UUID], file_set, INPUT_FILES_BAM, mwfr_input, smaht_key)


def mwfr_hic_alignment(fileset_accession, smaht_key):
    """Creates a MetaWorflowRun item in the portal for HI-C alignment of submitted files within a fileset"""

    mwf = get_latest_mwf(MWF_NAME_HIC, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    file_set = get_file_set(fileset_accession, smaht_key)
    mwfr_input = get_core_alignment_mwfr_input_from_readpairs(
        file_set, INPUT_FILES_R1_FASTQ_GZ, INPUT_FILES_R2_FASTQ_GZ, smaht_key
    )
    create_and_post_mwfr(
        mwf[UUID], file_set, INPUT_FILES_R1_FASTQ_GZ, mwfr_input, smaht_key
    )


def mwfr_ont_alignment(fileset_accession, smaht_key):
    """Creates a MetaWorflowRun item in the portal for ONT alignment of submitted files within a fileset"""

    mwf = get_latest_mwf(MWF_NAME_ONT, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    # Collect Input
    file_set = get_file_set(fileset_accession, smaht_key)
    library = get_library_from_file_set(file_set, smaht_key)
    sample = get_sample_from_library(library, smaht_key)

    # We are only retrieving the fastq files and get the bams from the derived_from property
    search_filter = f"?type=UnalignedReads&file_format.display_title=fastq_gz&file_sets.uuid={file_set[UUID]}"
    files_fastq = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)
    files_fastq.reverse()

    # Create files list for input args
    fastqs, bams = [], []
    for dim, file_fastq in enumerate(files_fastq):
        fastqs.append({"file": file_fastq[UUID], "dimension": f"{dim}"})
        bams.append(
            {"file": file_fastq["derived_from"][0][UUID], "dimension": f"{dim}"}
        )

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_FASTQ_GZ, fastqs),
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bams),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, sample[ACCESSION]),
        get_mwfr_parameter_input_arg(LIBRARY_ID, library[ACCESSION]),
    ]

    create_and_post_mwfr(
        mwf["uuid"], file_set, INPUT_FILES_FASTQ_GZ, mwfr_input, smaht_key
    )


################################################
#   Conversion MetaWorkflowRuns
################################################


def mwfr_cram_to_fastq_paired_end(fileset_accession, smaht_key):
    file_set = get_file_set(fileset_accession, smaht_key)
    mwf = get_latest_mwf(MWF_NAME_CRAM_TO_FASTQ_PAIRED_END, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    # Get submitted CRAMs in fileset (can be aligned or unaligned)
    search_filter = f"?file_sets.uuid={file_set[UUID]}&type=SubmittedFile&file_format.display_title=cram"
    files_to_run = ff_utils.search_metadata((f"search/{search_filter}"), key=smaht_key)
    files_to_run.reverse()

    if len(files_to_run) == 0:
        print(f"No files to run for search {search_filter}")
        return

    reference_genome_uuid = None
    crams = []
    for dim, file in enumerate(files_to_run):
        crams.append({"file": file[UUID], "dimension": f"{dim}"})
        # We need to assume that all crams have the same reference genome. Fail otherwise
        current_ref_genome = file.get("reference_genome")
        if not current_ref_genome:
            raise Exception(f"File {file[UUID]} has not reference genome")
        if reference_genome_uuid and current_ref_genome[UUID] != reference_genome_uuid:
            raise Exception(f"Multiple reference genomes detected.")
        reference_genome_uuid = current_ref_genome[UUID]
    reference_genome_item = ff_utils.get_metadata(reference_genome_uuid, smaht_key)
    reference_genome_file = reference_genome_item["files"][0][UUID]
    reference_genome = [{"file": reference_genome_file}]
    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_CRAM, crams),
        get_mwfr_file_input_arg(GENOME_REFERENCE_FASTA, reference_genome),
    ]
    # pprint.pprint(mwfr_input)
    create_and_post_mwfr(mwf[UUID], file_set, INPUT_FILES_CRAM, mwfr_input, smaht_key)


################################################
#   QC MetaWorkflowRuns
################################################


def mwfr_fastqc(fileset_accession, smaht_key):

    file_set = get_file_set(fileset_accession, smaht_key)
    mwf = get_latest_mwf(MWF_NAME_FASTQC, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    # Get unaligned R2 reads in the fileset that don't have already QC
    search_filter = f"?file_sets.uuid={file_set[UUID]}&type=File&file_format.display_title=fastq_gz&read_pair_number=R2&quality_metrics=No+value"

    files_to_run_r2 = ff_utils.search_metadata(
        (f"search/{search_filter}"), key=smaht_key
    )
    files_to_run_r2.reverse()

    if len(files_to_run_r2) == 0:
        print(f"No files to run for search {search_filter}")
        return

    # Create files list for input args
    files_input = []
    for dim, file_r2 in enumerate(files_to_run_r2):
        files_input.append({"file": file_r2[UUID], "dimension": f"1,{dim}"})
        files_input.append(
            {"file": file_r2["paired_with"][UUID], "dimension": f"0,{dim}"}
        )

    # # Manual override
    # files_input = []
    # files_input.append({"file": "577b4259-81f5-4b0c-9ffc-254696b37493", "dimension": f"1,0"}) # R2
    # files_input.append({"file": "cc0cb991-5fbc-4166-ad03-eaf8a42de649", "dimension": f"0,0"}) # R1

    files_input_list = sorted(files_input, key=lambda x: x["dimension"])

    mwfr_input = [get_mwfr_file_input_arg(INPUT_FILES_FASTQ_GZ, files_input_list)]
    create_and_post_mwfr(
        mwf[UUID], file_set, INPUT_FILES_FASTQ_GZ, mwfr_input, smaht_key
    )


def mwfr_ubam_qc_long_read(fileset_accession, smaht_key):
    
    file_set = get_file_set(fileset_accession, smaht_key)
    mwf = get_latest_mwf(MWF_NAME_FASTQ_LONG_READ, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    # Get unaligned BAMs in the fileset that don't have already QC
    search_filter = f"?file_sets.uuid={file_set[UUID]}&type=UnalignedReads&file_format.display_title=bam&quality_metrics=No+value"

    bams = ff_utils.search_metadata((f"search/{search_filter}"), key=smaht_key)
    bams.reverse()

    if len(bams) == 0:
        print(f"No files to run for search {search_filter}")
        return

    # Create files list for input args
    files_input = []
    for dim, bam in enumerate(bams):
        files_input.append({"file": bam[UUID], "dimension": f"{dim}"})

    mwfr_input = [get_mwfr_file_input_arg(INPUT_FILES, files_input)]
    create_and_post_mwfr(mwf[UUID], file_set, INPUT_FILES, mwfr_input, smaht_key)


def mwfr_bamqc_short_read(file_accession, smaht_key):
    mwf = get_latest_mwf(MWF_NAME_BAMQC_SHORT_READ, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")
    bam_meta = get_metadata(file_accession, smaht_key)
    bam = [{"file": bam_meta[UUID], "dimension": "0"}]

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bam),
    ]

    create_and_post_mwfr(mwf["uuid"], None, INPUT_FILES_BAM, mwfr_input, smaht_key)


def mwfr_ultra_long_bamqc(file_accession, smaht_key):
    mwf = get_latest_mwf(MWF_NAME_ULTRA_LONG_BAMQC, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")
    bam_meta = get_metadata(file_accession, smaht_key)
    bam = [{"file": bam_meta[UUID], "dimension": "0"}]

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bam),
    ]

    create_and_post_mwfr(mwf["uuid"], None, INPUT_FILES_BAM, mwfr_input, smaht_key)


def mwfr_long_read_bamqc(file_accession, smaht_key):
    mwf = get_latest_mwf(MWF_NAME_LONG_READ_BAMQC, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")
    bam_meta = get_metadata(file_accession, smaht_key)
    bam = [{"file": bam_meta[UUID], "dimension": "0"}]

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bam),
    ]

    create_and_post_mwfr(mwf["uuid"], None, INPUT_FILES_BAM, mwfr_input, smaht_key)


################################################
#   Helper functions
################################################


def get_common_fields(file_set):

    sequencing_centers = file_set[SUBMISSION_CENTERS]
    if len(sequencing_centers) != 1:
        raise Exception(
            f"Exacted exactly one submission center for file set {file_set[ACCESSION]} expected but got {len(sequencing_centers)}"
        )

    common_fields = {SEQUENCING_CENTER: sequencing_centers[0]}

    return common_fields


def get_core_alignment_mwfr_input_from_readpairs(
    file_set, file_input_arg_1, file_input_arg_2, smaht_key
):

    library = get_library_from_file_set(file_set, smaht_key)
    sample = get_sample_from_library(library, smaht_key)

    # We are only retrieving the R2 reads and get the R1 read from the paired_with property
    search_filter = (
        # f"?type=UnalignedReads&read_pair_number=R2&file_sets.uuid={file_set[UUID]}"
        f"?type=File&read_pair_number=R2&file_sets.uuid={file_set[UUID]}"
    )
    reads_r2 = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)
    reads_r2.reverse()

    # Create files list for input args
    files_r1, files_r2 = [], []
    for dim, file_r2 in enumerate(reads_r2):
        files_r2.append({"file": file_r2[UUID], "dimension": f"{dim}"})
        files_r1.append({"file": file_r2["paired_with"][UUID], "dimension": f"{dim}"})

    mwfr_input = [
        get_mwfr_file_input_arg(file_input_arg_1, files_r1),
        get_mwfr_file_input_arg(file_input_arg_2, files_r2),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, sample[ACCESSION]),
        get_mwfr_parameter_input_arg(LIBRARY_ID, library[ACCESSION]),
    ]
    return mwfr_input


def get_core_alignment_mwfr_input(file_set, file_input_arg, smaht_key):

    library = get_library_from_file_set(file_set, smaht_key)
    sample = get_sample_from_library(library, smaht_key)

    search_filter = f"?type=UnalignedReads&file_sets.uuid={file_set[UUID]}"
    search_result = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)
    search_result.reverse()

    # Create files list for input args
    files = []
    for dim, file in enumerate(search_result):
        files.append({"file": file[UUID], "dimension": f"{dim}"})

    mwfr_input = [
        get_mwfr_file_input_arg(file_input_arg, files),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, sample[ACCESSION]),
        get_mwfr_parameter_input_arg(LIBRARY_ID, library[ACCESSION]),
    ]
    return mwfr_input


def create_and_post_mwfr(mwf_uuid, file_set, input_arg, mwfr_input, smaht_key):

    mwfr = mwfr_from_input(mwf_uuid, mwfr_input, input_arg, smaht_key)
    if file_set:
        mwfr[FILE_SETS] = [file_set[UUID]]
        mwfr[COMMON_FIELDS] = get_common_fields(file_set)
    # mwfr['final_status'] = 'stopped'
    # pprint.pprint(mwfr)

    post_response = ff_utils.post_metadata(mwfr, META_WORFLOW_RUN, smaht_key)
    mwfr_accession = post_response["@graph"][0]["accession"]
    if file_set:
        print(
            f"Posted MetaWorkflowRun {mwfr_accession} for Fileset {file_set[ACCESSION]}."
        )
    else:
        print(f"Posted MetaWorkflowRun {mwfr_accession}.")


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

    metawf_meta = get_metadata(metawf_uuid, ff_key)

    for arg in input:
        if arg["argument_name"] == input_arg:
            input_structure = generate_input_structure(arg["files"])

    mwf = MetaWorkflow(metawf_meta)
    mwfr = mwf.write_run(input_structure)

    mwfr[UUID] = str(uuid.uuid4())
    mwfr[CONSORTIA] = consortia
    mwfr[SUBMISSION_CENTERS] = submission_centers
    mwfr["input"] = input

    return mwfr


def generate_input_structure(files):
    dimension_first_file = files[0][
        "dimension"
    ]  # We assume that this is representative of the input structure
    if dimension_first_file.count(",") == 0:
        return list(range(len(files)))
    elif dimension_first_file.count(",") == 1:
        dimensions = list(map(lambda x: x["dimension"].split(","), files))
        dimensions = list(map(lambda x: [int(x[0]), int(x[1])], dimensions))
        # Example for dimensions: [[1, 0],[0, 0],[1, 1],[0, 1],[1, 2]]
        dimensions_dict = {}
        for dim in dimensions:
            if dim[0] not in dimensions_dict:
                dimensions_dict[dim[0]] = [dim[1]]
            else:
                dimensions_dict[dim[0]].append(dim[1])
        # Example for dimensions_dict: {0: [0, 1], 1: [0, 1, 2]}
        input_structure = []
        for key in sorted(dimensions_dict.keys()):
            input_structure.append(dimensions_dict[key])
        # Example for input_structure: [[0, 1], [0, 1, 2]]
        return input_structure
    else:
        print("More than 2 input dimensions are currently no supported")
        exit()


def get_metadata(identifier, key):
    return ff_utils.get_metadata(
        identifier, add_on="frame=raw&datastore=database", key=key
    )
