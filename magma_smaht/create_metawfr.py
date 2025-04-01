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
from magma_smaht.utils import mwfr_from_input
import pprint

pp = pprint.PrettyPrinter(indent=2)

# magma
from magma_smaht.utils import (
    get_latest_mwf,
    get_file_set,
    get_library_from_file_set,
    get_library_preparation_from_library,
    get_samples_from_library,
    get_sample_name_for_mwfr,
    get_mwfr_file_input_arg,
    get_mwfr_parameter_input_arg,
    get_wfr_from_mwfr,
    get_tag_for_sample_identity_check,
    get_item,
    get_latest_somalier_run_for_donor,
)

from magma_smaht.constants import (
    MWF_NAME_ILLUMINA,
    MWF_NAME_RNASEQ,
    MWF_NAME_ONT,
    MWF_NAME_PACBIO,
    MWF_NAME_HIC,
    MWF_NAME_FASTQC,
    MWF_NAME_FASTQ_LONG_READ,
    MWF_NAME_FASTQ_SHORT_READ,
    MWF_NAME_CRAM_TO_FASTQ_PAIRED_END,
    MWF_NAME_BAM_TO_FASTQ_PAIRED_END,
    MWF_NAME_BAMQC_SHORT_READ,
    MWF_NAME_ULTRA_LONG_BAMQC,
    MWF_NAME_LONG_READ_BAMQC,
    MWF_SAMPLE_IDENTITY_CHECK,
    INPUT_FILES_R1_FASTQ_GZ,
    INPUT_FILES_R2_FASTQ_GZ,
    INPUT_FILES_BAM,
    INPUT_FILES_FASTQ_GZ,
    INPUT_FILES_CRAM,
    GENOME_REFERENCE_FASTA,
    SAMPLE_NAME,
    SAMPLE_NAMES,
    LENGTH_REQUIRED,
    LIBRARY_ID,
    GENOME_REFERENCE_STAR,
    IS_STRANDED,
    STRANDEDNESS,
    COMMON_FIELDS,
    UUID,
    SUBMISSION_CENTERS,
    SEQUENCING_CENTER,
    FILE_SETS,
    META_WORFLOW_RUN,
    ACCESSION,
    ALIASES,
    UPLOADED,
    FIRST_STRANDED,
    SECOND_STRANDED,
)


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


def mwfr_rnaseq_alignment(fileset_accession, sequence_length, smaht_key):
    """Creates a MetaWorflowRun item in the portal for RNA-Seq alignment of submitted files within a fileset"""

    mwf = get_latest_mwf(MWF_NAME_RNASEQ, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    file_set = get_file_set(fileset_accession, smaht_key)
    mwfr_input = get_core_alignment_mwfr_input_from_readpairs(
        file_set, INPUT_FILES_R1_FASTQ_GZ, INPUT_FILES_R2_FASTQ_GZ, smaht_key
    )
    # RNA-Seq specific input
    genome_reference_star_alias = f"smaht:ReferenceFile-star-index-no-alt-no-hla-gencode47-oh{sequence_length-1}_GCA_000001405.15_GRCh38_no_decoy"
    search_reference_file = "?type=File" f"&aliases={genome_reference_star_alias}"
    reference_files = ff_utils.search_metadata(
        f"/search/{search_reference_file}", key=smaht_key
    )
    if len(reference_files) != 1:
        raise Exception(
            f"Did not find exactly one genome_reference_star reference file. Search was: {search_reference_file}"
        )
    genome_reference_star_file = reference_files[0]

    mwfr_input.append(
        get_mwfr_file_input_arg(
            GENOME_REFERENCE_STAR,
            [{"file": genome_reference_star_file[UUID]}],
        )
    )

    # Get strandedness info
    library = get_library_from_file_set(file_set, smaht_key)
    library_preparation = get_library_preparation_from_library(library, smaht_key)
    strand = library_preparation["strand"]

    if strand in [FIRST_STRANDED, SECOND_STRANDED]:
        strandedness_mapping = {FIRST_STRANDED: "rf", SECOND_STRANDED: "fr"}
        mwfr_input.extend(
            [
                get_mwfr_parameter_input_arg(IS_STRANDED, "true"),
                get_mwfr_parameter_input_arg(
                    STRANDEDNESS, strandedness_mapping[strand]
                ),
            ]
        )

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
    samples = get_samples_from_library(library, smaht_key)
    sample_name = get_sample_name_for_mwfr(samples)

    # We are only retrieving the fastq files and get the bams from the derived_from property
    search_filter = (
        "?type=UnalignedReads"
        f"&file_sets.uuid={file_set[UUID]}"
        f"&status={UPLOADED}"
        "&file_format.display_title=fastq_gz"
    )
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
        get_mwfr_parameter_input_arg(SAMPLE_NAME, sample_name),
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
    search_filter = (
        f"?file_sets.uuid={file_set[UUID]}"
        "&type=SubmittedFile"
        "&file_format.display_title=cram"
    )
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
    reference_genome_item = ff_utils.get_item(reference_genome_uuid, smaht_key)
    reference_genome_file = reference_genome_item["files"][0][UUID]
    reference_genome = [{"file": reference_genome_file}]
    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_CRAM, crams),
        get_mwfr_file_input_arg(GENOME_REFERENCE_FASTA, reference_genome),
    ]
    create_and_post_mwfr(mwf[UUID], file_set, INPUT_FILES_CRAM, mwfr_input, smaht_key)


def mwfr_bam_to_fastq_paired_end(fileset_accession, smaht_key):
    file_set = get_file_set(fileset_accession, smaht_key)
    mwf = get_latest_mwf(MWF_NAME_BAM_TO_FASTQ_PAIRED_END, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    # Get submitted CRAMs in fileset (can be aligned or unaligned)
    search_filter = (
        f"?file_sets.uuid={file_set[UUID]}"
        "&type=SubmittedFile"
        "&file_format.display_title=bam"
    )
    files_to_run = ff_utils.search_metadata((f"search/{search_filter}"), key=smaht_key)
    files_to_run.reverse()

    if len(files_to_run) == 0:
        print(f"No files to run for search {search_filter}")
        return

    bams = []
    for dim, file in enumerate(files_to_run):
        bams.append({"file": file[UUID], "dimension": f"{dim}"})

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bams),
    ]
    create_and_post_mwfr(mwf[UUID], file_set, INPUT_FILES_BAM, mwfr_input, smaht_key)


################################################
#   QC MetaWorkflowRuns
################################################


def mwfr_fastqc(fileset_accession, check_lanes, replace_existing_qc, smaht_key):

    file_set = get_file_set(fileset_accession, smaht_key)
    if check_lanes:
        print(f"Using MetaWorkflow {MWF_NAME_FASTQC}")
        mwf = get_latest_mwf(MWF_NAME_FASTQC, smaht_key)
    else:
        print(f"Using MetaWorkflow {MWF_NAME_FASTQ_SHORT_READ}")
        mwf = get_latest_mwf(MWF_NAME_FASTQ_SHORT_READ, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    # Get unaligned R2 reads in the fileset that don't have already QC
    search_filter = (
        "?type=File"
        f"&file_sets.uuid={file_set[UUID]}"
        f"&status={UPLOADED}"
        "&file_format.display_title=fastq_gz"
        "&read_pair_number=R2"
        # "&quality_metrics=No+value"
    )

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

        if replace_existing_qc:
            print(f"QC reset for {file_r2[UUID]}")
            ff_utils.patch_metadata(
                {},
                obj_id=f"{file_r2[UUID]}?delete_fields=quality_metrics",
                key=smaht_key,
            )
            print(f"QC reset for {file_r2['paired_with'][UUID]}")
            ff_utils.patch_metadata(
                {},
                obj_id=f"{file_r2['paired_with'][UUID]}?delete_fields=quality_metrics",
                key=smaht_key,
            )

    # # Manual override
    # files_input = []
    # files_input.append({"file": "577b4259-81f5-4b0c-9ffc-254696b37493", "dimension": f"1,0"}) # R2
    # files_input.append({"file": "cc0cb991-5fbc-4166-ad03-eaf8a42de649", "dimension": f"0,0"}) # R1

    files_input_list = sorted(files_input, key=lambda x: x["dimension"])

    # If we are not running the Illumina MWF, reformat to 1D list
    if not check_lanes:
        for i, inp in enumerate(files_input_list):
            inp["dimension"] = f"{i}"

    mwfr_input = [get_mwfr_file_input_arg(INPUT_FILES_FASTQ_GZ, files_input_list)]
    create_and_post_mwfr(
        mwf[UUID], file_set, INPUT_FILES_FASTQ_GZ, mwfr_input, smaht_key
    )


def mwfr_ubam_qc_long_read(fileset_accession, replace_existing_qc, smaht_key):

    file_set = get_file_set(fileset_accession, smaht_key)
    mwf = get_latest_mwf(MWF_NAME_FASTQ_LONG_READ, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    # Get unaligned BAMs in the fileset that don't have already QC
    search_filter = (
        "?type=UnalignedReads"
        f"&status={UPLOADED}"
        "&file_format.display_title=bam"
        # "&quality_metrics=No+value"
        f"&file_sets.uuid={file_set[UUID]}"
    )

    bams = ff_utils.search_metadata((f"search/{search_filter}"), key=smaht_key)
    bams.reverse()

    if len(bams) == 0:
        print(f"No files to run for search {search_filter}")
        return

    if replace_existing_qc:
        for bam in bams:
            print(f"QC reset for {bam['uuid']}")
            ff_utils.patch_metadata(
                {}, obj_id=f"{bam['uuid']}?delete_fields=quality_metrics", key=smaht_key
            )

    # Create files list for input args
    files_input = []
    for dim, bam in enumerate(bams):
        files_input.append({"file": bam[UUID], "dimension": f"{dim}"})

    mwfr_input = [get_mwfr_file_input_arg(INPUT_FILES_BAM, files_input)]
    create_and_post_mwfr(mwf[UUID], file_set, INPUT_FILES_BAM, mwfr_input, smaht_key)


####
def mwfr_custom_qc(file_accession, smaht_key):
    # Placeholder for custom QC runs
    return
    # mwf_name = "CUSTOM_long-reads_mosdepth_verifybamid2"
    # # mwf_name = "CUSTOM_short-reads_mosdepth_verifybamid2"
    # mwf = get_latest_mwf(mwf_name, smaht_key)
    # print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")
    # bam_meta = get_item(file_accession, smaht_key)
    # bam = [{"file": bam_meta[UUID], "dimension": "0"}]

    # mwfr_input = [
    #     get_mwfr_file_input_arg(INPUT_FILES_BAM, bam),
    # ]

    # create_and_post_mwfr(mwf["uuid"], None, INPUT_FILES_BAM, mwfr_input, smaht_key)


###


def mwfr_bamqc_short_read(file_accession, smaht_key):
    mwf = get_latest_mwf(MWF_NAME_BAMQC_SHORT_READ, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")
    bam_meta = get_item(file_accession, smaht_key)
    bam = [{"file": bam_meta[UUID], "dimension": "0"}]

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bam),
    ]

    create_and_post_mwfr(mwf["uuid"], None, INPUT_FILES_BAM, mwfr_input, smaht_key)


def mwfr_ultra_long_bamqc(file_accession, replace_existing_qc, smaht_key):
    mwf = get_latest_mwf(MWF_NAME_ULTRA_LONG_BAMQC, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")
    bam_meta = get_item(file_accession, smaht_key)
    bam = [{"file": bam_meta[UUID], "dimension": "0"}]

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bam),
    ]

    if replace_existing_qc:
        print(f"QC reset for {file_accession}")
        ff_utils.patch_metadata(
            {},
            obj_id=f"{bam_meta[UUID]}?delete_fields=quality_metrics",
            key=smaht_key,
        )

    create_and_post_mwfr(mwf["uuid"], None, INPUT_FILES_BAM, mwfr_input, smaht_key)


def mwfr_long_read_bamqc(file_accession, replace_existing_qc, smaht_key):
    mwf = get_latest_mwf(MWF_NAME_LONG_READ_BAMQC, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")
    bam_meta = get_item(file_accession, smaht_key)
    bam = [{"file": bam_meta[UUID], "dimension": "0"}]

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bam),
    ]

    if replace_existing_qc:
        print(f"QC reset for {file_accession}")
        ff_utils.patch_metadata(
            {},
            obj_id=f"{bam_meta[UUID]}?delete_fields=quality_metrics",
            key=smaht_key,
        )

    create_and_post_mwfr(mwf["uuid"], None, INPUT_FILES_BAM, mwfr_input, smaht_key)


def mwfr_short_read_fastqc(file_accession, smaht_key):
    mwf = get_latest_mwf(MWF_NAME_FASTQ_SHORT_READ, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")
    fastq_meta = get_item(file_accession, smaht_key)
    fastq = [{"file": fastq_meta[UUID], "dimension": "0"}]

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_FASTQ_GZ, fastq),
    ]

    create_and_post_mwfr(mwf["uuid"], None, INPUT_FILES_FASTQ_GZ, mwfr_input, smaht_key)


def mwfr_sample_identity_check(files, donor, smaht_key):

    mwf = get_latest_mwf(MWF_SAMPLE_IDENTITY_CHECK, smaht_key)
    print(f"Using MetaWorkflow {mwf[ACCESSION]} ({mwf[ALIASES][0]})")

    previous_bam_ids = []
    previous_bam_dimension = {}
    previous_mwfr = get_latest_somalier_run_for_donor(donor, smaht_key)
    if previous_mwfr:
        print(f"Importing data from previous MetaWorkflowRun for donor {donor}")
        # We want the raw frame - that's why we get it here again
        previous_mwfr = get_item(previous_mwfr[0][UUID], smaht_key)
        for input in previous_mwfr["input"]:
            if input["argument_name"] == INPUT_FILES_BAM:
                previous_bam_dimension = {
                    file["file"]: file["dimension"] for file in input["files"]
                }
                previous_bam_ids = [file["file"] for file in input["files"]]
    else:
        print(f"No previous identity check found for donor {donor}")

    bams = []
    for id in previous_bam_ids + list(files):
        bam_meta = get_item(id, smaht_key)
        bams.append(bam_meta)

    dimension_mapping = (
        {}
    )  # Maps the new shard numbers to the old ones, if they were present in the previous run
    bam_inputs, bam_accessions = [], []
    for dim, bam in enumerate(bams):
        if bam[UUID] in previous_bam_dimension:
            dimension_mapping[str(dim)] = previous_bam_dimension[bam[UUID]]
        bam_accessions.append(bam[ACCESSION])
        bam_inputs.append({"file": bam[UUID], "dimension": f"{dim}"})

    mwfr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_BAM, bam_inputs),
        get_mwfr_parameter_input_arg(SAMPLE_NAMES, bam_accessions),
    ]
    mwfr = mwfr_from_input(mwf[UUID], mwfr_input, INPUT_FILES_BAM, smaht_key)

    # Copy completed wfrs from previous mwfr
    for workflow_run in mwfr["workflow_runs"]:
        props_to_copy = ["job_id", "output", "status", "workflow_run"]
        if (
            workflow_run["name"] in ["ReplaceReadGroups", "somalier_extract"]
            and workflow_run["shard"] in dimension_mapping
        ):
            old_shard = dimension_mapping[workflow_run["shard"]]
            previous_workflow_run = get_wfr_from_mwfr(
                previous_mwfr, workflow_run["name"], old_shard
            )
            for prop in props_to_copy:
                workflow_run[prop] = previous_workflow_run[prop]
            if workflow_run["name"] == "ReplaceReadGroups":
                workflow_run["status"] = "completed"

    mwfr["tags"] = [get_tag_for_sample_identity_check(donor)]
    # mwfr["final_status"] = "stopped"

    post_response = ff_utils.post_metadata(mwfr, META_WORFLOW_RUN, smaht_key)
    mwfr_accession = post_response["@graph"][0]["accession"]
    print(f"Posted MetaWorkflowRun {mwfr_accession}.")


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
    samples = get_samples_from_library(library, smaht_key)
    sample_name = get_sample_name_for_mwfr(samples)

    # We are only retrieving the R2 reads and get the R1 read from the paired_with property
    search_filter = (
        # f"?type=UnalignedReads&read_pair_number=R2&file_sets.uuid={file_set[UUID]}"
        "?type=File"
        f"&status={UPLOADED}"
        "&read_pair_number=R2"
        f"&file_sets.uuid={file_set[UUID]}"
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
        get_mwfr_parameter_input_arg(SAMPLE_NAME, sample_name),
        get_mwfr_parameter_input_arg(LIBRARY_ID, library[ACCESSION]),
    ]
    return mwfr_input


def get_core_alignment_mwfr_input(file_set, file_input_arg, smaht_key):

    library = get_library_from_file_set(file_set, smaht_key)
    samples = get_samples_from_library(library, smaht_key)
    sample_name = get_sample_name_for_mwfr(samples)

    search_filter = (
        "?type=UnalignedReads" f"&file_sets.uuid={file_set[UUID]}" f"&status={UPLOADED}"
    )
    search_result = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)
    search_result.reverse()

    # Create files list for input args
    files = []
    for dim, file in enumerate(search_result):
        files.append({"file": file[UUID], "dimension": f"{dim}"})

    mwfr_input = [
        get_mwfr_file_input_arg(file_input_arg, files),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, sample_name),
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


