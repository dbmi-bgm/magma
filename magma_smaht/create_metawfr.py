#!/usr/bin/env python3

################################################
#
#   Functions to create a MetaWorkflowRun
#
################################################

################################################
#   Libraries
################################################
import click
import json, uuid
from dcicutils import ff_utils

# magma
from magma_smaht.metawfl import MetaWorkflow
from magma_smaht.utils import get_latest_mwf, get_library_from_file_set, get_sample_from_library, get_mwfr_file_input_arg, get_mwfr_parameter_input_arg, get_auth_key

# MetaWorkflow names used to get latest version. 
# We assume that they don't change!
MWF_NAME_ILLUMINA = "Illumina_alignment_GRCh38"
MWF_NAME_ONT = "ONT_alignment_GRCh38"
MWF_NAME_PACBIO = "PacBio_alignment_GRCh38"
MWF_NAME_HIC = "Hi-C_alignment_GRCh38"
MWF_NAME_FASTQC = "short_reads_FASTQ_quality_metrics"


################################################
#   Functions
################################################

@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-f", 
    "--fileset-accession", 
    required=True, 
    type=str, 
    help="Fileset accession"
)
@click.option(
    "-l", 
    "--length-required", 
    required=True, 
    type=int, 
    help="Requried length (can be obtained from FastQC output)"
)
@click.option(
    "-e", 
    "--auth-env", 
    required=True, 
    type=str, 
    help="Requried length (can be obtained from FastQC output)"
)
def mwfr_illumina_alignment(fileset_accession, length_required, auth_env):
    """Creates a MetaWorflowRun item in the portal for Illumina alignemnt of submitted files within a file set
    """

    smaht_key = get_auth_key(auth_env)

    mwf = get_latest_mwf(MWF_NAME_ILLUMINA, smaht_key)
    mwf_uuid = mwf["uuid"]
    print(f"Using MetaWorkflow {mwf["accession"]} ({mwf["aliases"][0]})")

    # Collect Input
    library = get_library_from_file_set(
        fileset_accession, smaht_key
    )
    library_accession = library["accession"]
    sample = get_sample_from_library(library, smaht_key)
    sample_accession = sample["accession"]

    # We are only retrieving the R2 reads and get the R1 read from the paired_with property
    search_filter = (
        f"?type=UnalignedReads&read_pair_number=R2&file_sets.accession={fileset_accession}"
    )
    reads_r2 = ff_utils.search_metadata(f"/search/{search_filter}", key=smaht_key)

    # Create files list for input args
    files_r1, files_r2 = [], []
    for dim, file_r2 in enumerate(reads_r2):
        files_r2.append({"file": file_r2["uuid"], "dimension": f"{dim}"})
        files_r1.append(
            {"file": reads_r2["paired_with"]["uuid"], "dimension": f"{dim}"}
        )

    mwfr_input = [
        get_mwfr_file_input_arg("input_files_r1_fastq_gz", files_r1),
        get_mwfr_file_input_arg("input_files_r2_fastq_gz", files_r2),
        get_mwfr_parameter_input_arg("sample_name", sample_accession),
        get_mwfr_parameter_input_arg("library_id", library_accession),
        get_mwfr_parameter_input_arg("length_required", length_required),
    ]

    input_arg = "input_files_r1_fastq_gz"
    mwfr = mwfr_from_input(mwf_uuid, mwfr_input, input_arg, smaht_key)
    #mwfr['final_status'] = 'stopped'
    post_response = ff_utils.post_metadata(mwfr, "MetaWorkflowRun", smaht_key)
    mwfr_accession = post_response["@graph"][0]["accession"]
    print(f"Posted alignment MetaWorkflowRun {mwfr_accession} for Fileset {fileset_accession}.")


@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-f", 
    "--fileset-accession", 
    required=True, 
    type=str, 
    help="Fileset accession"
)
@click.option(
    "-e", 
    "--auth-env", 
    required=True, 
    type=str, 
    help="Requried length (can be obtained from FastQC output)"
)
def mwfr_fastqc(fileset_accession, auth_env):

    smaht_key = get_auth_key(auth_env)

    mwf = get_latest_mwf(MWF_NAME_FASTQC, smaht_key)
    mwf_uuid = mwf["uuid"]
    print(f"Using MetaWorkflow {mwf["accession"]} ({mwf["aliases"][0]})")

    # Get unaligned reads in fileset that don't have already QC
    search_filter = f"?&file_sets.accession={fileset_accession}&type=UnalignedReads&file_format.display_title=fastq_gz&quality_metrics=No+value"
    files_to_run = ff_utils.search_metadata(
    (
        f"search/{search_filter}"
    ),
    key=smaht_key)

    if len(files_to_run) == 0:
        print(f"No files to run for search {search_filter}")
        return
    
    files_to_run = [files_to_run["uuid"] for file in files_to_run]

    files_input = []
    for dim, file in enumerate(files_to_run):
        files_input.append({"file": file["uuid"], "dimension": f"{dim}"})

    input_arg = "input_files_fastq_gz"
    mwfr_input = [get_mwfr_file_input_arg(input_arg, files_input)]
    mwfr = mwfr_from_input(mwf_uuid, mwfr_input, input_arg, smaht_key)
    #mwfr['final_status'] = 'stopped'
    post_response = ff_utils.post_metadata(mwfr, "MetaWorkflowRun", smaht_key)
    mwfr_accession = post_response["@graph"][0]["accession"]
    print(f"Posted alignment MetaWorkflowRun {mwfr_accession} for Fileset {fileset_accession}.")


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
