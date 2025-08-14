#!/usr/bin/env python3

################################################
#
#
#
################################################

################################################
#   Libraries
################################################
import json, uuid
from pathlib import Path
from typing import Any, Dict, Sequence
from magma_smaht.metawfl import MetaWorkflow
from magma_smaht.constants import (
    UUID,
    CONSORTIA,
    SUBMISSION_CENTERS,
    STATUS,
    COMPLETED,
    DELETED,
    ACCESSION,
    WGS,
    RNASEQ,
    MWF_NAME_BAM_TO_CRAM
)

from packaging import version

from dcicutils import ff_utils


JsonObject = Dict[str, Any]

SMAHT_KEYS_FILE = Path.expanduser(Path("~/.smaht-keys.json")).absolute()


################################################
#   Functions
################################################
def make_embed_request(ids, fields, auth_key, single_item=False):
    """POST to embed API for retrieval of specified fields for given
    identifiers (from Postgres, not ES).

    :param ids: Item identifier(s)
    :type ids: str or list(str)
    :param fields: Fields to retrieve for identifiers
    :type fields: str or list(str)
    :param auth_key: Portal authorization key
    :type auth_key: dict
    :param single_item: Whether to return non-list result because only
         maximum one response is expected
    :type single_item: bool
    :return: Embed API response
    :rtype: list or dict or None
    """
    result = []
    if isinstance(ids, str):
        ids = [ids]
    if isinstance(fields, str):
        fields = [fields]
    id_chunks = chunk_ids(ids)
    server = auth_key.get("server")
    for id_chunk in id_chunks:
        post_body = {"ids": id_chunk, "fields": fields}
        embed_request = ff_utils.authorized_request(
            server + "/embed", verb="POST", auth=auth_key, data=json.dumps(post_body)
        ).json()
        result += embed_request
    if single_item:
        if not result:
            result = None
        elif len(result) == 1:
            result = result[0]
        else:
            raise ValueError(
                "Expected at most a single response but received multiple: %s" % result
            )
    return result


def chunk_ids(ids):
    """Split list into list of lists of maximum chunk size length.

    Embed API currently accepts max 5 identifiers, so chunk size is 5.

    :param ids: Identifiers to chunk
    :type ids: list
    :return: Chunked identifiers
    :rtype: list
    """
    result = []
    chunk_size = 5
    for idx in range(0, len(ids), chunk_size):
        result.append(ids[idx : idx + chunk_size])
    return result


def check_status(meta_workflow_run, valid_final_status=None):
    """Check if MetaWorkflowRun status is valid.

    If given valid final status, check MetaWorkflowRun.final_status
    as well.

    :param meta_workflow_run: MetaWorkflowRun[json]
    :type meta_workflow_run: dict
    :param valid_status: Final status considered valid
    :type valid_status: list
    :return: Whether MetaWorkflowRun final_status is valid
    :rtype: bool
    """
    item_status = meta_workflow_run.get("status", "deleted")
    if item_status not in ["obsolete", "deleted"]:
        result = True
        if valid_final_status:
            final_status = meta_workflow_run.get("final_status")
            if final_status not in valid_final_status:
                result = False
    else:
        result = False
    return result


class AuthorizationError(Exception):
    pass


def get_cgap_keys_path() -> Path:
    return SMAHT_KEYS_FILE


# TODO: dcicutils.creds_utils handles all of this
def get_auth_key(env_key: str) -> JsonObject:
    keys_path = get_cgap_keys_path()
    with keys_path.open() as file_handle:
        keys = json.load(file_handle)
    key = keys.get(env_key)
    if key is None:
        raise AuthorizationError(
            f"No key in {str(SMAHT_KEYS_FILE.absolute())} matches '{env_key}'"
        )
    return key

# The QC visualization assumes that sample identity MWFRs are tagged as follows:
def get_tag_for_sample_identity_check(donor_accession):
    return f"sample_identity_check_for_donor_{donor_accession}"


def keep_last_item(items: Sequence) -> Sequence:
    if len(items) <= 1:
        result = items
    elif len(items) > 1:
        result = items[-1:]
    return result


def get_file_set(fileset_accession, smaht_key):
    """Get the fileset from its accession

    Args:
        fileset_accession (str): fileset accession
        smaht_key (dict): SMaHT key

    Returns:
        dict: Fileset item from portal
    """
    return ff_utils.get_metadata(
        fileset_accession, add_on="frame=raw&datastore=database", key=smaht_key
    )


def get_library_from_file_set(file_set, smaht_key):
    """Get the library that is associated with a fileset

    Args:
        file_set(dicr): fileset from portal
        smaht_key (dict): SMaHT key

    Raises:
        Exception: Raises an exception when there are multiple libraries associated

    Returns:
        dict: Library item from portal
    """

    if len(file_set["libraries"]) > 1:
        raise Exception(f"Multiple libraries found for fileset {file_set['accession']}")
    library = ff_utils.get_metadata(
        file_set["libraries"][0], add_on="frame=raw&datastore=database", key=smaht_key
    )
    return library


def get_samples_from_library(library, smaht_key):
    """Get the samples that are associated with a library

    Args:
        library (dict): library item from portal
        smaht_key (dict): SMaHT key

    Returns:
        list: Sample items from portal
    """
    sample_uuids = []
    analytes = library.get("analytes", [])
    for analyte in analytes:
        item = ff_utils.get_metadata(
            analyte, add_on="frame=raw&datastore=database", key=smaht_key
        )
        sample_uuids += item.get("samples", [])

    samples = []
    for uuid in sample_uuids:
        sample = ff_utils.get_metadata(
            uuid, add_on="frame=raw&datastore=database", key=smaht_key
        )
        samples.append(sample)

    return samples

def get_sample_sources_from_sample(sample, smaht_key):
    """Get sample sources from a sample

    Args:
        sample (dict): Sample item from portal
        smaht_key (dict): SMaHT key

    Returns:
        list[dict]: sample source items from portal
    """
    sample_sources = sample.get("sample_sources", [])
    sample_sources_items = []
    for sample_source in sample_sources:
        item = get_item(sample_source, smaht_key, frame='embedded')
        sample_sources_items.append(item)
    return sample_sources_items


def get_sample_name_for_mwfr(samples):
    """Get the sample_name that is added to the MWFR

    Args:
        samples (list): List of samples from portal
    """
    accessions = map(lambda s: s["accession"], samples)
    return "_".join(accessions)


def get_library_preparation_from_library(library, smaht_key):
    """Get the library preparation that is associated with a library

    Args:
        library (dict): library item from portal
        smaht_key (dict): SMaHT key

    Raises:
        Exception: Raises an exception when there is no library preparation

    Returns:
        dict: library_preparation item from portal
    """
    library_preparation = library.get("library_preparation")
    if not library_preparation:
        raise Exception(
            f"No library preparation found for library {library['accession']}"
        )

    library_preparation_item = ff_utils.get_metadata(
        library_preparation, add_on="frame=raw&datastore=database", key=smaht_key
    )
    return library_preparation_item


def get_donors_from_mwfr(mwfr, smaht_key):
    """Get the donor that is associated with the MWFR

    Args:
        mwfr (dict): MWFR item from portal
        smaht_key (dict): SMaHT key

    Returns:
        dict: Donor item from portal
    """
    file_sets = mwfr.get("file_sets")
    if not file_sets:
        raise Exception(f"No file sets found for MWF {mwfr['uuid']}")

    file_set =  get_file_set(file_sets[0]['uuid'], smaht_key)
    library = get_library_from_file_set(file_set, smaht_key)
    samples = get_samples_from_library(library, smaht_key)
    donor_ids = []
    for sample in samples:
        sample_sources = get_sample_sources_from_sample(sample, smaht_key)
        for sample_source in sample_sources:
            
            if sample_source.get("code") == "HAPMAP6":
                continue

            if "donor" in sample_source:
                donor_ids.append(sample_source['donor']['uuid'])
            elif "cell_line" in sample_source:
                cell_lines = sample_source["cell_line"]
                for cell_line in cell_lines:
                    cell_line_item = get_item(cell_line['uuid'], smaht_key, frame='embedded')
                    if "donor" in cell_line_item:
                        donor_ids.append(cell_line_item['donor']['uuid'])
                    elif "source_donor" in cell_line_item:
                        donor_ids.append(cell_line_item['source_donor']['uuid'])
                    else:
                        print(f"Can't get donor from sample source {sample_source['uuid']}")
                        continue
            else:
                print(f"Can't get donor from sample source {sample_source['uuid']}")
                continue
               

    donors = []
    donor_ids = list(set(donor_ids))
    for donor_id in donor_ids:
        donor = get_item(donor_id, smaht_key, frame='object')
        donors.append(donor)
    return donors


def get_latest_mwf(mwf_name, smaht_key):
    """Get the latest version of the MWF with name `mwf_name`

    Args:
        mwf_name (string): Name of the MWF
        smaht_key (dcit): SMaHT key

    Returns:
        dict: MWF item from portal
    """
    query = f"/search/?type=MetaWorkflow&name={mwf_name}"
    search_results = ff_utils.search_metadata(query, key=smaht_key)

    if len(search_results) == 0:
        return None

    latest_result = search_results[0]
    if len(search_results) == 1:
        return latest_result

    # There are multiple MWFs. Get the latest version
    for search_result in search_results:
        if version.parse(latest_result["version"]) < version.parse(
            search_result["version"]
        ):
            latest_result = search_result
    return latest_result


def get_mwfr_file_input_arg(argument_name, files):
    return {"argument_name": argument_name, "argument_type": "file", "files": files}


def get_mwfr_parameter_input_arg(argument_name, value):
    return {
        "argument_name": argument_name,
        "argument_type": "parameter",
        "value": value,
    }

def get_wfr_from_mwfr(mwfr, wfr_name, shard):
    workflow_run = next(
        (
            item
            for item in mwfr["workflow_runs"]
            if item["name"] == wfr_name and item["shard"] == str(shard)
        ),
        None,
    )
    if not workflow_run:
        raise Exception(
            f"No {wfr_name} workflow run not found for shard {shard}"
        )
    return workflow_run

def get_latest_somalier_run_for_donor(donor_accession, key):
    search_filter = (
        "?type=MetaWorkflowRun"
        f"&meta_workflow.name=sample_identity_check"
        f"&tags={get_tag_for_sample_identity_check(donor_accession)}"
        "&final_status=completed"
        "&sort=-date_created"
        "&limit=1"
    )
    return ff_utils.search_metadata(f"/search/{search_filter}", key=key)


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

    metawf_meta = get_item(metawf_uuid, ff_key)

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
        print("More than 2 input dimensions are currently not supported")
        exit()


def has_bam_to_cram_mwfr(fileset, key):
    """Check if the fileset has a BAM to CRAM workflow run.

    Args:
        fileset (dict): Fileset item from portal

    Returns:
        bool: True if BAM to CRAM workflow run exists, False otherwise
    """
    mwfrs = fileset.get("meta_workflow_runs", [])
    for mwfr in mwfrs:
        mwf_item = get_item_es(mwfr['meta_workflow'][UUID], key)
        if mwf_item['name'] == MWF_NAME_BAM_TO_CRAM:
            return True
       
    return False

def get_alignment_mwfr(fileset, key):
    mwfrs = fileset.get("meta_workflow_runs", [])
    results = []
    for mwfr in mwfrs:
        mwfr_item = get_item_es(mwfr[UUID], key, frame='embedded')
        if mwfr_item[STATUS] == DELETED or mwfr_item["final_status"] != COMPLETED:
            continue
        categories = mwfr_item["meta_workflow"]["category"]

        if "Alignment" in categories:
            results.append(mwfr_item)
    if len(results) == 1:
        return results[0]
    elif len(results) > 1:
        mwfr = results[-1]  # Take the last one if there are multiple
        print(
            f"Warning: Fileset {fileset[ACCESSION]} has multiple alignment MWFRs. Taking last one: {mwfr[ACCESSION]}"
        )
        return mwfr
    return None

def get_final_output_file(mwfr, assay, key):
    """Get the final output file from a MetaWorkflowRun based on assay.

    Args:
        mwfr (dict): MetaWorkflowRun item from portal
        assay (str): Processing mode (WGS or RNASEQ)
        key (dict): Portal authorization key
        
    Returns:
        dict: Final output file item from portal, or None if not found
        
    Raises:
        ValueError: If assay is not supported
    """
    if assay not in [WGS, RNASEQ]:
        raise ValueError(f"Unsupported assay: {assay}. Supported assays are: {WGS}, {RNASEQ}")

    mwf_version = version.parse(mwfr["meta_workflow"]["version"])
    threshold_version = version.parse("0.3.0")

    # Define workflow mapping based on assay and version
    if assay == WGS:
        if mwf_version <= threshold_version:
            target_workflow = "samtools_merge"
        else:
            target_workflow = "bam_to_cram"
    elif assay == RNASEQ:
        target_workflow = "sentieon_Dedup"
    
    # Find the target workflow run
    for workflow_run in mwfr["workflow_runs"]:
        if workflow_run["name"] == target_workflow:
            file_uuid = workflow_run["output"][0]["file"][UUID]
            file = get_item_es(file_uuid, key, frame='embedded')
            if file["output_status"] == "Final Output":
                return file
    
    return None

def get_item(identifier, key, frame="raw"):
    return ff_utils.get_metadata(
        identifier, add_on=f"frame={frame}&datastore=database", key=key
    )

def get_item_es(identifier, key, frame="raw"):
    return ff_utils.get_metadata(
        identifier, add_on=f"frame={frame}", key=key
    )
