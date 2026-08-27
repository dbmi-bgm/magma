#!/usr/bin/env python3

################################################
#
#
#
################################################

################################################
#   Libraries
################################################
import pprint
import functools
import json, uuid
from pathlib import Path
from typing import Any, Dict, List, Sequence
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
               
    donors_list = get_all_donors("object", smaht_key)
    donors_from_portal = {}
    for donor in donors_list:
        donors_from_portal[donor[UUID]] = donor

    donors = []
    donor_ids = list(set(donor_ids))
    for donor_id in donor_ids:
        donors.append(donors_from_portal[donor_id])
    return donors

def get_all_donors(frame, smaht_key):
    """Get all donors in the portal (cached version)

    Args:
        frame (str): Frame type for the request
        smaht_key (dict): SMaHT key

    Returns:
        list: List of donor items from portal
    """
    serialized_key = _serialize_key(smaht_key)
    return _get_all_donors_cached(frame, serialized_key)

@functools.lru_cache(maxsize=128)
def _get_all_donors_cached(frame, serialized_key):
    """Internal cached function that works with hashable parameters."""
    smaht_key = json.loads(serialized_key)
    
    query = f"/search/?type=Donor&field=uuid&limit=2000"
    search_results = ff_utils.search_metadata(query, key=smaht_key)

    donors = []
    for donor in search_results:
        donor_item = get_item(donor[UUID], smaht_key, frame=frame)
        donors.append(donor_item)
    return donors


def get_latest_mwf(mwf_name, smaht_key):
    """Get the latest version of the MWF with name `mwf_name`

    Args:
        mwf_name (string): Name of the MWF
        smaht_key (dcit): SMaHT key

    Returns:
        dict: MWF item from portal
    """
    #query = f"/search/?type=MetaWorkflow&version=0.3.0&name={mwf_name}"
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

    input_structure = None
    for arg in input:
        if arg["argument_name"] == input_arg:
            input_structure = generate_input_structure(arg["files"])
    if input_structure is None:
        raise ValueError(
            f"There is no input argument {input_arg} to calculate the input"
            " structure from. Available input arguments:"
            f" {', '.join(arg['argument_name'] for arg in input)}."
        )

    # Steps that scatter over an input argument other than input_arg get their
    # shards from the structure of that argument
    input_structures = get_input_structures(metawf_meta, input)

    mwf = MetaWorkflow(metawf_meta)
    mwfr = mwf.write_run(input_structure, input_structures=input_structures)

    mwfr[UUID] = str(uuid.uuid4())
    mwfr[CONSORTIA] = consortia
    mwfr[SUBMISSION_CENTERS] = submission_centers
    mwfr["input"] = input

    return mwfr


def get_scattered_argument_names(metawf_meta: JsonObject) -> List[str]:
    """Get the names of the input arguments of a MetaWorkflow[portal] that are
    scattered over by at least one of its workflows.

    Arguments that are matched to the output of a previous workflow (source) are
    not returned, they scatter over that output and not over the input.

    :param metawf_meta: MetaWorkflow[portal]
    :type metawf_meta: dict
    :return: Names of the input arguments that are scattered over
    :rtype: list(str)
    """
    argument_names = []
    for workflow in metawf_meta.get("workflows", []):
        for arg in workflow.get("input", []):
            if not arg.get("scatter") or arg.get("source"):
                continue
            if arg.get("argument_type") != "file":
                continue
            # source_argument_name is the name of the argument
            #   in the input of the MetaWorkflowRun, if specified
            name = arg.get("source_argument_name") or arg.get("argument_name")
            if name not in argument_names:
                argument_names.append(name)
    return argument_names


def get_input_structures(
    metawf_meta: JsonObject, input: Sequence[JsonObject]
) -> Dict[str, List[Any]]:
    """Calculate the input structure of every input argument of a
    MetaWorkflowRun that is scattered over by one of the workflows.

    :param metawf_meta: MetaWorkflow[portal] the MetaWorkflowRun is derived from
    :type metawf_meta: dict
    :param input: Input arguments as list, where each argument is a dictionary
    :type input: list(dict)
    :return: Input structures by input argument name
    :rtype: dict
    """
    scattered_argument_names = get_scattered_argument_names(metawf_meta)

    input_structures = {}
    for arg in input:
        argument_name = arg.get("argument_name")
        if argument_name not in scattered_argument_names:
            continue
        if arg.get("argument_type") != "file" or not arg.get("files"):
            continue
        try:
            input_structures[argument_name] = generate_input_structure(arg["files"])
        except ValueError as e:
            raise ValueError(
                f"Cannot calculate the input structure of the scattered input"
                f" argument {argument_name}: {e}"
            ) from e
    return input_structures


def generate_input_structure(files: Sequence[Dict[str, Any]]) -> List[Any]:
    """Calculate the input structure of a MetaWorkflowRun from the input files
    of an input argument that is scattered over.

    The `dimension` of every file is parsed and validated, i.e. the
    dimensionality of the input structure is not inferred from the first file
    alone. Files with mixed dimensionalities, and duplicate, gapped or negative
    dimensions, raise a ValueError instead of resulting in an input structure
    that does not match the given files.

    :param files: Files of a single input argument, e.g.
        [{'file': 'UUID', 'dimension': '0'}, ...]
    :type files: list(dict)
    :return: Input structure with maximum scatter, 1 or 2 dimensions
    :rtype: list
    :raises ValueError: If the dimensions of the given files don't describe a
        complete 1 or 2 dimensional input structure
    """
    if not files:
        raise ValueError(
            "Cannot generate an input structure from an empty list of files."
        )

    def file_description(file, dimension):
        return f"file {file.get('file', '<unknown>')} (dimension {dimension!r})"

    # Parse the dimension of every file. Files without a dimension yield an
    # empty list of indices, i.e. no dimension at all.
    parsed = []  # [(file, dimension as given, [dimension indices]), ...]
    for file in files:
        dimension = file.get("dimension")
        dimension = "" if dimension is None else str(dimension).strip()
        indices = []
        for component in dimension.split(",") if dimension else []:
            component = component.strip()
            if not component.isdigit():
                raise ValueError(
                    f"Invalid dimension component {component!r} in "
                    f"{file_description(file, dimension)}. A dimension must be a"
                    " comma separated list of non-negative integers."
                )
            indices.append(int(component))
        parsed.append((file, dimension, indices))

    # All files must have the same number of dimensions, i.e. the first file
    # is not assumed to be representative of the input structure.
    first_file, first_dimension, first_indices = parsed[0]
    num_dimensions = len(first_indices)
    for file, dimension, indices in parsed[1:]:
        if len(indices) != num_dimensions:
            raise ValueError(
                f"Inconsistent dimensions: {file_description(first_file, first_dimension)}"
                f" has {num_dimensions} dimension(s), but "
                f"{file_description(file, dimension)} has {len(indices)}. All"
                " files of an input argument must have the same number of"
                " dimensions."
            )

    if num_dimensions == 0:
        # No file has a dimension, the files are treated as a positional
        # 1 dimensional list.
        if len(files) > 1:
            print(
                warning_text(
                    f"WARNING: None of the {len(files)} input files has a"
                    " dimension. They are treated as a 1 dimensional list, in"
                    " the given order."
                )
            )
        return list(range(len(files)))
    elif num_dimensions == 1:
        _validate_dimension_indices(
            [indices[0] for _, _, indices in parsed], "input files"
        )
        return list(range(len(files)))
    elif num_dimensions == 2:
        dimensions_dict = {}
        for _, _, indices in parsed:
            dimensions_dict.setdefault(indices[0], []).append(indices[1])
        # Example for dimensions_dict: {0: [0, 1], 1: [0, 1, 2]}
        _validate_dimension_indices(list(dimensions_dict.keys()), "first dimension")
        for index, inner_indices in dimensions_dict.items():
            _validate_dimension_indices(
                inner_indices, f"second dimension of index {index}"
            )
        # The indices within a sublist are kept in the order in which they
        # appear in `files`
        return [dimensions_dict[index] for index in sorted(dimensions_dict)]
    else:
        raise ValueError(
            "Input structures with more than 2 dimensions are currently not"
            f" supported (got {num_dimensions} dimensions, e.g."
            f" {file_description(first_file, first_dimension)})."
        )


def _validate_dimension_indices(indices: Sequence[int], description: str) -> None:
    """Check that the given dimension indices are a permutation of
    0, ..., len(indices)-1, i.e. that they are complete and free of duplicates.

    :param indices: Dimension indices to validate
    :type indices: list(int)
    :param description: Description of the validated indices for the error message
    :type description: str
    :raises ValueError: If the indices are not complete or contain duplicates
    """
    expected = list(range(len(indices)))
    if sorted(indices) != expected:
        raise ValueError(
            f"The dimensions of the {description} are not a complete range"
            f" without duplicates: expected indices {expected}, got"
            f" {sorted(indices)}."
        )


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

def _serialize_key(key_dict):
    """Convert dictionary key to a hashable string for caching."""
    return json.dumps(key_dict, sort_keys=True)

class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def ok_blue_text(text: str) -> str:
    return f"{bcolors.OKBLUE}{text}{bcolors.ENDC}"


def ok_green_text(text: str) -> str:
    return f"{bcolors.OKGREEN}{text}{bcolors.ENDC}"


def bold_text(text: str) -> str:
    return f"{bcolors.BOLD}{text}{bcolors.ENDC}"


def warning_text(text: str) -> str:
    return f"{bcolors.WARNING}{text}{bcolors.ENDC}"


def fail_text(text: str) -> str:
    return f"{bcolors.FAIL}{text}{bcolors.ENDC}"
