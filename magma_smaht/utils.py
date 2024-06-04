#!/usr/bin/env python3

################################################
#
#
#
################################################

################################################
#   Libraries
################################################
import json
from pathlib import Path
from typing import Any, Dict, Sequence
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
        result.append(ids[idx: idx + chunk_size])
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

def get_sample_from_library(library, smaht_key):
    """Get the sample that is associated with a library

    Args:
        library (dict): library item from portal
        smaht_key (dict): SMaHT key

    Raises:
        Exception: Raises an exception when there are multiple samples associated

    Returns:
        dict: Sample item from portal
    """
    samples = []
    analytes = library.get("analytes", [])
    for analyte in analytes:
        item = ff_utils.get_metadata(
            analyte, add_on="frame=raw&datastore=database", key=smaht_key
        )
        samples += item.get("samples", [])
    if len(set(samples)) > 1:
        raise Exception(f"Multiple samples found for library {library['accession']}")
    sample = ff_utils.get_metadata(
        samples[0], add_on="frame=raw&datastore=database", key=smaht_key
    )
    return sample


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
        if version.parse(latest_result["version"]) < version.parse(search_result["version"]):
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
