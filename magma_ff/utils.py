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

from dcicutils import ff_utils

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
                "Expected at most a single response but received multiple: %s"
                % result
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

def check_final_status(meta_workflow_run, valid_status):
    """Check if MetaWorkflowRun.final_status is valid.

    :param meta_workflow_run: MetaWorkflowRun[json]
    :type meta_workflow_run: dict
    :param valid_status: Status considered valid
    :type valid_status: list
    :return: Whether MetaWorkflowRun final_status is valid
    :rtype: bool
    """
    final_status = meta_workflow_run.get("final_status")
    if final_status in valid_status:
        result = True
    else:
        result = False
    return result
