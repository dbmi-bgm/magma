import json

from dcicutils import ff_utils


def make_embed_request(ids, fields, auth_key):
    """"""
    result = []
    single_item = False
    if isinstance(ids, str):
        ids = [ids]
    if isinstance(fields, str):
        fields = [fields]
    if len(ids) == 1:
        single_item = True
    id_chunks = chunk_ids(ids)
    server = auth_key["server"]
    for id_chunk in id_chunks:
        post_body = {"ids": id_chunk, "fields": fields}
        embed_request = ff_utils.authorized_request(
            server + "/embed", verb="POST", auth=auth_key, data=json.dumps(post_body)
        ).json()
        result += embed_request
    if single_item:
        if embed_request:
            result = embed_request[0]
        else:
            result = None
    return result


def chunk_ids(ids):
    """"""
    result = []
    chunk_size = 5
    for idx in range(0, len(ids), chunk_size):
        result.append(ids[idx: idx + chunk_size])
    return result
