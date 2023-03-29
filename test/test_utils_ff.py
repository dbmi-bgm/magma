import json
import mock
import pytest
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterator

from magma_ff import utils as magma_ff_utils_module
from magma_ff.utils import (
    JsonObject,
    check_status,
    chunk_ids,
    get_auth_key,
    make_embed_request,
    AuthorizationError,
)
from .utils import patch_context


class ReturnValue:
    """Simple class to mock ff_utils.authorized_request"""

    def __init__(self, return_value):
        self.return_value = return_value

    def json(self):
        return self.return_value


@pytest.mark.parametrize(
    "ids,fields,single_item,return_value,expected_mock_calls,expected",
    [
        ([], [], False, None, 0, []),
        ("some_id", "some_field", False, ReturnValue([]), 1, []),
        ("some_id", "some_field", True, ReturnValue([]), 1, None),
        ("some_id", "some_field", False, ReturnValue(["found"]), 1, ["found"]),
        ("some_id", "some_field", True, ReturnValue(["found"]), 1, "found"),
        (["some_id", "another_id"], "some_field", False, ReturnValue([]), 1, []),
        (
            ["some_id", "another_id"],
            "some_field",
            False,
            ReturnValue(["found"]),
            1,
            ["found"],
        ),
        (["1", "2", "3", "4", "5", "6"], "some_field", False, ReturnValue([]), 2, []),
    ],
)
def test_make_embed_request(
    ids, fields, single_item, return_value, expected_mock_calls, expected
):
    """Test POST to embed API.

    Request is mocked out and untested here.
    """
    with mock.patch(
        "magma_ff.utils.ff_utils.authorized_request", return_value=return_value
    ) as mocked_request:
        auth_key = {"server": "some_server"}
        result = make_embed_request(ids, fields, auth_key, single_item=single_item)
        assert result == expected
        assert len(mocked_request.call_args_list) == expected_mock_calls


@pytest.mark.parametrize(
    "ids,expected",
    [
        ([], []),
        (["id"], [["id"]]),
        (["1", "2", "3", "4", "5"], [["1", "2", "3", "4", "5"]]),
        (["1", "2", "3", "4", "5", "6"], [["1", "2", "3", "4", "5"], ["6"]]),
    ],
)
def test_chunk_ids(ids, expected):
    """Test chunking of list into list of lists of size no larger than
    chunk size.
    """
    result = chunk_ids(ids)
    assert result == expected


@pytest.mark.parametrize(
    "meta_workflow_run,valid_status,expected",
    [
        ({}, [], False),
        ({"status": "passing"}, [], True),
        ({"status": "passing", "foo": "bar"}, ["running"], False),
        ({"status": "passing", "final_status": "foo"}, ["completed", "running"], False),
        ({"status": "passing", "final_status": "running"}, ["running"], True),
        ({"final_status": "running"}, ["running"], False),
        ({"status": "passing", "final_status": "running"}, ["completed"], False),
        (
            {"status": "passing", "final_status": "running"},
            ["completed", "running"],
            True,
        ),
    ],
)
def test_check_status(meta_workflow_run, valid_status, expected):
    """Test validating MetaWorkflowRun status and final status."""
    result = check_status(meta_workflow_run, valid_status)
    assert result == expected


SOME_AUTH = {"auth": "bar"}
SOME_AUTH_KEY = {"foo": SOME_AUTH}


@contextmanager
def patch_cgap_keys_path(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        magma_ff_utils_module, "get_cgap_keys_path", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def get_named_tmp_file_path_with_content(contents: Any) -> NamedTemporaryFile:
    with NamedTemporaryFile("w") as tmp:
        tmp.write(contents)
        tmp.seek(0)
        yield Path(tmp.name).absolute()


@pytest.mark.parametrize(
    "env_key,exception_expected,expected",
    [
        ("fu", True, None),
        ("foo", False, SOME_AUTH),
    ],
)
def test_get_auth_key(
    env_key: str, exception_expected: bool, expected: JsonObject
) -> None:
    tmp_contents = json.dumps(SOME_AUTH_KEY)
    with get_named_tmp_file_path_with_content(tmp_contents) as tmp_path:
        with patch_cgap_keys_path(return_value=tmp_path):
            if exception_expected:
                with pytest.raises(AuthorizationError):
                    get_auth_key(env_key)
            else:
                result = get_auth_key(env_key)
                assert result == expected
