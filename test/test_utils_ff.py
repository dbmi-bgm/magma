import mock
import pytest

from magma_ff.utils import check_status, chunk_ids, make_embed_request


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
