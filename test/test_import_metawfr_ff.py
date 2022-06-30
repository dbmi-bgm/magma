import mock

import pytest

from magma_ff.import_metawfr import (
    add_new_meta_workflow_run_uuid_to_copied_items,
    add_to_patch,
)


ASSOCIATED_KEY = "associated_meta_workflow_runs"
META_WORKFLOW_RUN_NO_WORKFLOW_RUNS = {"uuid": "1234", "foo": "bar"}
WORKFLOW_RUN_UUID_1 = "wfr_uuid_1"
WORKFLOW_RUN_UUID_2 = "wfr_uuid_2"
WORKFLOW_RUN_UUIDS = [WORKFLOW_RUN_UUID_1, WORKFLOW_RUN_UUID_2]
META_WORKFLOW_RUN = {
    "uuid": "2345",
    "workflow_runs": [
        {"workflow_run": WORKFLOW_RUN_UUID_1},
        {"workflow_run": WORKFLOW_RUN_UUID_2},
    ],
}
FILE_UUID_1 = "file_uuid_1"
FILE_UUID_2 = "file_uuid_2"
QC_UUID_1 = "qc_uuid_1"
QC_UUID_2 = "qc_uuid_2"
OUTPUT_ITEM_UUIDS = [FILE_UUID_1, FILE_UUID_2, QC_UUID_1, QC_UUID_2]
EMBED_RESULT = [
    {
        "uuid": WORKFLOW_RUN_UUID_1,
        "output_files": [
            {"value": {"uuid": FILE_UUID_1}, "value_qc": {"uuid": QC_UUID_1}},
            {"value": {"uuid": FILE_UUID_2}},
        ]
    },
    {
        "uuid": WORKFLOW_RUN_UUID_2,
        "output_files": [{"value_qc": {"uuid": QC_UUID_2}}]
    },
]
EXPECTED_UUIDS = WORKFLOW_RUN_UUIDS + OUTPUT_ITEM_UUIDS


@pytest.mark.parametrize(
    "meta_workflow_run,expected_embed_ids,embed_result,expected_uuids,patch_success",
    [
        ({}, [], [], [], True),
        ({}, [], [], [], False),
        (META_WORKFLOW_RUN_NO_WORKFLOW_RUNS, [], [], [], True),
        (META_WORKFLOW_RUN_NO_WORKFLOW_RUNS, [], [], [], False),
        (META_WORKFLOW_RUN, WORKFLOW_RUN_UUIDS, [], [], True),
        (META_WORKFLOW_RUN, WORKFLOW_RUN_UUIDS, [], [], False),
        (META_WORKFLOW_RUN, WORKFLOW_RUN_UUIDS, EMBED_RESULT, EXPECTED_UUIDS, True),
        (META_WORKFLOW_RUN, WORKFLOW_RUN_UUIDS, EMBED_RESULT, EXPECTED_UUIDS, False),
    ],
)
def test_add_new_meta_workflow_run_uuid_to_copied_items(
    meta_workflow_run, expected_embed_ids, embed_result, expected_uuids, patch_success
):
    """Test PATCH with addition of new MWFR UUID to all output items of
    copied steps from previous MWFR.
    """
    auth_key = "mocked_out"
    with mock.patch(
        "magma_ff.import_metawfr.make_embed_request", return_value=embed_result
    ) as mock_embed_request:
        patch_side_effect = Exception("Test raised an exception")
        if patch_success:
            patch_side_effect = None
        with mock.patch(
            "magma_ff.import_metawfr.ff_utils.patch_metadata",
            side_effect=patch_side_effect,
        ) as mock_patch_metadata:
            success, error = add_new_meta_workflow_run_uuid_to_copied_items(
                meta_workflow_run, auth_key
            )
            if patch_success:
                assert set(expected_uuids) == set(success)
            else:
                assert set(expected_uuids) == set(error.keys())
            mock_embed_request.assert_called_once()
            if expected_embed_ids:
                call_args = mock_embed_request.call_args
                assert expected_embed_ids == call_args[0][0]  # Should be first arg
            assert len(expected_uuids) == len(mock_patch_metadata.call_args_list)


@pytest.mark.parametrize(
    "patch_dict,item_properties,meta_workflow_run_uuid,expected",
    [
        ({}, {}, "foo", {}),
        ({}, {"fu": "bar"}, "foo", {}),
        ({}, {"uuid": "bar"}, "foo", {"bar": {ASSOCIATED_KEY: ["foo"]}}),
        (
            {},
            {"uuid": "bar", ASSOCIATED_KEY: ["fu"]},
            "foo",
            {"bar": {ASSOCIATED_KEY: ["fu", "foo"]}},
        ),
        (
            {"bar": {"fu": "bur"}},
            {"uuid": "bar"},
            "foo",
            {"bar": {ASSOCIATED_KEY: ["foo"]}},
        ),
    ],
)
def test_add_to_patch(patch_dict, item_properties, meta_workflow_run_uuid, expected):
    """Test update of dictionary with item UUID and PATCH body."""
    add_to_patch(patch_dict, item_properties, meta_workflow_run_uuid)
    assert patch_dict == expected
