import mock
import pytest
from contextlib import contextmanager
from test.utils import patch_context
from typing import Iterator  # thx Doug mwehehe

import datetime

# import json

from magma.magma_constants import *
from magma_ff.utils import JsonObject
import magma_ff.create_metawflrun_handler as create_metaworkflow_run_handler_module
from magma_ff.create_metawflrun_handler import (
    MetaWorkflowRunHandlerFromItem,
    MetaWorkflowRunHandlerCreationError,
)


@contextmanager
def patch_post_metadata(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_metaworkflow_run_handler_module.ff_utils, "post_metadata", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_make_embed_request(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_metaworkflow_run_handler_module, "make_embed_request", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_retrieve_associated_item_dict(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_metaworkflow_run_handler_module.MetaWorkflowRunHandlerFromItem,
        "_retrieve_associated_item_dict",
        **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_retrieve_meta_workflow_handler_dict(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_metaworkflow_run_handler_module.MetaWorkflowRunHandlerFromItem,
        "_retrieve_meta_workflow_handler_dict",
        **kwargs
    ) as mock_item:
        yield mock_item

@contextmanager
def patch_create_meta_workflow_runs_array(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_metaworkflow_run_handler_module.MetaWorkflowRunHandlerFromItem,
        "_create_meta_workflow_runs_array",
        **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_generate_uuid4(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_metaworkflow_run_handler_module.uuid, "uuid4", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_create_meta_workflow_run_handler_dict(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_metaworkflow_run_handler_module.MetaWorkflowRunHandlerFromItem,
        "_create_meta_workflow_run_handler",
        **kwargs
    ) as mock_item:
        yield mock_item


TODAY = datetime.date.today().isoformat()
TESTER_PROJECT = "project_tester"
TESTER_INSTITUTION = "institution_tester"
TESTER_TITLE = "title_tester"

ASSOCIATED_ITEM_UUID = "associated_item_tester_uuid"
ASSOCIATED_ITEM_SIMPLE_DICT = {
    UUID: ASSOCIATED_ITEM_UUID,
    PROJECT: TESTER_PROJECT,
    INSTITUTION: TESTER_INSTITUTION,
    # META_WORKFLOW_RUNS: [] # in the case that we wanna add dup flag back in future development
    # TODO: and patching this array? should be handled in run mwfr handler
}

META_WORKFLOW_HANDLER_UUID = "meta_workflow_handler_tester_uuid"
META_WORKFLOW_HANDLER_SIMPLE_DICT = {
    UUID: META_WORKFLOW_HANDLER_UUID,
    TITLE: TESTER_TITLE,
    META_WORKFLOWS: [], #TODO: check my long note in magma/metawfl_handler.py
}

META_WORKFLOW_RUN_HANDLER_UUID = "meta_workflow_run_handler_tester_uuid"
AUTH_KEY = {"key": "foo"}
META_WORKFLOW_RUN_HANDLER_SIMPLE_DICT = {
    UUID: META_WORKFLOW_RUN_HANDLER_UUID,
    PROJECT: TESTER_PROJECT,
    INSTITUTION: TESTER_INSTITUTION,
    "auth_key": AUTH_KEY,
    ASSOCIATED_META_WORKFLOW_HANDLER: META_WORKFLOW_HANDLER_UUID,
    ASSOCIATED_ITEM: ASSOCIATED_ITEM_UUID,
    FINAL_STATUS: PENDING,
    META_WORKFLOW_RUNS: [], #TODO: is this correct
}

META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE = {
    "auth_key": AUTH_KEY,
    "associated_item_dict": ASSOCIATED_ITEM_SIMPLE_DICT,
    "meta_workflow_handler_dict": META_WORKFLOW_HANDLER_SIMPLE_DICT,
    PROJECT: TESTER_PROJECT,
    INSTITUTION: TESTER_INSTITUTION,
    "associated_item_id": ASSOCIATED_ITEM_UUID,
    "meta_workflow_handler_id": META_WORKFLOW_HANDLER_UUID,
    "meta_workflow_run_handler_uuid": META_WORKFLOW_RUN_HANDLER_UUID,
    "meta_workflow_run_handler": META_WORKFLOW_RUN_HANDLER_SIMPLE_DICT,
}


@pytest.fixture
def meta_workflow_run_handler_from_item(
    assoc_item_dict_embed, mwf_handler_dict_embed, mwfr_handler_instance
):
    """
    Class for testing creation of MetaWorkflowRunHandlerFromItem,
    with portal requests & imported library calls mocked.
    """
    # import pdb; pdb.set_trace()
    with patch_retrieve_associated_item_dict(
        return_value=assoc_item_dict_embed
    ) as mock_embed_associated_item:
        with patch_retrieve_meta_workflow_handler_dict(
            return_value=mwf_handler_dict_embed
        ) as mock_embed_meta_workflow_handler:
            with patch_generate_uuid4(
                return_value=META_WORKFLOW_RUN_HANDLER_UUID
            ) as mock_generate_run_handler_uuid4:
                with patch_create_meta_workflow_run_handler_dict(
                    return_value=mwfr_handler_instance
                ) as mock_generate_run_handler_dict:
                    # import pdb; pdb.set_trace()
                    return MetaWorkflowRunHandlerFromItem(
                        ASSOCIATED_ITEM_UUID, META_WORKFLOW_HANDLER_UUID, AUTH_KEY
                    )



class TestMetaWorkflowRunHandlerFromItem:
    @pytest.mark.parametrize(
        "attribute, expected_value, assoc_item_dict_embed, mwf_handler_dict_embed, mwfr_handler_instance",
        [
            (
                "auth_key",
                AUTH_KEY,
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),
            (
                "associated_item_dict",
                ASSOCIATED_ITEM_SIMPLE_DICT,
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),  #
            (
                "associated_item_id",
                ASSOCIATED_ITEM_UUID,
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),  #
            (
                "project",
                TESTER_PROJECT,
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),  #
            (
                "institution",
                TESTER_INSTITUTION,
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),  #
            (
                "meta_workflow_handler_dict",
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),
            (
                "meta_workflow_handler_id",
                META_WORKFLOW_HANDLER_UUID,
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),
            (
                "meta_workflow_run_handler_uuid",
                META_WORKFLOW_RUN_HANDLER_UUID,
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),
            (
                "meta_workflow_run_handler",
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),
        ],
    )
    def test_instance_attributes(
        self, attribute, expected_value, meta_workflow_run_handler_from_item
    ):
        """Test that instance attributes are set correctly."""
        result = getattr(meta_workflow_run_handler_from_item, attribute)
        assert result == expected_value
    # TODO: add checks of inputting wrong identifiers for embed requests
    # do this in separate pytest. but put a couple here (integrated) for good measure


    @pytest.mark.parametrize(
        "assoc_item_dict_embed, mwf_handler_dict_embed, mwfr_handler_instance",
        [
            (
                ASSOCIATED_ITEM_SIMPLE_DICT,
                META_WORKFLOW_HANDLER_SIMPLE_DICT,
                META_WORKFLOW_RUN_HANDLER_SIMPLE_INSTANCE,
            ),
        ],
    )
    def test_create_meta_workflow_runs_array(
        self, meta_workflow_run_handler_from_item
    ):
        """
        Tests invocation of topological sort within MetaWorkflowHandler object,
        and the conversion of the sorted MetaWorkflow Steps in MetaWorkflow Handler
        to MetaWorkflow Run Steps in the Run Handler.
        """
        # result = getattr(meta_workflow_run_handler_from_item, "meta_workflow_run_handler")
        # print(result)
        # assert result == result
        with patch_create_meta_workflow_runs_array(

        ) as mock_create_meta_workflow_runs_array:
            with patch_make_embed_request


#     @pytest.mark.parametrize(
#         "meta_workflow_run,error,expected",
#         [
#             (META_WORKFLOW_RUN_NO_FILES_INPUT, True, None),
#             (META_WORKFLOW_RUN_NO_WORKFLOW_RUNS, False, META_WORKFLOW_RUN),
#         ],
#     )
#     def test_create_workflow_runs(
#         self,
#         meta_workflow_run,
#         error,
#         expected,
#         meta_workflow_run_from_item,
#     ):
#         """Test creation of workflow runs from given MetaWorkflowRun
#         properties.
#         """
#         if error:
#             with pytest.raises(MetaWorkflowRunCreationError):
#                 meta_workflow_run_from_item.create_workflow_runs(meta_workflow_run)
#         else:
#             meta_workflow_run_from_item.create_workflow_runs(meta_workflow_run)
#             assert meta_workflow_run == expected

#     @pytest.mark.parametrize(
#         "return_value,exception,expected",
#         [
#             ({"foo": "bar"}, True, None),
#             ({"foo": "bar"}, False, {"foo": "bar"}),
#         ],
#     )
#     def test_get_item_properties(
#         self, meta_workflow_run_from_item, return_value, exception, expected
#     ):
#         """Test item GET from portal."""
#         side_effect = None
#         if exception:
#             side_effect = Exception
#         with mock.patch(
#             "magma_ff.create_metawfr.ff_utils.get_metadata",
#             return_value=return_value,
#             side_effect=side_effect,
#         ) as mock_get_metadata:
#             result = meta_workflow_run_from_item.get_item_properties("foo")
#             assert result == expected
#             mock_get_metadata.assert_called_once_with(
#                 "foo", key=AUTH_KEY, add_on="frame=raw"
#             )

#     @pytest.mark.parametrize("exception", [True, False])
#     def test_post_meta_workflow_item(self, meta_workflow_run_from_item, exception):
#         """Test MWFR POST to portal."""
#         side_effect = None
#         if exception:
#             side_effect = Exception
#         with mock.patch(
#             "magma_ff.create_metawfr.ff_utils.post_metadata",
#             side_effect=side_effect,
#         ) as mock_post_metadata:
#             if exception:
#                 with pytest.raises(MetaWorkflowRunCreationError):
#                     meta_workflow_run_from_item.post_meta_workflow_run()
#             else:
#                 meta_workflow_run_from_item.post_meta_workflow_run()
#             mock_post_metadata.assert_called_once_with(
#                 {}, MetaWorkflowRunFromItem.META_WORKFLOW_RUN_ENDPOINT, key=AUTH_KEY
#             )
