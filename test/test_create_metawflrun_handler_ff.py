import mock
import pytest
from contextlib import contextmanager
from test.utils import patch_context
from typing import Iterator  # thx Doug mwehehe

from magma_ff.utils import JsonObject
from magma.magma_constants import *
import magma_ff.create_metawflrun_handler as create_metaworkflow_run_handler_module
from magma_ff.create_metawflrun_handler import (
    MetaWorkflowRunHandlerFromItem,
    MetaWorkflowRunHandlerCreationError,
    create_meta_workflow_run_handler
)

from test.meta_workflow_handler_constants import *

from magma_ff.metawfl_handler import MetaWorkflowHandler
from magma.metawfl_handler import MetaWorkflowStep


@contextmanager
def patch_post_metadata(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch ff_utils.post_metadata call within MetaWorkflowRunHAndlerFromItem class."""
    with patch_context(
        create_metaworkflow_run_handler_module.ff_utils, "post_metadata", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_make_embed_request(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch make_embed_request function defined in magma_ff/utils.py,
    which is called within MetaWorkflowRunHandlerFromItem class."""
    with patch_context(
        create_metaworkflow_run_handler_module,
        "make_embed_request",
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
def patch_embed_items_for_creation(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch function that uses embed requests to convert property traces to IDs."""
    with patch_context(
        create_metaworkflow_run_handler_module.MetaWorkflowRunHandlerFromItem,
        "_embed_items_for_creation",
        **kwargs
    ) as mock_item:
        yield mock_item

@contextmanager
def patch_retrieved_meta_workflow_handler(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch cached property of embedded meta_workflow_handler"""
    with patch_context(
        create_metaworkflow_run_handler_module.MetaWorkflowRunHandlerFromItem,
        "retrieved_meta_workflow_handler",
        new_callable=mock.PropertyMock,
        **kwargs
    ) as mock_item:
        yield mock_item

@contextmanager
def patch_retrieved_associated_item(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch cached property of embedded meta_workflow_handler"""
    with patch_context(
        create_metaworkflow_run_handler_module.MetaWorkflowRunHandlerFromItem,
        "retrieved_associated_item",
        new_callable=mock.PropertyMock,
        **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_generate_uuid4(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch generator of uuids,
    which is called within MetaWorkflowRunHandlerFromItem class."""
    with patch_context(
        create_metaworkflow_run_handler_module.uuid, "uuid4", **kwargs
    ) as mock_item:
        yield mock_item


TODAY = "2023-05-12"
TESTER_PROJECT = "project_tester"
TESTER_INSTITUTION = "institution_tester"
TESTER_TITLE = "title_tester"
TESTER_UUID = "uuid"
TESTER_PROP_TRACE = "property.trace"

ASSOCIATED_ITEM_UUID = "associated_item_tester_uuid"
ASSOCIATED_ITEM_SIMPLE_DICT = {
    UUID: ASSOCIATED_ITEM_UUID,
    PROJECT: TESTER_PROJECT,
    INSTITUTION: TESTER_INSTITUTION,
    # META_WORKFLOW_RUNS: [] # in the case that we wanna add dup flag back in future development
    # TODO: and patching this array? should be handled in run mwfr handler
}

META_WORKFLOW_HANDLER_UUID = "meta_workflow_handler_tester_uuid"

MWF_STEP_NO_EMBEDS = {
    META_WORKFLOW: "foo",
    NAME: "bar",
    ITEMS_FOR_CREATION_UUID: TESTER_UUID
}
MWF_STEP_NO_EMBEDS_2 = {
    META_WORKFLOW: "foo",
    NAME: "bar",
    ITEMS_FOR_CREATION_UUID: [TESTER_UUID]
}
MWF_STEP_EMBED_SIMPLE = {
    META_WORKFLOW: "foo",
    NAME: "bar",
    ITEMS_FOR_CREATION_PROP_TRACE: TESTER_PROP_TRACE
}
MWF_STEP_EMBED_SEVERAL = {
    META_WORKFLOW: "foo",
    NAME: "bar",
    ITEMS_FOR_CREATION_PROP_TRACE: [TESTER_PROP_TRACE, TESTER_PROP_TRACE] 
}

# just redefining the uuids from the mwf handler dicts for consistency
# DAG_0
# A     B -----> C
HANDLER_DAG_0[UUID] = META_WORKFLOW_HANDLER_UUID
# DAG_1
# B -----> D
# |     ⋀  ⋀
# |   /    |
# ⋁ /      |
# A <----- C 
HANDLER_DAG_1[UUID] = META_WORKFLOW_HANDLER_UUID

# with title
HANDLER_DAG_0_W_TITLE = deepcopy(HANDLER_DAG_0)
HANDLER_DAG_0_W_TITLE[TITLE] = "DAG 0"


META_WORKFLOW_RUN_HANDLER_UUID = "meta_workflow_run_handler_tester_uuid"
AUTH_KEY = {"key": "foo"}


@pytest.fixture
def meta_workflow_run_handler_from_item_fixture():
    """Fixture of MetaWorkflowRunHandlerFromItem instance"""
    return MetaWorkflowRunHandlerFromItem(ASSOCIATED_ITEM_UUID, META_WORKFLOW_HANDLER_UUID, AUTH_KEY)


class TestMetaWorkflowRunHandlerFromItem:
    """Tests for methods/properties for MetaWorkflowRunHandlerFromItem class."""

    @pytest.mark.parametrize(
        "attribute, expected_value, assoc_item_id, mwf_handler_id, auth_key",
        [
            (
                "auth_key",
                AUTH_KEY,
                ASSOCIATED_ITEM_UUID,
                META_WORKFLOW_HANDLER_UUID,
                AUTH_KEY,
            ),
            (
                "associated_item_identifier",
                ASSOCIATED_ITEM_UUID,
                ASSOCIATED_ITEM_UUID,
                META_WORKFLOW_HANDLER_UUID,
                AUTH_KEY,
            ),
            (
                "meta_workflow_handler_identifier",
                META_WORKFLOW_HANDLER_UUID,
                ASSOCIATED_ITEM_UUID,
                META_WORKFLOW_HANDLER_UUID,
                AUTH_KEY,
            ),
            (
                "auth_key",
                None,
                ASSOCIATED_ITEM_UUID,
                META_WORKFLOW_HANDLER_UUID,
                None,
            ),
            (
                "associated_item_identifier",
                None,
                None,
                META_WORKFLOW_HANDLER_UUID,
                AUTH_KEY,
            ),
            (
                "meta_workflow_handler_identifier",
                None,
                ASSOCIATED_ITEM_UUID,
                None,
                AUTH_KEY,
            ),
        ],
    )
    def test_init(
        self, attribute, expected_value, assoc_item_id, mwf_handler_id, auth_key
    ):
        """Test that instance attributes are set correctly."""
        try:
            meta_workflow_run_handler_from_item = MetaWorkflowRunHandlerFromItem(
                assoc_item_id, mwf_handler_id, auth_key
            )
            result = getattr(meta_workflow_run_handler_from_item, attribute)
            assert result == expected_value
        except MetaWorkflowRunHandlerCreationError as creation_err:
            assert attribute in str(creation_err)


    @pytest.mark.parametrize(
        "meta_workflow_step, exception_expected, return_value, num_embed_calls",
        [
            (
                MetaWorkflowStep(MWF_STEP_EMBED_SIMPLE),
                True,
                None,
                1
            ),
            (
                MetaWorkflowStep(MWF_STEP_EMBED_SEVERAL),
                True,
                None,
                1
            ),
            (
                MetaWorkflowStep(MWF_STEP_NO_EMBEDS),
                False,
                TESTER_UUID,
                0
            ),
            (
                MetaWorkflowStep(MWF_STEP_NO_EMBEDS_2),
                False,
                [TESTER_UUID],
                0
            ),
            (
                MetaWorkflowStep(MWF_STEP_EMBED_SIMPLE),
                False,
                TESTER_UUID,
                1
            ),
            (
                MetaWorkflowStep(MWF_STEP_EMBED_SEVERAL),
                False,
                [TESTER_UUID, TESTER_UUID],
                2
            )
        ],
    )
    def test_embed_items_for_creation(
        self, meta_workflow_step, exception_expected, return_value, num_embed_calls, meta_workflow_run_handler_from_item_fixture
    ):
        """
        Tests the conversion of the items_for_creation_(uuid/prop_trace) in MetaWorkflow Steps
        to items_for_creation in MetaWorkflow Run Steps in the Run Handler.
        """
        with patch_make_embed_request() as mock_embed_request:
            if exception_expected:
                mock_embed_request.return_value = None
                with pytest.raises(MetaWorkflowRunHandlerCreationError):
                    result = meta_workflow_run_handler_from_item_fixture._embed_items_for_creation(meta_workflow_step)
                    assert mock_embed_request.call_count == num_embed_calls
            else:
                mock_embed_request.return_value = TESTER_UUID
                result = meta_workflow_run_handler_from_item_fixture._embed_items_for_creation(meta_workflow_step)
                assert result == return_value
                assert mock_embed_request.call_count == num_embed_calls

    @pytest.mark.parametrize(
        "meta_workflow_handler, num_step_calls",
        [
            (HANDLER_DAG_0, 3),
            (HANDLER_DAG_1, 4)
        ],
    )
    def test_create_meta_workflow_runs_array(
        self, meta_workflow_handler, num_step_calls, meta_workflow_run_handler_from_item_fixture
    ):
        """
        Tests the conversion of the ordered MetaWorkflow Steps
        to MetaWorkflow Run Steps in the Run Handler.
        Implicitly testing the property ordered_meta_workflows,
        and cached property retrieved_meta_workflow_handler.
        """
        with patch_retrieved_meta_workflow_handler(return_value=meta_workflow_handler):
            with patch_embed_items_for_creation(return_value=TESTER_UUID) as mock_embed_request:
                handler = meta_workflow_run_handler_from_item_fixture
                result = handler._create_meta_workflow_runs_array()

                orig_ordered_mwf_names = getattr(handler.meta_workflow_handler_instance, ORDERED_META_WORKFLOWS)
                orig_mwf_steps = getattr(handler.meta_workflow_handler_instance, META_WORKFLOWS)

                for idx, name in enumerate(orig_ordered_mwf_names):
                    assert result[idx][NAME] == name
                    assert result[idx][DEPENDENCIES] == getattr(orig_mwf_steps[name], DEPENDENCIES)
                    assert result[idx][ITEMS_FOR_CREATION] == TESTER_UUID

                assert mock_embed_request.call_count == num_step_calls


    @pytest.mark.parametrize(
        "meta_workflow_handler",
        [
            (HANDLER_DAG_0),
            (HANDLER_DAG_1)
        ],
    )
    def test_create_meta_workflow_run_handler_no_title(
        self, meta_workflow_handler, meta_workflow_run_handler_from_item_fixture
    ):
        """
        Tests creation of run handler function,
        using regular handler as template.
        """
        with patch_retrieved_meta_workflow_handler(return_value=meta_workflow_handler):
            with patch_retrieved_associated_item(return_value=ASSOCIATED_ITEM_SIMPLE_DICT) as mocked_assoc_item:
                with patch_generate_uuid4(return_value=META_WORKFLOW_RUN_HANDLER_UUID) as mocked_uuid:
                    with mock.patch('datetime.date') as mocked_current_date:
                        with patch_create_meta_workflow_runs_array() as mocked_mwfr_arr_creation:
                            completed_handler = meta_workflow_run_handler_from_item_fixture.create_meta_workflow_run_handler()
                            mocked_uuid.assert_called_once()
                            mocked_current_date.assert_not_called()
                            assert mocked_assoc_item.call_count == 2
                            mocked_mwfr_arr_creation.assert_called_once()

                            assert completed_handler[PROJECT] == TESTER_PROJECT
                            assert completed_handler[INSTITUTION] == TESTER_INSTITUTION
                            assert completed_handler[UUID] == META_WORKFLOW_RUN_HANDLER_UUID
                            assert completed_handler[ASSOCIATED_META_WORKFLOW_HANDLER] == META_WORKFLOW_HANDLER_UUID
                            assert completed_handler[ASSOCIATED_ITEM] == ASSOCIATED_ITEM_UUID
                            assert completed_handler[FINAL_STATUS] == PENDING
                            assert completed_handler.get(TITLE) is None
                            assert getattr(meta_workflow_run_handler_from_item_fixture, "meta_workflow_run_handler", None) is not None

    @mock.patch('magma_ff.create_metawflrun_handler.date')
    def test_create_meta_workflow_run_handler_with_title(
        self, mocked_date, meta_workflow_run_handler_from_item_fixture
    ):
        """
        Tests creation of run handler function,
        using regular handler as template, including title formatting.
        """
        with patch_retrieved_meta_workflow_handler(return_value=HANDLER_DAG_0_W_TITLE):
            with patch_retrieved_associated_item(return_value=ASSOCIATED_ITEM_SIMPLE_DICT):
                with patch_generate_uuid4(return_value=META_WORKFLOW_RUN_HANDLER_UUID):
                    with patch_create_meta_workflow_runs_array():
                        mocked_date.today.return_value.isoformat.return_value = TODAY
                        completed_handler = meta_workflow_run_handler_from_item_fixture.create_meta_workflow_run_handler()
                        mocked_date.today.assert_called_once()
                        assert completed_handler[TITLE] == f"MetaWorkflowRun Handler {HANDLER_DAG_0_W_TITLE[TITLE]} created {TODAY}"
                            

    @pytest.mark.parametrize("exception", [True, False])
    def test_post_meta_workflow_run_handler(self, exception, meta_workflow_run_handler_from_item_fixture):
        """Test MetaWorkflow Run Handler POST to CGAP portal."""

        with patch_retrieved_meta_workflow_handler(return_value=HANDLER_DAG_0):
            with patch_retrieved_associated_item(return_value=ASSOCIATED_ITEM_SIMPLE_DICT):
                with patch_generate_uuid4(return_value=META_WORKFLOW_RUN_HANDLER_UUID):
                    meta_workflow_run_handler_from_item_fixture.create_meta_workflow_run_handler()
                    if exception:
                        with patch_post_metadata(side_effect=Exception) as mock_post_with_error:
                            with pytest.raises(MetaWorkflowRunHandlerCreationError) as creation_err:
                                meta_workflow_run_handler_from_item_fixture.post_meta_workflow_run_handler()
                                assert "MetaWorkflowRunHandler not POSTed" in creation_err
                                mock_post_with_error.assert_called_once()
                    else:
                        with patch_post_metadata() as mock_post:
                            meta_workflow_run_handler_from_item_fixture.post_meta_workflow_run_handler()
                            mock_post.assert_called_once_with(
                                getattr(meta_workflow_run_handler_from_item_fixture, "meta_workflow_run_handler"),
                                MetaWorkflowRunHandlerFromItem.META_WORKFLOW_RUN_HANDLER_ENDPOINT,
                                key=AUTH_KEY
                            )

#####################################################
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

@contextmanager
def patch_create_meta_workflow_run_handler(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch function that uses embed requests to convert property traces to IDs."""
    with patch_context(
        create_metaworkflow_run_handler_module.MetaWorkflowRunHandlerFromItem,
        "create_meta_workflow_run_handler",
        **kwargs
    ) as mock_item:
        yield mock_item

@contextmanager
def patch_post_meta_workflow_run_handler(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch function that uses embed requests to convert property traces to IDs."""
    with patch_context(
        create_metaworkflow_run_handler_module.MetaWorkflowRunHandlerFromItem,
        "post_meta_workflow_run_handler",
        **kwargs
    ) as mock_item:
        yield mock_item


@pytest.mark.parametrize("post", [True, False])
def test_create_meta_workflow_run_handler(
    post: bool
) -> None:
    """Test of wrapper function to Run Handler creation class."""
    with patch_create_meta_workflow_run_handler(return_value=META_WORKFLOW_RUN_HANDLER_SIMPLE_DICT) as mock_handler_creation:
        with patch_post_meta_workflow_run_handler() as mock_post_handler:
            result = create_meta_workflow_run_handler(
                ASSOCIATED_ITEM_UUID,
                META_WORKFLOW_HANDLER_UUID,
                AUTH_KEY,
                post
            )
            mock_handler_creation.assert_called_once()
            if post:
                mock_post_handler.assert_called_once()
            else:
                mock_post_handler.assert_not_called()

            assert result == META_WORKFLOW_RUN_HANDLER_SIMPLE_DICT