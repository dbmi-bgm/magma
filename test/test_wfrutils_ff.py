from contextlib import contextmanager
from test.utils import patch_context
from typing import Iterator  # , List, Any, Optional
from requests.exceptions import HTTPError

import mock
import pytest

import magma_ff.wfrutils as wfrutils_module
from magma_ff.wfrutils import (
    # FFWfrUtils,
    FFMetaWfrUtils,
)

from magma.magma_constants import *
from magma_ff.utils import JsonObject

# TODO: add to constants file?
TEST_MWFR_ID_A = "test_uuid_a"
TEST_MWFR_ID_B = "test_uuid_b"
AUTH_KEY = {"server": "some_server"}
RANDOM_COST = 34.56

MWFR_A_PORTAL_OBJ = {UUID: TEST_MWFR_ID_A, FINAL_STATUS: PENDING, COST: RANDOM_COST}

MWFR_B_PORTAL_OBJ = {UUID: TEST_MWFR_ID_B, FINAL_STATUS: RUNNING}

CACHE_WITH_MWFR = {TEST_MWFR_ID_B: MWFR_B_PORTAL_OBJ}


@contextmanager
def patch_get_metadata(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch ff_utils.get_metadata call within FFMetaWfrUtils class."""
    with patch_context(wfrutils_module.ff_utils, "get_metadata", **kwargs) as mock_item:
        yield mock_item


@contextmanager
def patch_meta_workflow_runs_cache(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch _meta_workflow_runs_cache property within FFMetaWfrUtils class."""
    with patch_context(
        wfrutils_module.FFMetaWfrUtils,
        "_meta_workflow_runs_cache",
        new_callable=mock.PropertyMock,
        **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_retrieve_meta_workflow_run(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch _retrieve_meta_workflow_run method within FFMetaWfrUtils class."""
    with patch_context(
        wfrutils_module.FFMetaWfrUtils, "_retrieve_meta_workflow_run", **kwargs
    ) as mock_item:
        yield mock_item


class TestFFMetaWfrUtils:
    """Tests for methods/properties for FFMetaWfrUtils class."""

    def test_meta_workflow_runs_cache(self) -> None:
        """
        Tests updates to _meta_workflow_runs_cache property.
        """
        meta_workflow_runs_retriever = FFMetaWfrUtils(AUTH_KEY)
        assert meta_workflow_runs_retriever._meta_workflow_runs_cache == {}
        meta_workflow_runs_retriever._meta_workflow_runs_cache[
            TEST_MWFR_ID_B
        ] = MWFR_B_PORTAL_OBJ
        assert meta_workflow_runs_retriever._meta_workflow_runs_cache == CACHE_WITH_MWFR
        meta_workflow_runs_retriever._meta_workflow_runs_cache[
            TEST_MWFR_ID_A
        ] = MWFR_A_PORTAL_OBJ
        assert len(meta_workflow_runs_retriever._meta_workflow_runs_cache) == 2

    @pytest.mark.parametrize(
        "meta_workflow_run_identifier, meta_workflow_run, in_cache, get_request_exception, cache_calls",
        [
            (
                TEST_MWFR_ID_A,
                MWFR_A_PORTAL_OBJ,
                False,
                False,
                2,
            ),  # successful GET from portal
            (
                TEST_MWFR_ID_A,
                MWFR_A_PORTAL_OBJ,
                False,
                True,
                1,
            ),  # unsuccessful GET from portal
            (
                TEST_MWFR_ID_B,
                MWFR_B_PORTAL_OBJ,
                True,
                False,
                2,
            ),  # MWFR already in the cache
        ],
    )
    def test_retrieve_meta_workflow_run(
        self,
        meta_workflow_run_identifier: str,
        meta_workflow_run: JsonObject,
        in_cache: bool,
        get_request_exception: bool,
        cache_calls: int,
    ) -> None:
        """
        Tests retrieval of MetaWorkflow Runs from portal, and addition to cache.
        """
        with patch_get_metadata() as mock_get_metadata:
            with patch_meta_workflow_runs_cache() as mock_cache:
                meta_workflow_runs_retriever = FFMetaWfrUtils(AUTH_KEY)
                if in_cache:
                    mock_cache.return_value = CACHE_WITH_MWFR
                    result = meta_workflow_runs_retriever._retrieve_meta_workflow_run(
                        meta_workflow_run_identifier
                    )
                    assert result == meta_workflow_run
                    mock_get_metadata.assert_not_called()
                    assert mock_cache.call_count == cache_calls
                else:
                    if get_request_exception:
                        mock_get_metadata.side_effect = Exception("oops")
                        with pytest.raises(HTTPError):
                            meta_workflow_runs_retriever._retrieve_meta_workflow_run(
                                meta_workflow_run_identifier
                            )
                            assert mock_cache.call_count == cache_calls
                    else:
                        mock_get_metadata.return_value = meta_workflow_run
                        result = (
                            meta_workflow_runs_retriever._retrieve_meta_workflow_run(
                                meta_workflow_run_identifier
                            )
                        )
                        assert mock_cache.call_count == cache_calls
                        assert result == meta_workflow_run

    @pytest.mark.parametrize(
        "meta_workflow_run_identifier, meta_workflow_run, expected_status",
        [
            (TEST_MWFR_ID_A, MWFR_A_PORTAL_OBJ, PENDING),
            (TEST_MWFR_ID_B, MWFR_B_PORTAL_OBJ, RUNNING)
        ],
    )
    def test_get_meta_workflow_run_status(
        self,
        meta_workflow_run_identifier: str,
        meta_workflow_run: JsonObject,
        expected_status: str
    ) -> None:
        """
        Tests retrieval of a MetaWorkflow Run's status attribute from portal.
        """
        with patch_retrieve_meta_workflow_run(return_value=meta_workflow_run):
            meta_workflow_runs_retriever = FFMetaWfrUtils(AUTH_KEY)
            result = meta_workflow_runs_retriever.get_meta_workflow_run_status(meta_workflow_run_identifier)
            assert result == expected_status

    @pytest.mark.parametrize(
        "meta_workflow_run_identifier, meta_workflow_run, expected_cost",
        [
            (TEST_MWFR_ID_A, MWFR_A_PORTAL_OBJ, RANDOM_COST),
            (TEST_MWFR_ID_B, MWFR_B_PORTAL_OBJ, float(0))
        ],
    )
    def test_get_meta_workflow_run_cost(
        self,
        meta_workflow_run_identifier: str,
        meta_workflow_run: JsonObject,
        expected_cost: float
    ) -> None:
        """
        Tests retrieval of a MetaWorkflow Run's cost attribute from portal.
        """
        with patch_retrieve_meta_workflow_run(return_value=meta_workflow_run):
            meta_workflow_runs_retriever = FFMetaWfrUtils(AUTH_KEY)
            result = meta_workflow_runs_retriever.get_meta_workflow_run_cost(meta_workflow_run_identifier)
            assert result == expected_cost
            assert isinstance(result, float)
        