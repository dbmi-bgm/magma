"""Shared fixtures for the magma test suite."""

import pytest

import magma_smaht.utils as utils_module


@pytest.fixture(autouse=True)
def clear_item_cache():
    """Isolate the memoized portal fetch between tests.

    `magma_smaht.utils._get_item_es_cached` is a module level `lru_cache` keyed on
    (identifier, serialized key, frame), and every test uses the same mocked auth
    key. Without this, two tests that reuse an identifier with different item
    fixtures read each other's data and pass or fail depending on
    which ran first -- which `-k` selection, xdist sharding, or simply inserting a
    test above changes. Cleared on the way in and out so a test that populates the
    cache cannot leak either forwards or backwards.
    """
    utils_module._get_item_es_cached.cache_clear()
    yield
    utils_module._get_item_es_cached.cache_clear()
