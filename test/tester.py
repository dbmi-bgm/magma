#!/usr/bin/env python3

### python3 -m test.tester

#################################################################
#   Libraries
#################################################################

import mock
from contextlib import contextmanager

from typing import Iterator

from test.utils import patch_context
# from magma.magma_constants import *
import magma_ff.run_metawflrun_handler as run_metaworkflow_run_handler_module
from magma_ff.run_metawflrun_handler import (
    ExecuteMetaWorkflowRunHandler,
    execute_metawflrun_handler,
)

# import magma_ff.create_metawfr as create_metaworkflow_run_module
from magma_ff.create_metawfr import (
    create_meta_workflow_run,
    MetaWorkflowRunCreationError,
)

from test.meta_workflow_run_handler_constants import *

from magma_ff.metawflrun_handler import MetaWorkflowRunHandler
from magma.metawflrun_handler import MetaWorkflowRunStep

META_WORKFLOW_RUN_HANDLER_UUID = "meta_workflow_run_handler_tester_uuid"
AUTH_KEY = {"server": "some_server"}

@contextmanager
def patch_get_metadata(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch ff_utils.get_metadata call within MetaWorkflowRunHAndlerFromItem class."""
    with patch_context(
        create_metaworkflow_run_module.ff_utils, "get_metadata", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_create_meta_workflow_run(**kwargs) -> Iterator[mock.MagicMock]:
    """Patch ff_utils.post_metadata call within MetaWorkflowRunHAndlerFromItem class."""
    with patch_context(
        run_metaworkflow_run_handler_module, "create_meta_workflow_run", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_create_and_update_meta_workflow_run_step(
    **kwargs,
) -> Iterator[mock.MagicMock]:
    """Patch ff_utils.post_metadata call within MetaWorkflowRunHAndlerFromItem class."""
    with patch_context(
        run_metaworkflow_run_handler_module,
        "_create_and_update_meta_workflow_run_step",
        **kwargs,
    ) as mock_item:
        yield mock_item

############################
if __name__ == "__main__":
    
    with patch_create_meta_workflow_run(return_value=TEST_MWFR_SIMPLE_GET_OUTPUT) as mock_create_mwfr:
            execution_generator = ExecuteMetaWorkflowRunHandler(
                HANDLER_PENDING, AUTH_KEY
            )
            generatorr = execution_generator.generator_of_created_meta_workflow_run_steps()
            # import pdb; pdb.set_trace()
            for idx, step in enumerate(generatorr):
                print(idx)
                print(step)
                print()