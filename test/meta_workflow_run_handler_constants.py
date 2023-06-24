#!/usr/bin/env python3

#################################################################
#   Libraries
#################################################################
from copy import deepcopy

from magma.magma_constants import *

#################################################################
#   Vars
#################################################################

MWF_RUN_HANDLER_NAME = "test_mwf_run_handler"
MWF_RUN_PROJECT = "test_project"
MWF_RUN_INSTITUTION = "test_institution"
MWF_RUN_HANDLER_UUID = "test_mwf_run_handler_uuid"

TESTER_UUID = "test_item_uuid"

TEST_MWFR_SIMPLE_GET_OUTPUT = {
    "project": MWF_RUN_PROJECT,
    "institution": MWF_RUN_INSTITUTION,
    # "final_status": "completed",
    "meta_workflow": "/meta-workflows/GAPMWIC28HMB/",
    "@id": "/meta-workflow-runs/1734e9ac-af8c-4312-ac35-8b0018ef7411/",
    "@type": ["MetaWorkflowRun", "Item"],
    "uuid": TESTER_UUID
}


# basic meta_workflow_run dicts used in meta_workflow_runs array
# will have attributes added to them using mwf_run_with_added_attrs()
MWFR_A = {"name": "A", "meta_workflow": "link_to_mwf_A"}
MWFR_B = {"name": "B", "meta_workflow": "link_to_mwf_B"}
MWFR_C = {"name": "C", "meta_workflow": "link_to_mwf_C"}
MWFR_D = {"name": "D", "meta_workflow": "link_to_mwf_D"}

MWF_NAMES_LIST = ["B", "C", "A", "D"]

DEP_ON_A = ["A"]
DEP_ON_B = ["B"]
DEP_ON_C = ["C"]
DEP_ON_D = ["D"]


def mwf_run_with_added_attrs(
    meta_workflow_run_dict,
    dependencies=None,
    items_for_creation=None,
    status=None,
    meta_workflow_run_linkto=None,
    error=None,
):
    """
    Generates an updated meta_workflow_run_dict given a basic meta_workflow_run_dict and attributes to add.
    These attributes are limited to dependencies, items_for_creation, and status for these tests.

    :param meta_workflow_run_dict: Dictionary with basic attribute(s) of a MetaWorkflow Run
    :type meta_workflow_run_dict: dict
    :param dependencies: MetaWorkflow Runs, by name, that the given MetaWorkflow Run depends on
    :type dependencies: list
    :param items_for_creation: Item linkTo(s) needed to created the given MetaWorkflow Run
    :type items_for_creation: str or list[str]
    :param status: the status of the given MetaWorkflow Run
    :type status: str
    :param meta_workflow_run_linkto: the linkTo to a "created" MetaWorkflow Run on CGAP portal
    :type meta_workflow_run_linkto: str
    :param error: error traceback at "creation" of a MetaWorkflow Run
    :type error: str
    :return: updated meta_workflow_run_dict
    """
    dict_copy = deepcopy(meta_workflow_run_dict)
    if dependencies is not None:
        dict_copy[DEPENDENCIES] = dependencies
    if items_for_creation is not None:
        dict_copy[ITEMS_FOR_CREATION] = items_for_creation
    if status is not None:
        dict_copy[STATUS] = status
    if meta_workflow_run_linkto is not None:
        dict_copy[META_WORKFLOW_RUN] = meta_workflow_run_linkto
    if error is not None:
        dict_copy[ERROR] = error
    return dict_copy


def mwfr_handler_dict_generator(meta_workflow_runs_array):
    """
    Given a meta_workflow_runs array, returns an input dict for
    creation of a MetaWorkflow Run Handler object.

    :param meta_workflow_runs_array: list of meta_workflow_run dicts
    :type meta_workflow_runs_array: list[dict]
    :return: dictionary to be used as input to instantiate a MetaWorkflow Run Handler object
    """
    return {
        NAME: MWF_RUN_HANDLER_NAME,
        PROJECT: MWF_RUN_PROJECT,
        INSTITUTION: MWF_RUN_INSTITUTION,
        UUID: MWF_RUN_HANDLER_UUID,
        ASSOCIATED_META_WORKFLOW_HANDLER: TESTER_UUID,
        META_WORKFLOW_RUNS: meta_workflow_runs_array,
    }


# handler without uuid -- fails validation of basic attributes
full_handler_dict_0 = mwfr_handler_dict_generator([])
full_handler_dict_0.pop(UUID)
HANDLER_WITHOUT_UUID_DICT = full_handler_dict_0


# handler without associated MetaWorkflow Handler uuid -- fails validation of basic attributes
full_handler_dict_1 = mwfr_handler_dict_generator([])
full_handler_dict_1.pop(ASSOCIATED_META_WORKFLOW_HANDLER)
HANDLER_WITHOUT_ASSOC_MWFH_DICT = full_handler_dict_1

# handler without meta_workflow_runs array -- fails validation of basic attributes
HANDLER_WITHOUT_META_WORKFLOW_RUNS_ARRAY = mwfr_handler_dict_generator(None)

# Constructing a Run Handler with the below step dependencies
# B -----> D
# |     ⋀  ⋀
# |   /    |
# ⋁ /      |
# A <----- C

# Pending MetaWorkflow Run dicts
MWFR_A_PENDING = mwf_run_with_added_attrs(
    MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, PENDING
)
MWFR_B_PENDING = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, PENDING)
MWFR_C_PENDING = mwf_run_with_added_attrs(MWFR_C, [], TESTER_UUID, PENDING)
MWFR_D_PENDING = mwf_run_with_added_attrs(
    MWFR_D, DEP_ON_A + DEP_ON_B + DEP_ON_C, TESTER_UUID, PENDING
)

# Running MetaWorkflow Run dicts
MWFR_A_RUNNING = mwf_run_with_added_attrs(
    MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, RUNNING, TESTER_UUID
)
MWFR_B_RUNNING = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, RUNNING, TESTER_UUID)
MWFR_C_RUNNING = mwf_run_with_added_attrs(MWFR_C, [], TESTER_UUID, RUNNING, TESTER_UUID)
MWFR_D_RUNNING = mwf_run_with_added_attrs(
    MWFR_D, DEP_ON_A + DEP_ON_B + DEP_ON_C, TESTER_UUID, RUNNING, TESTER_UUID
)

# Failed/stopped MetaWorkflowRun dicts
MWFR_A_FAILED = mwf_run_with_added_attrs(
    MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, FAILED, TESTER_UUID
)
MWFR_A_STOPPED = mwf_run_with_added_attrs(
    MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, STOPPED, TESTER_UUID
)

# Completed MetaWorkflow Run dicts
MWFR_A_COMPLETED = mwf_run_with_added_attrs(
    MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, COMPLETED, TESTER_UUID
)
MWFR_B_COMPLETED = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, COMPLETED, TESTER_UUID)
MWFR_C_COMPLETED = mwf_run_with_added_attrs(MWFR_C, [], TESTER_UUID, COMPLETED, TESTER_UUID)
MWFR_D_COMPLETED = mwf_run_with_added_attrs(
    MWFR_D, DEP_ON_A + DEP_ON_B + DEP_ON_C, TESTER_UUID, COMPLETED, TESTER_UUID
)


# Magma FF - specific attributes handled here (for updating meta_workflow_runs array method)
MWFR_B_COMPLETED_W_LINKTO = mwf_run_with_added_attrs(
    MWFR_B, [], TESTER_UUID, COMPLETED, "a_link_to"
)
MWFR_A_FAILED_W_ERROR = mwf_run_with_added_attrs(
    MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, FAILED, None, "error_message"
)
MWFR_A_STOPPED_W_LINKTO_AND_ERROR = mwf_run_with_added_attrs(
    MWFR_A,
    DEP_ON_B + DEP_ON_C,
    TESTER_UUID,
    STOPPED,
    "another_link_to",
    "and_another_error_message",
)

# Note: these MetaWorkflowRuns above will be mixed and matched for testing purposes
# See meta_workflow_runs arrays and Run Handler input dicts below

# All steps pending
PENDING_ARRAY = [MWFR_B_PENDING, MWFR_C_PENDING, MWFR_A_PENDING, MWFR_D_PENDING]
HANDLER_PENDING = mwfr_handler_dict_generator(PENDING_ARRAY)
HANDLER_PENDING_COPY = deepcopy(HANDLER_PENDING) #TODO: fix this hoe

# Handlers currently running
FIRST_STEP_RUNNING_ARRAY = [MWFR_B_RUNNING, MWFR_C_PENDING, MWFR_A_PENDING, MWFR_D_PENDING]
FIRST_STEP_COMPLETED_ARRAY = [MWFR_B_COMPLETED, MWFR_C_PENDING, MWFR_A_PENDING, MWFR_D_PENDING]
RUNNING_MWFR_ARRAY = [MWFR_B_RUNNING, MWFR_C_RUNNING, MWFR_A_PENDING, MWFR_D_PENDING]
RUNNING_MWFR_ARRAY_2 = [
    MWFR_B_COMPLETED_W_LINKTO,
    MWFR_C_RUNNING,
    MWFR_A_PENDING,
    MWFR_D_PENDING,
]
# this wouldn't happen with THIS dag in particular,
# but could in other cases (made for the sake of the final_status test for the handler TODO:)
# RUNNING_MWFR_ARRAY_3 = [MWFR_B_COMPLETED, MWFR_C_PENDING, MWFR_A_RUNNING, MWFR_D_PENDING]
HANDLER_STEPS_RUNNING = mwfr_handler_dict_generator(RUNNING_MWFR_ARRAY)
HANDLER_STEPS_RUNNING_2 = mwfr_handler_dict_generator(RUNNING_MWFR_ARRAY_2)
# HANDLER_STEPS_RUNNING_3 = mwfr_handler_dict_generator(RUNNING_MWFR_ARRAY_3)

# Handlers that have failed
HALFWAY_DONE_N_FAIL_ARRAY = [
    MWFR_B_COMPLETED,
    MWFR_C_COMPLETED,
    MWFR_A_FAILED,
    MWFR_D_PENDING,
]
HALFWAY_DONE_N_FAIL_ARRAY_2 = [
    MWFR_B_COMPLETED,
    MWFR_C_COMPLETED,
    MWFR_A_FAILED_W_ERROR,
    MWFR_D_RUNNING,
]
HANDLER_FAILED = mwfr_handler_dict_generator(HALFWAY_DONE_N_FAIL_ARRAY)
HANDLER_FAILED_2 = mwfr_handler_dict_generator(HALFWAY_DONE_N_FAIL_ARRAY_2)

# Handler that has been stopped
HALFWAY_DONE_N_STOPPED_ARRAY = [
    MWFR_B_COMPLETED,
    MWFR_C_COMPLETED,
    MWFR_A_STOPPED,
    MWFR_D_PENDING,
]
HALFWAY_DONE_N_STOPPED_ARRAY_2 = [
    MWFR_B_COMPLETED,
    MWFR_C_COMPLETED,
    MWFR_A_STOPPED_W_LINKTO_AND_ERROR,
    MWFR_D_PENDING,
]
HANDLER_STOPPED = mwfr_handler_dict_generator(HALFWAY_DONE_N_STOPPED_ARRAY)

# Handler that is completed
COMPLETED_ARRAY = [
    MWFR_B_COMPLETED,
    MWFR_C_COMPLETED,
    MWFR_A_COMPLETED,
    MWFR_D_COMPLETED,
]
HANDLER_COMPLETED = mwfr_handler_dict_generator(COMPLETED_ARRAY)
