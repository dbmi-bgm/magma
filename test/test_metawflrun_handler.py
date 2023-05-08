#!/usr/bin/env python3

#################################################################
#   Libraries
#################################################################
import pytest
from copy import deepcopy

from magma.metawflrun_handler import MetaWorkflowRunStep, MetaWorkflowRunHandler
from magma.magma_constants import *

#################################################################
#   Vars
#################################################################

MWF_RUN_HANDLER_NAME = "test_mwf_run_handler"
MWF_RUN_PROJECT = "test_project"
MWF_RUN_INSTITUTION = "test_institution"
MWF_RUN_HANDLER_UUID = "test_mwf_run_handler_uuid"

TESTER_UUID = "test_item_uuid"


# basic meta_workflow_run dicts used in meta_workflow_runs array
# will have attributes added to them using mwf_run_with_added_attrs()
MWFR_A = {"name": "A"}
MWFR_B = {"name": "B"}
MWFR_C = {"name": "C"}
MWFR_D = {"name": "D"}

MWF_NAMES_LIST = ["B", "C", "A", "D"]

DEP_ON_A = ["A"]
DEP_ON_B = ["B"]
DEP_ON_C = ["C"]
DEP_ON_D = ["D"]

def mwf_run_with_added_attrs(meta_workflow_run_dict, dependencies=None, items_for_creation=None, \
    status=None, meta_workflow_run_linkto=None, error=None):
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
        META_WORKFLOW_RUNS: meta_workflow_runs_array
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
MWFR_A_PENDING = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, PENDING)
MWFR_B_PENDING = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, PENDING)
MWFR_C_PENDING = mwf_run_with_added_attrs(MWFR_C, [], TESTER_UUID, PENDING)
MWFR_D_PENDING = mwf_run_with_added_attrs(MWFR_D, DEP_ON_A + DEP_ON_B + DEP_ON_C, TESTER_UUID, PENDING)

# Running MetaWorkflow Run dicts
MWFR_A_RUNNING = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, RUNNING)
MWFR_B_RUNNING = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, RUNNING)
MWFR_C_RUNNING = mwf_run_with_added_attrs(MWFR_C, [], TESTER_UUID, RUNNING)
MWFR_D_RUNNING = mwf_run_with_added_attrs(MWFR_D, DEP_ON_A + DEP_ON_B + DEP_ON_C, TESTER_UUID, RUNNING)

# Failed/stopped MetaWorkflowRun dicts
MWFR_A_FAILED = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, FAILED)
MWFR_A_STOPPED = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, STOPPED)

# Completed MetaWorkflow Run dicts
MWFR_A_COMPLETED = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C,  TESTER_UUID, COMPLETED)
MWFR_B_COMPLETED = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, COMPLETED)
MWFR_C_COMPLETED = mwf_run_with_added_attrs(MWFR_C, [], TESTER_UUID, COMPLETED)
MWFR_D_COMPLETED = mwf_run_with_added_attrs(MWFR_D, DEP_ON_A + DEP_ON_B + DEP_ON_C, TESTER_UUID, COMPLETED)


# Magma FF - specific attributes handled here (for updating meta_workflow_runs array method)
MWFR_B_COMPLETED_W_LINKTO = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, COMPLETED, "a_link_to")
MWFR_A_FAILED_W_ERROR = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, FAILED, None, "error_message")
MWFR_A_STOPPED_W_LINKTO_AND_ERROR = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, STOPPED,\
                                    "another_link_to", "and_another_error_message")

# Note: these MetaWorkflowRuns above will be mixed and matched for testing purposes
# See meta_workflow_runs arrays and Run Handle input dicts below

# All steps pending
PENDING_ARRAY = [MWFR_B_PENDING, MWFR_C_PENDING, MWFR_A_PENDING, MWFR_D_PENDING]
HANDLER_PENDING = mwfr_handler_dict_generator(PENDING_ARRAY)

# Handlers currently running
RUNNING_MWFR_ARRAY = [MWFR_B_RUNNING, MWFR_C_RUNNING, MWFR_A_PENDING, MWFR_D_PENDING]
RUNNING_MWFR_ARRAY_2 = [MWFR_B_COMPLETED_W_LINKTO, MWFR_C_RUNNING, MWFR_A_PENDING, MWFR_D_PENDING]
# this wouldn't happen with THIS dag in particular, 
# but could in other cases (made for the sake of the final_status test for the handler TODO:)
# RUNNING_MWFR_ARRAY_3 = [MWFR_B_COMPLETED, MWFR_C_PENDING, MWFR_A_RUNNING, MWFR_D_PENDING]
HANDLER_STEPS_RUNNING = mwfr_handler_dict_generator(RUNNING_MWFR_ARRAY)
HANDLER_STEPS_RUNNING_2 = mwfr_handler_dict_generator(RUNNING_MWFR_ARRAY_2)
# HANDLER_STEPS_RUNNING_3 = mwfr_handler_dict_generator(RUNNING_MWFR_ARRAY_3)

# Handlers that have failed
HALFWAY_DONE_N_FAIL_ARRAY = [MWFR_B_COMPLETED, MWFR_C_COMPLETED, MWFR_A_FAILED, MWFR_D_PENDING]
HALFWAY_DONE_N_FAIL_ARRAY_2 = [MWFR_B_COMPLETED, MWFR_C_COMPLETED, MWFR_A_FAILED_W_ERROR, MWFR_D_RUNNING]
HANDLER_FAILED = mwfr_handler_dict_generator(HALFWAY_DONE_N_FAIL_ARRAY)
HANDLER_FAILED_2 = mwfr_handler_dict_generator(HALFWAY_DONE_N_FAIL_ARRAY_2)

# Handler that has been stopped
HALFWAY_DONE_N_STOPPED_ARRAY = [MWFR_B_COMPLETED, MWFR_C_COMPLETED, MWFR_A_STOPPED, MWFR_D_PENDING]
HALFWAY_DONE_N_STOPPED_ARRAY_2 = [MWFR_B_COMPLETED, MWFR_C_COMPLETED, MWFR_A_STOPPED_W_LINKTO_AND_ERROR, MWFR_D_PENDING]
HANDLER_STOPPED = mwfr_handler_dict_generator(HALFWAY_DONE_N_STOPPED_ARRAY)

# Handler that is completed
COMPLETED_ARRAY = [MWFR_B_COMPLETED, MWFR_C_COMPLETED, MWFR_A_COMPLETED, MWFR_D_COMPLETED]
HANDLER_COMPLETED = mwfr_handler_dict_generator(COMPLETED_ARRAY)

#################################################################
#   Tests
#################################################################
class TestMetaWorkflowRunStep:
    @pytest.mark.parametrize(
        "mwf_run_step_dict, dependencies, items_for_creation, num_attributes",
        [
            (MWFR_A, [], [TESTER_UUID], 4), # successfully creates
            (MWFR_A, [], None, 3) # TODO: for now, doesn't fail if no items for creation
        ]
    )
    def test_attribute_validation(self, mwf_run_step_dict, dependencies, items_for_creation, num_attributes):
        """
        Tests creation of appropriate MetaWorkflowRunStep objects,
        no errors raised.
        """
        completed_dict = mwf_run_with_added_attrs(mwf_run_step_dict, dependencies, items_for_creation)
        meta_workflow_run_step_object = MetaWorkflowRunStep(completed_dict)
        assert num_attributes == len(meta_workflow_run_step_object.__dict__)
        assert meta_workflow_run_step_object.status == PENDING

        required_attributes = [NAME, DEPENDENCIES]#, "items_for_creation"]
        for attr in required_attributes:
            assert hasattr(meta_workflow_run_step_object, attr) == True

    @pytest.mark.parametrize(
        "mwf_run_step_dict, dependencies, items_for_creation",
        [
            ({}, [], [TESTER_UUID]), # fails because no name
            (MWFR_A, None, [TESTER_UUID]), # fails because no dependencies
        ]
    )
    def test_attribute_validation_attribute_errors(self, mwf_run_step_dict, dependencies, items_for_creation):
        """
        Tests creation of appropriate MetaWorkflowRunStep objects,
        Attribute Errors raised (missing required attributes).
        """
        with pytest.raises(AttributeError) as attr_err_info:
            completed_dict = mwf_run_with_added_attrs(mwf_run_step_dict, dependencies, items_for_creation)
            MetaWorkflowRunStep(completed_dict)


class TestMetaWorkflowRunHandler:
    def test_attribute_validation(self):
        """
        Tests creation of appropriate MetaWorkflowRun Handler objects,
        no errors raised.
        # TODO: for now, doesn't fail if no associated_item -- could make this check in ff
        """
        meta_workflow_run_handler = MetaWorkflowRunHandler(HANDLER_PENDING)
        assert getattr(meta_workflow_run_handler, FINAL_STATUS) == PENDING
        required_attributes = [UUID, ASSOCIATED_META_WORKFLOW_HANDLER]
        for attr in required_attributes:
            assert hasattr(meta_workflow_run_handler, attr) == True

    @pytest.mark.parametrize(
        "input_dict",
        [
            (HANDLER_WITHOUT_UUID_DICT), # fails because no uuid
            (HANDLER_WITHOUT_ASSOC_MWFH_DICT), # fails because no associated MetaWorkflow Handler
            (HANDLER_WITHOUT_META_WORKFLOW_RUNS_ARRAY) # fails because no meta_workflow_runs array
        ]
    )
    def test_attribute_validation_attribute_errors(self, input_dict):
        """
        Tests creation of appropriate MetaWorkflowRunHandler objects,
        Attribute Errors raised (missing required attributes).
        """
        with pytest.raises(AttributeError) as attr_err_info:
            MetaWorkflowRunHandler(input_dict)
        assert "Object validation error" in str(attr_err_info.value)

    def test_set_meta_workflow_runs_dict(self):
        """
        Tests creation of MetaWorkflowRunStep objects for all MetaWorkflow Runs
        in the meta_workflow_runs array, and creates dict out of them for quick access and update.
        """
        meta_workflow_run_handler = MetaWorkflowRunHandler(HANDLER_PENDING)
        meta_workflow_run_steps_dict = getattr(meta_workflow_run_handler, "meta_workflow_run_steps_dict")
        assert len(meta_workflow_run_steps_dict) == 4
        for mwf_name, mwf_run_step in meta_workflow_run_steps_dict.items():
            assert mwf_name in MWF_NAMES_LIST
            assert isinstance(mwf_run_step, MetaWorkflowRunStep)

    @pytest.mark.parametrize(
        "input_dict, updated_final_status",
        [
            (HANDLER_PENDING, PENDING),
            (HANDLER_STEPS_RUNNING, RUNNING),
            (HANDLER_STEPS_RUNNING_2, RUNNING),
            # (HANDLER_STEPS_RUNNING_3, RUNNING),
            (HANDLER_FAILED, FAILED),
            (HANDLER_FAILED_2, FAILED),
            (HANDLER_STOPPED, STOPPED),
            (HANDLER_COMPLETED, COMPLETED)
        ]
    )
    def test_update_final_status(self, input_dict, updated_final_status):
        """
        Tests the updating of the final_status attribute of a Run Handler
        based on the combination of MetaWorkflowRunStep object statuses.
        """
        meta_workflow_run_handler = MetaWorkflowRunHandler(input_dict)
        assert meta_workflow_run_handler.final_status == PENDING
        meta_workflow_run_handler.update_final_status()
        assert meta_workflow_run_handler.final_status == updated_final_status

    @pytest.mark.parametrize(
        "input_dict, meta_workflow_run_name, step_dict",
        [
            (HANDLER_PENDING, "A", MWFR_A_PENDING),
            (HANDLER_PENDING, "non_existent_mwf_run_step", None) # fails because invalid name
        ]
    )
    def test_retrieve_meta_workflow_run_step_obj_by_name(self, input_dict, meta_workflow_run_name, step_dict):
        """
        Tests the retrieval of a MetaWorkflowRunStep object by name.
        """
        try:
            meta_workflow_run_handler = MetaWorkflowRunHandler(input_dict)
            result = meta_workflow_run_handler._retrieve_meta_workflow_run_step_obj_by_name(meta_workflow_run_name)
        except KeyError as key_err_info:
            assert meta_workflow_run_name in str(key_err_info)
        else:
            step = MetaWorkflowRunStep(step_dict)
            assert type(result) == MetaWorkflowRunStep
            assert result.__dict__ == step.__dict__

    @pytest.mark.parametrize(
        "input_dict, mwfr_step_name_to_access, attribute_to_fetch, expected_value",
        [
            (HANDLER_COMPLETED, "A", "status", COMPLETED),
            (HANDLER_COMPLETED, "A", "non_existent_attr", None) # fails because invalid attribute name
        ]
    )
    def test_get_meta_workflow_run_step_attr(self, input_dict, mwfr_step_name_to_access, attribute_to_fetch, expected_value):
        """
        Tests the retrieval of a MetaWorkflowRunStep object's attribute.
        """
        handler_obj = MetaWorkflowRunHandler(input_dict)
        result = handler_obj.get_meta_workflow_run_step_attr(mwfr_step_name_to_access, attribute_to_fetch)
        assert result == expected_value


    @pytest.mark.parametrize(
        "input_dict, mwfr_step_name_to_update, attribute, value",
        [
            (HANDLER_COMPLETED, "A", "status", FAILED),
            (HANDLER_COMPLETED, "non_existent_mwf_run_step", None, None) # fails because invalid name
        ]
    )
    def test_update_meta_workflow_run_step_obj(self, input_dict, mwfr_step_name_to_update, attribute, value):
        """
        Tests the updating of a MetaWorkflowRunStep object' attribute with the provided value.
        """
        try:
            handler_obj = MetaWorkflowRunHandler(input_dict)
            attr_value_before_change = getattr(handler_obj.meta_workflow_run_steps_dict[mwfr_step_name_to_update], attribute)
            handler_obj.update_meta_workflow_run_step_obj(mwfr_step_name_to_update, attribute, value)
            attr_value_after_change = getattr(handler_obj.meta_workflow_run_steps_dict[mwfr_step_name_to_update], attribute)
            assert attr_value_before_change != attr_value_after_change
            assert attr_value_after_change == value
        except KeyError as key_err_info:
            assert mwfr_step_name_to_update in str(key_err_info)

    @pytest.mark.parametrize(
        "input_dict, steps_currently_pending",
        [
            (HANDLER_PENDING, MWF_NAMES_LIST),
            (HANDLER_STEPS_RUNNING, ["A", "D"]),
            (HANDLER_STEPS_RUNNING_2, ["A", "D"]),
            (HANDLER_FAILED, ["D"]),
            (HANDLER_FAILED_2, []),
            (HANDLER_COMPLETED, [])
        ]
    )
    def test_pending_steps(self, input_dict, steps_currently_pending):
        """
        Tests the listing of MetaWorkflow Run names that are pending.
        """
        handler_obj = MetaWorkflowRunHandler(input_dict)
        result = handler_obj.pending_steps()
        assert result == steps_currently_pending

    @pytest.mark.parametrize(
        "input_dict, steps_currently_running",
        [
            (HANDLER_PENDING, []),
            (HANDLER_STEPS_RUNNING, ["B", "C"]),
            (HANDLER_STEPS_RUNNING_2, ["C"]),
            (HANDLER_FAILED, []),
            (HANDLER_FAILED_2, ["D"]),
            (HANDLER_COMPLETED, [])
        ]
    )
    def test_running_steps(self, input_dict, steps_currently_running):
        """
        Tests the listing of MetaWorkflow Run names that are running.
        """
        handler_obj = MetaWorkflowRunHandler(input_dict)
        result = handler_obj.running_steps()
        assert result == steps_currently_running


    @pytest.mark.parametrize(
        "input_dict, mwfr_steps_to_update, attrs_to_update, updated_values, expected_meta_workflow_runs_array",
        [
            (HANDLER_STEPS_RUNNING, ["B", "B"], [STATUS, META_WORKFLOW_RUN], [COMPLETED, "a_link_to"], RUNNING_MWFR_ARRAY_2),
            (HANDLER_FAILED, ["A", "D"], [ERROR, STATUS], ["error_message", RUNNING], HALFWAY_DONE_N_FAIL_ARRAY_2),
            (HANDLER_STOPPED, ["A", "A"], [META_WORKFLOW_RUN, ERROR], ["another_link_to", "and_another_error_message"], HALFWAY_DONE_N_STOPPED_ARRAY_2)
        ]
    )
    def test_update_meta_workflow_runs_array(self, input_dict, mwfr_steps_to_update, attrs_to_update, updated_values, expected_meta_workflow_runs_array):
        """
        Tests the updating of a meta_workflow_runs array based on
        changed attributes of MetaWorkflowRunStep objects.
        """
        handler_obj = MetaWorkflowRunHandler(input_dict)
        # import pdb; pdb.set_trace()
        for idx in range(len(mwfr_steps_to_update)):
            handler_obj.update_meta_workflow_run_step_obj(mwfr_steps_to_update[idx], attrs_to_update[idx], updated_values[idx])
        
        result = handler_obj.update_meta_workflow_runs_array()
        assert result == expected_meta_workflow_runs_array