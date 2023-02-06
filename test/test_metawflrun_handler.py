#!/usr/bin/env python3

#################################################################
#   Libraries
#################################################################
import pytest
from copy import deepcopy

from magma.metawflrun_handler import MetaWorkflowRunStep, MetaWorkflowRunHandler

#################################################################
#   Vars
#################################################################

#TODO: make globals for attributes that you're checking in the tests

MWF_RUN_HANDLER_NAME = "test_mwf_run_handler"
PROJECT = "test_project"
INSTITUTION = "test_institution"
MWF_RUN_HANDLER_UUID = "test_mwf_run_handler_uuid"

TESTER_UUID = "test_item_uuid"

# statuses
PENDING = "pending"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
STOPPED = "stopped"

# basic meta_workflow steps (dicts) used in meta_workflows array
#TODO: for validation of basic attributes, what if the value of an attribute is None?
# e.g. name or meta_workflow in metaworkflowRunstep? (because my helper function
# only checks that you can get the actual attribute, but getattr works still
# if the value is None)
MWFR_A = {"name": "A"}
MWFR_B = {"name": "B"}
MWFR_C = {"name": "C"}
MWFR_D = {"name": "D"}

MWF_NAMES_LIST = ["B", "C", "A", "D"]

DEP_ON_A = ["A"]
DEP_ON_B = ["B"]
DEP_ON_C = ["C"]
DEP_ON_D = ["D"]

def mwf_run_with_added_attrs(metaworkflow_dict, dependencies=None, items_for_creation=None, status=None):
    dict_copy = deepcopy(metaworkflow_dict)
    if dependencies is not None:
        dict_copy["dependencies"] = dependencies
    if items_for_creation is not None:
        dict_copy["items_for_creation"] = items_for_creation
    if status is not None:
        dict_copy["status"] = status
    return dict_copy

def mwfr_handler_dict_generator(meta_workflow_runs_array):
    return {
        "name": MWF_RUN_HANDLER_NAME,
        "project": PROJECT,
        "institution": INSTITUTION,
        "uuid": MWF_RUN_HANDLER_UUID,
        "meta_workflow_handler": TESTER_UUID,
        "meta_workflow_runs": meta_workflow_runs_array
    }


# handler without uuid -- fails validation of basic attributes
HANDLER_WITHOUT_UUID_DICT = {
    "name": MWF_RUN_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "meta_workflow_handler": TESTER_UUID,
    "meta_workflow_runs": []
}

# handler without associated MetaWorkflow Handler uuid -- fails validation of basic attributes
HANDLER_WITHOUT_ASSOC_MWFH_DICT = {
    "name": MWF_RUN_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_RUN_HANDLER_UUID,
    "meta_workflow_runs": []
}

# handler without meta_workflow_runs array -- fails validation of basic attributes
HANDLER_WITHOUT_META_WORKFLOW_RUNS_ARRAY = {
    "name": MWF_RUN_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_RUN_HANDLER_UUID,
    "meta_workflow_handler": TESTER_UUID
}

# B -----> D
# |     ⋀  ⋀
# |   /    |
# ⋁ /      |
# A <----- C 
MWFR_A_PENDING = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, PENDING)
MWFR_B_PENDING = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, PENDING)
MWFR_C_PENDING = mwf_run_with_added_attrs(MWFR_C, [], TESTER_UUID, PENDING)
MWFR_D_PENDING = mwf_run_with_added_attrs(MWFR_D, DEP_ON_A + DEP_ON_B + DEP_ON_C, TESTER_UUID, PENDING)

MWFR_A_RUNNING = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, RUNNING)
MWFR_B_RUNNING = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, RUNNING)
MWFR_C_RUNNING = mwf_run_with_added_attrs(MWFR_C, [], TESTER_UUID, RUNNING)
MWFR_D_RUNNING = mwf_run_with_added_attrs(MWFR_D, DEP_ON_A + DEP_ON_B + DEP_ON_C, TESTER_UUID, RUNNING)

MWFR_A_FAILED = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, FAILED)

MWFR_A_STOPPED = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C, TESTER_UUID, STOPPED)

MWFR_A_COMPLETED = mwf_run_with_added_attrs(MWFR_A, DEP_ON_B + DEP_ON_C,  TESTER_UUID, COMPLETED)
MWFR_B_COMPLETED = mwf_run_with_added_attrs(MWFR_B, [], TESTER_UUID, COMPLETED)
MWFR_C_COMPLETED = mwf_run_with_added_attrs(MWFR_C, [], TESTER_UUID, COMPLETED)
MWFR_D_COMPLETED = mwf_run_with_added_attrs(MWFR_D, DEP_ON_A + DEP_ON_B + DEP_ON_C, TESTER_UUID, COMPLETED)


PENDING_ARRAY = [MWFR_B_PENDING, MWFR_C_PENDING, MWFR_A_PENDING, MWFR_D_PENDING]
HANDLER_PENDING = mwfr_handler_dict_generator(PENDING_ARRAY)

RUNNING_MWFR_ARRAY = [MWFR_B_RUNNING, MWFR_C_RUNNING, MWFR_A_PENDING, MWFR_D_PENDING]
RUNNING_MWFR_ARRAY_2 = [MWFR_B_COMPLETED, MWFR_C_RUNNING, MWFR_A_PENDING, MWFR_D_PENDING]
# this wouldn't happen with THIS dag in particular, 
# but could in other cases (made for the sake of the final_status test for the handler)
RUNNING_MWFR_ARRAY_3 = [MWFR_B_COMPLETED, MWFR_C_PENDING, MWFR_A_RUNNING, MWFR_D_PENDING]
HANDLER_STEPS_RUNNING = mwfr_handler_dict_generator(RUNNING_MWFR_ARRAY)
HANDLER_STEPS_RUNNING_2 = mwfr_handler_dict_generator(RUNNING_MWFR_ARRAY_2)
HANDLER_STEPS_RUNNING_3 = mwfr_handler_dict_generator(RUNNING_MWFR_ARRAY_3)

HALFWAY_DONE_N_FAIL_ARRAY = [MWFR_B_COMPLETED, MWFR_C_COMPLETED, MWFR_A_FAILED, MWFR_D_PENDING]
HALFWAY_DONE_N_FAIL_ARRAY_2 = [MWFR_B_COMPLETED, MWFR_C_COMPLETED, MWFR_A_FAILED, MWFR_D_RUNNING]
HANDLER_FAILED = mwfr_handler_dict_generator(HALFWAY_DONE_N_FAIL_ARRAY)
HANDLER_FAILED_2 = mwfr_handler_dict_generator(HALFWAY_DONE_N_FAIL_ARRAY_2)

HALFWAY_DONE_N_STOPPED_ARRAY = [MWFR_B_COMPLETED, MWFR_C_COMPLETED, MWFR_A_STOPPED, MWFR_D_PENDING]
HANDLER_STOPPED = mwfr_handler_dict_generator(HALFWAY_DONE_N_STOPPED_ARRAY)

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
        # import pdb; pdb.set_trace()
        assert num_attributes == len(meta_workflow_run_step_object.__dict__)
        assert meta_workflow_run_step_object.status == PENDING

        required_attributes = ["name", "dependencies"]#, "items_for_creation"]
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
        no errors raised.
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
        assert getattr(meta_workflow_run_handler, "final_status") == PENDING
        required_attributes = ["uuid", "meta_workflow_handler"]
        for attr in required_attributes:
            assert hasattr(meta_workflow_run_handler, attr) == True

    @pytest.mark.parametrize(
        "input_dict",
        [
            (HANDLER_WITHOUT_UUID_DICT), # fails because no uuid
            (HANDLER_WITHOUT_ASSOC_MWFH_DICT), # fails because no associated metaworkflow handler
            (HANDLER_WITHOUT_META_WORKFLOW_RUNS_ARRAY) # fails because no meta_workflow_runs array
        ]
    )
    def test_attribute_validation_attribute_errors(self, input_dict):
        """
        Tests creation of appropriate MetaWorkflowRunHandler objects,
        no errors raised.
        """
        with pytest.raises(AttributeError) as attr_err_info:
            MetaWorkflowRunHandler(input_dict)
        assert "Object validation error" in str(attr_err_info.value)

    def test_create_meta_workflow_run_step_objects(self):
        meta_workflow_run_handler = MetaWorkflowRunHandler(HANDLER_PENDING)
        meta_workflow_run_step_dict = getattr(meta_workflow_run_handler, "meta_workflow_run_step_dict")
        assert len(meta_workflow_run_step_dict) == 4
        for mwf_name, mwf_run_step in meta_workflow_run_step_dict.items():
            assert mwf_name in MWF_NAMES_LIST
            assert isinstance(mwf_run_step, MetaWorkflowRunStep)

    @pytest.mark.parametrize(
        "input_dict, updated_final_status",
        [
            (HANDLER_PENDING, PENDING),
            (HANDLER_STEPS_RUNNING, RUNNING),
            (HANDLER_STEPS_RUNNING_2, RUNNING),
            (HANDLER_STEPS_RUNNING_3, RUNNING),
            (HANDLER_FAILED, FAILED),
            (HANDLER_FAILED_2, FAILED),
            (HANDLER_STOPPED, STOPPED),
            (HANDLER_COMPLETED, COMPLETED)
        ]
    )
    def test_update_final_status(self, input_dict, updated_final_status):
        meta_workflow_run_handler = MetaWorkflowRunHandler(input_dict)
        assert meta_workflow_run_handler.final_status == PENDING
        meta_workflow_run_handler.update_final_status()
        assert meta_workflow_run_handler.final_status == updated_final_status

    @pytest.mark.parametrize(
        "input_dict, mwfr_step_name_to_reset",
        [
            (HANDLER_COMPLETED, "A"),
            (HANDLER_COMPLETED, "non_existent_mwf_run_step")
        ]
    )
    def test_reset_meta_workflow_run_step(self, input_dict, mwfr_step_name_to_reset):
        try:
            handler_obj = MetaWorkflowRunHandler(input_dict)
            prior_step_status = handler_obj.meta_workflow_run_step_dict[mwfr_step_name_to_reset].status
            handler_obj.reset_meta_workflow_run_step(mwfr_step_name_to_reset)
            updated_step_status = handler_obj.meta_workflow_run_step_dict[mwfr_step_name_to_reset].status
            assert prior_step_status != updated_step_status
            assert updated_step_status == PENDING
            updated_step_run = handler_obj.meta_workflow_run_step_dict[mwfr_step_name_to_reset].meta_workflow_run
            assert updated_step_run is None
        except KeyError as key_err_info:
            assert mwfr_step_name_to_reset in str(key_err_info)

    @pytest.mark.parametrize(
        "input_dict, mwfr_step_name_to_update, attribute, value",
        [
            (HANDLER_COMPLETED, "A", "status", FAILED),
            (HANDLER_COMPLETED, "non_existent_mwf_run_step", None, None)
        ]
    )
    def test_update_meta_workflow_run_step(self, input_dict, mwfr_step_name_to_update, attribute, value):
        try:
            handler_obj = MetaWorkflowRunHandler(input_dict)
            attr_value_before_change = getattr(handler_obj.meta_workflow_run_step_dict[mwfr_step_name_to_update], attribute)
            handler_obj.update_meta_workflow_run_step(mwfr_step_name_to_update, attribute, value)
            attr_value_after_change = getattr(handler_obj.meta_workflow_run_step_dict[mwfr_step_name_to_update], attribute)
            assert attr_value_before_change != attr_value_after_change
            assert attr_value_after_change == value
        except KeyError as key_err_info:
            assert mwfr_step_name_to_update in str(key_err_info)

    @pytest.mark.parametrize(
        "input_dict, steps_to_run",
        [
            (HANDLER_PENDING, MWF_NAMES_LIST),
            (HANDLER_STEPS_RUNNING, ["A", "D"]),
            (HANDLER_STEPS_RUNNING_2, ["A", "D"]),
            (HANDLER_FAILED, ["D"]),
            (HANDLER_FAILED_2, []),
            (HANDLER_COMPLETED, [])
        ]
    )
    def test_pending_steps(self, input_dict, steps_to_run):
        handler_obj = MetaWorkflowRunHandler(input_dict)
        result = handler_obj.pending_steps()
        assert result == steps_to_run

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
        handler_obj = MetaWorkflowRunHandler(input_dict)
        result = handler_obj.running_steps()
        assert result == steps_currently_running