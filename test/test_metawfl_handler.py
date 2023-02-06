#!/usr/bin/env python3
 
#################################################################
#   Libraries
#################################################################
import pytest
from copy import deepcopy

from magma.metawfl_handler import *

#TODO: throw error on self dependency

#################################################################
#   Vars
#################################################################

MWF_HANDLER_NAME = "test_mwf_handler"
PROJECT = "test_project"
INSTITUTION = "test_institution"
MWF_HANDLER_UUID = "test_mwf_handler_uuid"

TESTER_UUID = "test_item_uuid"

# basic meta_workflow steps (dicts) used in meta_workflows array
#TODO: for validation of basic attributes, what if the value of an attribute is None?
# e.g. name or meta_workflow in metaworkflowstep? (because my helper function
# only checks that you can get the actual attribute, but getattr works still
# if the value is None)
MWF_A = {"meta_workflow": "test_mwf_uuid_0", "name": "A"}
MWF_B = {"meta_workflow": "test_mwf_uuid_1", "name": "B"}
MWF_C = {"meta_workflow": "test_mwf_uuid_2", "name": "C"}
MWF_D = {"meta_workflow": "test_mwf_uuid_3", "name": "D"}

DEP_ON_A = ["A"]
DEP_ON_B = ["B"]
DEP_ON_C = ["C"]
DEP_ON_D = ["D"]

def mwf_with_added_attrs(metaworkflow_dict, items_for_creation_property_trace=None, items_for_creation_uuid=None, dependencies=None, duplication_flag=None):
    dict_copy = deepcopy(metaworkflow_dict)
    if items_for_creation_property_trace:
        dict_copy["items_for_creation_property_trace"] = items_for_creation_property_trace
    if items_for_creation_uuid:
        dict_copy["items_for_creation_uuid"] = items_for_creation_uuid
    if dependencies is not None:
        dict_copy["dependencies"] = dependencies
    if duplication_flag is not None:
        dict_copy["duplication_flag"] = duplication_flag
    return dict_copy


# meta_workflows arrays for MetaWorkflow Handler
# handler without uuid -- fails validation of basic attributes
HANDLER_WITHOUT_UUID_DICT = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION
}

# handler without metaworkflows array -- passes validation, should set empty metaworkflows array
HANDLER_WITHOUT_MWF_ARRAY_DICT = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_HANDLER_UUID
}

# DAG_0
# A     B -----> C
MWF_A_DAG_0 = mwf_with_added_attrs(MWF_A, None, TESTER_UUID, [], True)
MWF_B_DAG_0 = mwf_with_added_attrs(MWF_B, None, TESTER_UUID, [], True)
MWF_B_DAG_0_W_DEP = mwf_with_added_attrs(MWF_B, None, TESTER_UUID, DEP_ON_A, True)
MWF_C_DAG_0 = mwf_with_added_attrs(MWF_C, None, TESTER_UUID, DEP_ON_B, True)
DAG_0_MWF_ARRAY = [MWF_B_DAG_0, MWF_A_DAG_0, MWF_C_DAG_0] # purposely in this order to test toposort
HANDLER_DAG_0 = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_HANDLER_UUID,
    "meta_workflows": DAG_0_MWF_ARRAY
}
DAG_0_MWF_ARRAY_W_DUPLICATES = [MWF_B_DAG_0, MWF_A_DAG_0, MWF_C_DAG_0, MWF_B_DAG_0]
HANDLER_DAG_0_W_DUPLICATES = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_HANDLER_UUID,
    "meta_workflows": DAG_0_MWF_ARRAY_W_DUPLICATES
}
DAG_0_MWF_ARRAY_W_DUPLICATES_BY_MWF_NAME = [MWF_B_DAG_0, MWF_A_DAG_0, MWF_C_DAG_0, MWF_B_DAG_0_W_DEP]
HANDLER_DAG_0_W_DUPLICATES_BY_MWF_NAME = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_HANDLER_UUID,
    "meta_workflows": DAG_0_MWF_ARRAY_W_DUPLICATES_BY_MWF_NAME
}
REORDERED_MWFS_DAG_0 = [["A", "B", "C"], ["B", "A", "C"], ["B", "C", "A"]]

# DAG_1
# B -----> D
# |     ⋀  ⋀
# |   /    |
# ⋁ /      |
# A <----- C 
MWF_A_DAG_1 = mwf_with_added_attrs(MWF_A, None, TESTER_UUID, DEP_ON_B + DEP_ON_C, True)
MWF_B_DAG_1 = mwf_with_added_attrs(MWF_B, None, TESTER_UUID, [], True)
MWF_C_DAG_1 = mwf_with_added_attrs(MWF_C, None, TESTER_UUID, [], True)
MWF_D_DAG_1 = mwf_with_added_attrs(MWF_D, None, TESTER_UUID, DEP_ON_A + DEP_ON_B + DEP_ON_C, True)
DAG_1_MWF_ARRAY = [MWF_A_DAG_1, MWF_B_DAG_1, MWF_C_DAG_1, MWF_D_DAG_1]
HANDLER_DAG_1 = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_HANDLER_UUID,
    "meta_workflows": DAG_1_MWF_ARRAY
}
REORDERED_MWFS_DAG_1 = [["B", "C", "A", "D"], ["C", "B", "A", "D"]]

# CYCLIC_0
# A        B__
#          ⋀  \_____ 
#          |        |
#          |        |
#          C <----- | 
MWF_A_CYCLIC_0 = mwf_with_added_attrs(MWF_A, None, TESTER_UUID, [], True)
MWF_B_CYCLIC_0 = mwf_with_added_attrs(MWF_B, None, TESTER_UUID, DEP_ON_C, True)
MWF_C_CYCLIC_0 = mwf_with_added_attrs(MWF_C, None, TESTER_UUID, DEP_ON_B, True)
CYCLIC_0_MWF_ARRAY = [MWF_A_CYCLIC_0, MWF_B_CYCLIC_0, MWF_C_CYCLIC_0]
HANDLER_CYCLIC_0 = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_HANDLER_UUID,
    "meta_workflows": CYCLIC_0_MWF_ARRAY
}

# CYCLIC_1
# A -----> B
# ⋀        |
# |        |
# |        ⋁
# D <----- C 
MWF_A_CYCLIC_1 = mwf_with_added_attrs(MWF_A, None, TESTER_UUID, DEP_ON_D, True)
MWF_B_CYCLIC_1 = mwf_with_added_attrs(MWF_B, None, TESTER_UUID, DEP_ON_A, True)
MWF_C_CYCLIC_1 = mwf_with_added_attrs(MWF_C, None, TESTER_UUID, DEP_ON_B, True)
MWF_D_CYCLIC_1 = mwf_with_added_attrs(MWF_D, None, TESTER_UUID, DEP_ON_C, True)
CYCLIC_1_MWF_ARRAY = [MWF_A_CYCLIC_1, MWF_B_CYCLIC_1, MWF_C_CYCLIC_1, MWF_D_CYCLIC_1]
HANDLER_CYCLIC_1 = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_HANDLER_UUID,
    "meta_workflows": CYCLIC_1_MWF_ARRAY
}

#################################################################
#   Tests
#################################################################
class TestMetaWorkflowStep:
    @pytest.mark.parametrize(
        "mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag, num_attributes",
        [
            (MWF_A, "sample_processing.samples", None, None, True, 4),
            (MWF_B, None, TESTER_UUID, None, False, 4),
            (MWF_B, None, TESTER_UUID, DEP_ON_A, True, 5)
        ]
    )
    def test_attribute_validation_no_errors(self, mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag, num_attributes):
        """
        Tests creation of appropriate MetaWorkflowStep objects,
        no errors raised.
        """
        completed_dict = mwf_with_added_attrs(mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag)
        meta_workflow_step_object = MetaWorkflowStep(completed_dict)
        assert num_attributes == len(meta_workflow_step_object.__dict__)

        required_attributes = ["meta_workflow", "name", "duplication_flag"]
        for attr in required_attributes:
            assert hasattr(meta_workflow_step_object, attr) == True

    @pytest.mark.parametrize(
        "mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag",
        [
            (MWF_C, "sample_processing.samples", TESTER_UUID, None, True), # has both uuid and property trace for items for creation
            (MWF_A, None, None, None, True), # missing items for creation
            (MWF_A, None, TESTER_UUID, None, None) # missing duplication flag
        ]
    )
    def test_attribute_validation_attribute_errors(self, mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag):
        """
        Tests creation of appropriate MetaWorkflowStep objects,
        no errors raised.
        """
        with pytest.raises(AttributeError) as attr_err_info:
            completed_dict = mwf_with_added_attrs(mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag)
            MetaWorkflowStep(completed_dict)
        assert "Object validation error" in str(attr_err_info.value)

    @pytest.mark.parametrize(
        "mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag",
        [
            (MWF_A, None, TESTER_UUID, DEP_ON_A, True)
        ]
    )
    def test_check_self_dep(self, mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag):
        """
        """
        with pytest.raises(MetaWorkflowStepSelfDependencyError) as self_dep_err_err_info:
            completed_dict = mwf_with_added_attrs(mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag)
            MetaWorkflowStep(completed_dict)
        assert mwf_step_dict["name"] in str(self_dep_err_err_info.value)


class TestMetaWorkflowHandler:
    @pytest.mark.parametrize(
        "mwf_handler_dict",
        [(HANDLER_WITHOUT_UUID_DICT), (HANDLER_WITHOUT_MWF_ARRAY_DICT)]
    )
    def test_attribute_validation_mwf_handler(self, mwf_handler_dict):
        try:
            handler_obj = MetaWorkflowHandler(mwf_handler_dict)
        except AttributeError as attr_err_info:
            assert "Object validation error" in str(attr_err_info)
        else:
            assert hasattr(handler_obj, "uuid") == True

        

    @pytest.mark.parametrize(
        "mwf_handler_dict, length_of_mwf_dict",
        [
            (HANDLER_WITHOUT_MWF_ARRAY_DICT, 0), # sets empty dict if attr not present
            (HANDLER_DAG_0, 3),
        ]
    )
    def test_set_meta_workflows_dict(self, mwf_handler_dict, length_of_mwf_dict):
        meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
        assert len(getattr(meta_workflow_handler, "meta_workflows")) == length_of_mwf_dict

        meta_workflow_steps_dict = getattr(meta_workflow_handler, "meta_workflows")
        assert isinstance(meta_workflow_steps_dict, dict)
        for step in meta_workflow_steps_dict.values():
            assert isinstance(step, MetaWorkflowStep)

    @pytest.mark.parametrize(
        "mwf_handler_dict",
        [
            (HANDLER_DAG_0_W_DUPLICATES), # complete duplicates
            (HANDLER_DAG_0_W_DUPLICATES_BY_MWF_NAME) # duplicates by mwf name
        ]
    )
    def test_set_meta_workflows_dict_w_error(self, mwf_handler_dict):
        with pytest.raises(MetaWorkflowStepDuplicateError) as dup_err_info:
            MetaWorkflowHandler(mwf_handler_dict)
            assert '"B" is a duplicate MetaWorkflow' in str(dup_err_info)


    @pytest.mark.parametrize(
        "mwf_handler_dict, possible_reordered_mwf_lists",
        [
            (HANDLER_WITHOUT_MWF_ARRAY_DICT, [[]]),
            (HANDLER_DAG_0, REORDERED_MWFS_DAG_0),
            (HANDLER_DAG_1, REORDERED_MWFS_DAG_1)
        ]
    )
    def test_create_ordered_meta_workflows_list(self, mwf_handler_dict, possible_reordered_mwf_lists):
        meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
        assert getattr(meta_workflow_handler, "ordered_meta_workflows") in possible_reordered_mwf_lists


    @pytest.mark.parametrize(
        "mwf_handler_dict",
        [
            (HANDLER_CYCLIC_0),
            (HANDLER_CYCLIC_1)
        ]
    )
    def test_cycles(self, mwf_handler_dict):
        with pytest.raises(MetaWorkflowStepCycleError) as cycle_err_info:
            MetaWorkflowHandler(mwf_handler_dict)
            assert "nodes are in a cycle" in str(cycle_err_info)