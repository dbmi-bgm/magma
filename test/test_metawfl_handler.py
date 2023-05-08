#!/usr/bin/env python3
 
#################################################################
#   Libraries
#################################################################
import pytest
from copy import deepcopy

from magma.metawfl_handler import *
from magma.magma_constants import *

#################################################################
#   Vars
#################################################################

MWF_HANDLER_NAME = "test_mwf_handler"
MWF_HANDLER_PROJECT = "test_project"
MWF_HANDLER_INSTITUTION = "test_institution"
MWF_HANDLER_UUID = "test_mwf_handler_uuid"

TESTER_UUID = "test_item_uuid"

# Basic meta_workflow steps (dicts) used in meta_workflows array
MWF_A = {"meta_workflow": "test_mwf_uuid_0", "name": "A"}
MWF_B = {"meta_workflow": "test_mwf_uuid_1", "name": "B"}
MWF_C = {"meta_workflow": "test_mwf_uuid_2", "name": "C"}
MWF_D = {"meta_workflow": "test_mwf_uuid_3", "name": "D"}

# Dependencies
DEP_ON_A = ["A"]
DEP_ON_B = ["B"]
DEP_ON_C = ["C"]
DEP_ON_D = ["D"]

def meta_workflow_with_added_attrs(meta_workflow_dict, items_for_creation_property_trace=None, items_for_creation_uuid=None, dependencies=None):
    """
    Helper function used to add non-required attributes to a MetaWorkflow step input dictionary.
    Returns new MetaWorkflow step dictionary with added attributes.

    :param meta_workflow_dict: dictionary containing required attributes for MetaWorkflow step ("name" and "meta_workflow"):
    :type meta_workflow_dict: dic
    :param items_for_creation_property_trace: property trace(s) of item(s) required to create MetaWorkflow Run from MetaWorkflow
    :type items_for_creation_property_trace: str or list[str] or None
    :param items_for_creation_uuid: uuid(s) of item(s) required to create MetaWorkflow Run from MetaWorkflow
    :type items_for_creation_uuid: str or list[str] or None
    :param dependencies: list of MetaWorkflows (names) that the current MetaWorkflow is dependent on
    :type dependencies: list[str]
    :return: reformatted MetaWorkflow dictionary with added attributes
    """
    dict_copy = deepcopy(meta_workflow_dict)
    if items_for_creation_property_trace:
        dict_copy[ITEMS_FOR_CREATION_PROP_TRACE] = items_for_creation_property_trace
    if items_for_creation_uuid:
        dict_copy[ITEMS_FOR_CREATION_UUID] = items_for_creation_uuid
    if dependencies is not None:
        dict_copy[DEPENDENCIES] = dependencies
    return dict_copy


# meta_workflows arrays for MetaWorkflow Handler
# handler without uuid -- fails validation of basic attributes
HANDLER_WITHOUT_UUID_DICT = {
    NAME: MWF_HANDLER_NAME,
    PROJECT: MWF_HANDLER_PROJECT,
    INSTITUTION: MWF_HANDLER_INSTITUTION
}

# handler without meta_workflows array -- passes validation, should set empty metaworkflows array
HANDLER_WITHOUT_MWF_ARRAY_DICT = {
    NAME: MWF_HANDLER_NAME,
    PROJECT: MWF_HANDLER_PROJECT,
    INSTITUTION: MWF_HANDLER_INSTITUTION,
    UUID: MWF_HANDLER_UUID
}

# DAG_0
# A     B -----> C
MWF_A_DAG_0 = meta_workflow_with_added_attrs(MWF_A, None, TESTER_UUID, [])
MWF_B_DAG_0 = meta_workflow_with_added_attrs(MWF_B, None, TESTER_UUID, [])
MWF_B_DAG_0_W_DEP = meta_workflow_with_added_attrs(MWF_B, None, TESTER_UUID, DEP_ON_A)
MWF_C_DAG_0 = meta_workflow_with_added_attrs(MWF_C, None, TESTER_UUID, DEP_ON_B)
DAG_0_MWF_ARRAY = [MWF_B_DAG_0, MWF_A_DAG_0, MWF_C_DAG_0] # purposely in this order to test toposort
HANDLER_DAG_0 = {
    NAME: MWF_HANDLER_NAME,
    PROJECT: MWF_HANDLER_PROJECT,
    INSTITUTION: MWF_HANDLER_INSTITUTION,
    UUID: MWF_HANDLER_UUID,
    META_WORKFLOWS: DAG_0_MWF_ARRAY
}
DAG_0_MWF_ARRAY_W_DUPLICATES = [MWF_B_DAG_0, MWF_A_DAG_0, MWF_C_DAG_0, MWF_B_DAG_0]
HANDLER_DAG_0_W_DUPLICATES = {
    NAME: MWF_HANDLER_NAME,
    PROJECT: MWF_HANDLER_PROJECT,
    INSTITUTION: MWF_HANDLER_INSTITUTION,
    UUID: MWF_HANDLER_UUID,
    META_WORKFLOWS: DAG_0_MWF_ARRAY_W_DUPLICATES
}
DAG_0_MWF_ARRAY_W_DUPLICATES_BY_MWF_NAME = [MWF_B_DAG_0, MWF_A_DAG_0, MWF_C_DAG_0, MWF_B_DAG_0_W_DEP]
HANDLER_DAG_0_W_DUPLICATES_BY_MWF_NAME = {
    NAME: MWF_HANDLER_NAME,
    PROJECT: MWF_HANDLER_PROJECT,
    INSTITUTION: MWF_HANDLER_INSTITUTION,
    UUID: MWF_HANDLER_UUID,
    META_WORKFLOWS: DAG_0_MWF_ARRAY_W_DUPLICATES_BY_MWF_NAME
}
REORDERED_MWFS_DAG_0 = [["A", "B", "C"], ["B", "A", "C"], ["B", "C", "A"]]

# DAG_1
# B -----> D
# |     ⋀  ⋀
# |   /    |
# ⋁ /      |
# A <----- C 
MWF_A_DAG_1 = meta_workflow_with_added_attrs(MWF_A, None, TESTER_UUID, DEP_ON_B + DEP_ON_C)
MWF_B_DAG_1 = meta_workflow_with_added_attrs(MWF_B, None, TESTER_UUID, [])
MWF_C_DAG_1 = meta_workflow_with_added_attrs(MWF_C, None, TESTER_UUID, [])
MWF_D_DAG_1 = meta_workflow_with_added_attrs(MWF_D, None, TESTER_UUID, DEP_ON_A + DEP_ON_B + DEP_ON_C)
DAG_1_MWF_ARRAY = [MWF_A_DAG_1, MWF_B_DAG_1, MWF_C_DAG_1, MWF_D_DAG_1]
HANDLER_DAG_1 = {
    NAME: MWF_HANDLER_NAME,
    PROJECT: MWF_HANDLER_PROJECT,
    INSTITUTION: MWF_HANDLER_INSTITUTION,
    UUID: MWF_HANDLER_UUID,
    META_WORKFLOWS: DAG_1_MWF_ARRAY
}
REORDERED_MWFS_DAG_1 = [["B", "C", "A", "D"], ["C", "B", "A", "D"]]

# CYCLIC_0
# A        B__
#          ⋀  \_____ 
#          |        |
#          |        |
#          C <----- | 
MWF_A_CYCLIC_0 = meta_workflow_with_added_attrs(MWF_A, None, TESTER_UUID, [])
MWF_B_CYCLIC_0 = meta_workflow_with_added_attrs(MWF_B, None, TESTER_UUID, DEP_ON_C)
MWF_C_CYCLIC_0 = meta_workflow_with_added_attrs(MWF_C, None, TESTER_UUID, DEP_ON_B)
CYCLIC_0_MWF_ARRAY = [MWF_A_CYCLIC_0, MWF_B_CYCLIC_0, MWF_C_CYCLIC_0]
HANDLER_CYCLIC_0 = {
    NAME: MWF_HANDLER_NAME,
    PROJECT: MWF_HANDLER_PROJECT,
    INSTITUTION: MWF_HANDLER_INSTITUTION,
    UUID: MWF_HANDLER_UUID,
    META_WORKFLOWS: CYCLIC_0_MWF_ARRAY
}

# CYCLIC_1
# A -----> B
# ⋀        |
# |        |
# |        ⋁
# D <----- C 
MWF_A_CYCLIC_1 = meta_workflow_with_added_attrs(MWF_A, None, TESTER_UUID, DEP_ON_D)
MWF_B_CYCLIC_1 = meta_workflow_with_added_attrs(MWF_B, None, TESTER_UUID, DEP_ON_A)
MWF_C_CYCLIC_1 = meta_workflow_with_added_attrs(MWF_C, None, TESTER_UUID, DEP_ON_B)
MWF_D_CYCLIC_1 = meta_workflow_with_added_attrs(MWF_D, None, TESTER_UUID, DEP_ON_C)
CYCLIC_1_MWF_ARRAY = [MWF_A_CYCLIC_1, MWF_B_CYCLIC_1, MWF_C_CYCLIC_1, MWF_D_CYCLIC_1]
HANDLER_CYCLIC_1 = {
    NAME: MWF_HANDLER_NAME,
    PROJECT: MWF_HANDLER_PROJECT,
    INSTITUTION: MWF_HANDLER_INSTITUTION,
    UUID: MWF_HANDLER_UUID,
    META_WORKFLOWS: CYCLIC_1_MWF_ARRAY
}

#################################################################
#   Tests
#################################################################
class TestMetaWorkflowStep:
    @pytest.mark.parametrize(
        "mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, num_attributes",
        [
            (MWF_A, "sample_processing.samples", None, None, 3),
            (MWF_B, None, TESTER_UUID, None, 3),
            (MWF_B, None, TESTER_UUID, DEP_ON_A, 4)
        ]
    )
    def test_attribute_validation_no_errors(self, mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, num_attributes):
        """
        Tests creation of appropriate MetaWorkflowStep objects,
        no errors raised.
        """
        completed_dict = meta_workflow_with_added_attrs(mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies)
        meta_workflow_step_object = MetaWorkflowStep(completed_dict)
        assert num_attributes == len(meta_workflow_step_object.__dict__)

        required_attributes = [META_WORKFLOW, NAME]
        for attr in required_attributes:
            assert hasattr(meta_workflow_step_object, attr) == True

    @pytest.mark.parametrize(
        "mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies",
        [
            (MWF_C, "sample_processing.samples", TESTER_UUID, None), # has both uuid and property trace for items for creation
            (MWF_A, None, None, None), # missing items for creation
        ]
    )
    def test_attribute_validation_attribute_errors(self, mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies):
        """
        Tests creation of appropriate MetaWorkflowStep objects,
        Attribute Error raised due to missing required attributes.
        """
        with pytest.raises(AttributeError) as attr_err_info:
            completed_dict = meta_workflow_with_added_attrs(mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies)
            MetaWorkflowStep(completed_dict)
        assert "Object validation error" in str(attr_err_info.value)

    @pytest.mark.parametrize(
        "mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies",
        [
            (MWF_A, None, TESTER_UUID, DEP_ON_A)
        ]
    )
    def test_check_self_dep(self, mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies):
        """
        Tests the method that checks that a MetaWorkflow Step doesn't depend on itself.
        """
        with pytest.raises(MetaWorkflowStepSelfDependencyError) as self_dep_err_err_info:
            completed_dict = meta_workflow_with_added_attrs(mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies)
            MetaWorkflowStep(completed_dict)
        assert mwf_step_dict[NAME] in str(self_dep_err_err_info.value)


class TestMetaWorkflowHandler:
    @pytest.mark.parametrize(
        "mwf_handler_dict",
        [(HANDLER_WITHOUT_UUID_DICT), (HANDLER_WITHOUT_MWF_ARRAY_DICT)]
    )
    def test_attribute_validation_mwf_handler(self, mwf_handler_dict):
        """
        Tests that makes sure handler has all required attributes ("uuid").
        """
        try:
            handler_obj = MetaWorkflowHandler(mwf_handler_dict)
        except AttributeError as attr_err_info:
            assert "Object validation error" in str(attr_err_info)
        else:
            assert hasattr(handler_obj, UUID) == True

        

    @pytest.mark.parametrize(
        "mwf_handler_dict, length_of_mwf_dict",
        [
            (HANDLER_WITHOUT_MWF_ARRAY_DICT, 0), # sets empty dict if attr not present
            (HANDLER_DAG_0, 3),
        ]
    )
    def test_set_meta_workflows_dict(self, mwf_handler_dict, length_of_mwf_dict):
        """
        Tests the creation of MetaWorkflow Step(s) dictionary.
        """
        meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
        assert len(getattr(meta_workflow_handler, META_WORKFLOWS)) == length_of_mwf_dict

        meta_workflow_steps_dict = getattr(meta_workflow_handler, META_WORKFLOWS)
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
        """
        Tests for the check of duplicate MetaWorkflow Steps, by name, during
        creation of the MetaWorkflow Step(s) dictionary.
        """
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
        """
        Tests the topological sorting of MetaWorkflow steps.
        """
        meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
        assert getattr(meta_workflow_handler, "ordered_meta_workflows") in possible_reordered_mwf_lists
        # TODO: add to constants file?


    @pytest.mark.parametrize(
        "mwf_handler_dict",
        [
            (HANDLER_CYCLIC_0),
            (HANDLER_CYCLIC_1)
        ]
    )
    def test_cycles(self, mwf_handler_dict):
        """
        Tests the topological sorting of MetaWorkflow steps,
        raising MetaWorkflowStepCycleError because of presence of cycles.
        """
        with pytest.raises(MetaWorkflowStepCycleError) as cycle_err_info:
            MetaWorkflowHandler(mwf_handler_dict)
            assert "nodes are in a cycle" in str(cycle_err_info)