#################################################################
#   Libraries
#################################################################
import pytest
from copy import deepcopy

from magma.metawfl_handler import MetaWorkflowStep, MetaWorkflowHandler

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
HANDLER_WITHOUT_UUID = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION
}

# handler without metaworkflows array -- passes validation, should set empty metaworkflows array
HANDLER_WITHOUT_MWF_ARRAY = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_HANDLER_UUID
}

# DAG_0
# A     B -----> C
MWF_A_DAG_0 = mwf_with_added_attrs(MWF_A, None, TESTER_UUID, [], True)
MWF_B_DAG_0 = mwf_with_added_attrs(MWF_B, None, TESTER_UUID, [], True)
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
REORDERED_MWF_ARRAY_DAG_0 = [MWF_B_DAG_0, MWF_A_DAG_0, MWF_C_DAG_0]

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
MWF_B_DAG_1_W_SELF_DEP = mwf_with_added_attrs(MWF_B, None, TESTER_UUID, DEP_ON_B, True)
DAG_1_MWF_ARRAY_W_SELF_DEP = [MWF_A_DAG_1, MWF_B_DAG_1_W_SELF_DEP, MWF_C_DAG_1, MWF_D_DAG_1]
HANDLER_DAG_1_W_SELF_DEP = {
    "name": MWF_HANDLER_NAME,
    "project": PROJECT,
    "institution": INSTITUTION,
    "uuid": MWF_HANDLER_UUID,
    "meta_workflows": DAG_1_MWF_ARRAY_W_SELF_DEP
}
REORDERED_MWF_ARRAY_DAG_1 = [MWF_B_DAG_1, MWF_C_DAG_1, MWF_A_DAG_1, MWF_D_DAG_1]

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

#TODO: make test for ValidatedDictionary parent object?
# I basically test it through the child classes below

@pytest.mark.parametrize(
    "mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag, num_attributes",
    [
        (MWF_A, "sample_processing.samples", None, None, True, 4),
        (MWF_B, None, TESTER_UUID, None, False, 4),
        (MWF_B, None, TESTER_UUID, DEP_ON_A, True, 5),
        (MWF_C, "sample_processing.samples", TESTER_UUID, None, True, 5), # items for creation UUID taken by default
        # the following should throw ValueError
        (MWF_A, None, None, None, True, None), # missing items for creation
        (MWF_A, None, TESTER_UUID, None, None, None) # missing duplication flag
    ]
)
def test_attribute_validation_mwf_step(mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag, num_attributes):
    try:
        completed_dict = mwf_with_added_attrs(mwf_step_dict, items_for_creation_property_trace, items_for_creation_uuid, dependencies, duplication_flag)
        meta_workflow_step_object = MetaWorkflowStep(completed_dict)
    except ValueError as val_err_info:
        assert "validation error" in str(val_err_info)
    else:
        assert num_attributes == len(meta_workflow_step_object.__dict__)


class TestMetaWorkflowHandler:
    def test_attribute_validation_mwf_handler(self):
        with pytest.raises(ValueError) as val_err_info:
            meta_workflow_handler = MetaWorkflowHandler(HANDLER_WITHOUT_UUID)
            assert "validation error" in str(val_err_info)

    @pytest.mark.parametrize(
        "mwf_handler_dict, length_of_mwf_list",
        [
            (HANDLER_WITHOUT_MWF_ARRAY, 0), # sets empty list if attr not present
            (HANDLER_DAG_0, 3),
            (HANDLER_DAG_0_W_DUPLICATES, 3) # gets rid of duplicates
        ]
    )
    def test_set_meta_workflows_list(self, mwf_handler_dict, length_of_mwf_list):
        meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
        assert len(getattr(meta_workflow_handler, "meta_workflows")) == length_of_mwf_list

    @pytest.mark.parametrize(
        "mwf_handler_dict, reordered_mwf_list",
        [
            (HANDLER_WITHOUT_MWF_ARRAY, []),
            (HANDLER_DAG_0, REORDERED_MWF_ARRAY_DAG_0),
            (HANDLER_DAG_0_W_DUPLICATES, REORDERED_MWF_ARRAY_DAG_0),
            (HANDLER_DAG_1, REORDERED_MWF_ARRAY_DAG_1),
            (HANDLER_DAG_1_W_SELF_DEP, REORDERED_MWF_ARRAY_DAG_1)
        ]
    )
    def test_create_ordered_meta_workflows_list(self, mwf_handler_dict, reordered_mwf_list):
        meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
        assert getattr(meta_workflow_handler, "ordered_meta_workflows") == reordered_mwf_list

    @pytest.mark.parametrize(
        "mwf_handler_dict",
        [
            (HANDLER_CYCLIC_0),
            (HANDLER_CYCLIC_1)
        ]
    )
    def test_cycles(self, mwf_handler_dict):
        with pytest.raises(Exception) as exc_info:
            meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
            assert "Cycle in graph: node" in str(exc_info)

    @pytest.mark.parametrize(
        "mwf_handler_dict",
        [
            (HANDLER_WITHOUT_MWF_ARRAY),
            (HANDLER_DAG_0),
            (HANDLER_DAG_0_W_DUPLICATES),
            (HANDLER_DAG_1),
            (HANDLER_DAG_1_W_SELF_DEP)
        ]
    )
    def test_create_ordered_meta_workflow_steps_list(self, mwf_handler_dict):
        meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
        ordered_meta_workflow_steps = getattr(meta_workflow_handler, "ordered_meta_workflow_steps")
        for step in ordered_meta_workflow_steps:
            assert isinstance(step, MetaWorkflowStep)
