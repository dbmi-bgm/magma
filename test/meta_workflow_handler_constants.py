from copy import deepcopy
from magma.magma_constants import *

MWF_HANDLER_NAME = "test_mwf_handler"
MWF_HANDLER_PROJECT = "test_project"
MWF_HANDLER_INSTITUTION = "test_institution"
MWF_HANDLER_UUID = "test_mwf_handler_uuid"

TESTER_UUID = "uuid"

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

#TODO: I never use the prop trace for tests...
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