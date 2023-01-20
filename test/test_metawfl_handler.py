#!/usr/bin/env python3
 
#################################################################
#   Libraries
#################################################################
import pytest
from copy import deepcopy

from magma.metawfl_handler import MetaWorkflowStep #, MetaWorkflowHandler

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


# class TestMetaWorkflowHandler:
#     def test_attribute_validation_mwf_handler(self):
#         with pytest.raises(ValueError) as val_err_info:
#             meta_workflow_handler = MetaWorkflowHandler(HANDLER_WITHOUT_UUID)
#             assert "validation error" in str(val_err_info)

#     @pytest.mark.parametrize(
#         "mwf_handler_dict, length_of_mwf_list",
#         [
#             (HANDLER_WITHOUT_MWF_ARRAY, 0), # sets empty list if attr not present
#             (HANDLER_DAG_0, 3),
#             (HANDLER_DAG_0_W_DUPLICATES, 3) # gets rid of duplicates
#         ]
#     )
#     def test_set_meta_workflows_list(self, mwf_handler_dict, length_of_mwf_list):
#         meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
#         assert len(getattr(meta_workflow_handler, "meta_workflows")) == length_of_mwf_list

#     @pytest.mark.parametrize(
#         "mwf_handler_dict, reordered_mwf_list",
#         [
#             (HANDLER_WITHOUT_MWF_ARRAY, []),
#             (HANDLER_DAG_0, REORDERED_MWF_ARRAY_DAG_0),
#             (HANDLER_DAG_0_W_DUPLICATES, REORDERED_MWF_ARRAY_DAG_0),
#             (HANDLER_DAG_1, REORDERED_MWF_ARRAY_DAG_1),
#             (HANDLER_DAG_1_W_SELF_DEP, REORDERED_MWF_ARRAY_DAG_1)
#         ]
#     )
#     def test_create_ordered_meta_workflows_list(self, mwf_handler_dict, reordered_mwf_list):
#         meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
#         assert getattr(meta_workflow_handler, "ordered_meta_workflows") == reordered_mwf_list

#     @pytest.mark.parametrize(
#         "mwf_handler_dict",
#         [
#             (HANDLER_CYCLIC_0),
#             (HANDLER_CYCLIC_1)
#         ]
#     )
#     def test_cycles(self, mwf_handler_dict):
#         with pytest.raises(Exception) as exc_info:
#             meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
#             assert "Cycle in graph: node" in str(exc_info)

#     @pytest.mark.parametrize(
#         "mwf_handler_dict",
#         [
#             (HANDLER_WITHOUT_MWF_ARRAY),
#             (HANDLER_DAG_0),
#             (HANDLER_DAG_0_W_DUPLICATES),
#             (HANDLER_DAG_1),
#             (HANDLER_DAG_1_W_SELF_DEP)
#         ]
#     )
#     def test_create_ordered_meta_workflow_steps_list(self, mwf_handler_dict):
#         meta_workflow_handler = MetaWorkflowHandler(mwf_handler_dict)
#         ordered_meta_workflow_steps = getattr(meta_workflow_handler, "ordered_meta_workflow_steps")
#         for step in ordered_meta_workflow_steps:
#             assert isinstance(step, MetaWorkflowStep)







# # dummy class for creating simple objects
# class ClassTester:
#     """
#     Class for creation of simple objects, based on an input dictionary
#     """

#     def __init__(self, input_dict):
#         """
#         Constructor method, initialize object and attributes.

#         :param input_dict: dictionary defining the basic attributes of object to be created
#         :type input_dict: dict
#         """
#         for key in input_dict:
#             setattr(self, key, input_dict[key])

# # TODO: is there a way to functionalize this?
# # input dicts to create ClassTester objects
# INPUT_DICT_SINGLE_SIMPLE_ATTR = {"test_0": 0}
# INPUT_DICT_SINGLE_SIMPLE_ATTR_1 = {"test_1": 0}
# INPUT_DICT_SINGLE_SIMPLE_ATTR_2 = {"test_2": 0}
# INPUT_DICT_SEVERAL_SIMPLE_ATTRS = {"test_0": 0, "test_1": 1, "test_2": 2}
# INPUT_DICT_SINGLE_EMPTY_LIST_ATTR = {"list_empty_0": []}
# INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS = {
#     "list_empty_0": [],
#     "list_empty_1": [],
#     "list_empty_2": [],
# }
# INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR = {"list_simple_0": [1, 2, 3]}
# INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS = {
#     "list_simple_0": [1, 2, 3],
#     "list_simple_1": ["a", "b", "c"],
# }
# INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR_W_DUP = {"list_simple_0": [1, 2, 3, 4, 3]}
# INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP = {
#     "list_simple_0": [1, "a", 3, 3],
#     "list_simple_1": ["a", "b", "c"],
#     "list_simple_2": ["c", 1, "c"],
# }

# LIST_OF_EMPTY_DICTS = [INPUT_DICT_SINGLE_EMPTY_LIST_ATTR, INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS]
# LIST_OF_SIMPLE_ATTR_DICTS = [
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_1,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
#     ]
# LIST_OF_SIMPLE_ATTR_DICTS_REORDERED = [
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_1,
#     ]
# LIST_OF_SIMPLE_ATTR_DICTS_W_DUP = [
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_1,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#     ]
# LIST_OF_SIMPLE_ATTR_DICTS_W_DUP_2 = [
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_1,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR,
#     ]
# LIST_OF_SIMPLE_ATTR_DICTS_W_DUP_3 = [
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
#         INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
#     ]

# INPUT_DICT_SINGLE_LIST_OF_DICTS = {
#     "list_of_dicts": LIST_OF_SIMPLE_ATTR_DICTS
# }
# INPUT_DICT_SEVERAL_LISTS_OF_DICTS = {
#     "list_of_dicts_0": LIST_OF_SIMPLE_ATTR_DICTS,
#     "list_of_dicts_1": LIST_OF_SIMPLE_ATTR_DICTS_REORDERED,
# }
# INPUT_DICT_SINGLE_LIST_OF_DICTS_W_DUP = {
#     "list_of_dicts": LIST_OF_SIMPLE_ATTR_DICTS_W_DUP
# }
# INPUT_DICT_SEVERAL_LISTS_OF_DICTS_W_DUP = {
#     "list_of_dicts_0": LIST_OF_SIMPLE_ATTR_DICTS,
#     "list_of_dicts_1": LIST_OF_SIMPLE_ATTR_DICTS_REORDERED,
#     "list_of_dicts_2": LIST_OF_SIMPLE_ATTR_DICTS_W_DUP_2,
#     "list_of_dicts_3": LIST_OF_SIMPLE_ATTR_DICTS_W_DUP_3
# }


# # ClassTester objects
# CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR = ClassTester(INPUT_DICT_SINGLE_SIMPLE_ATTR)
# CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS = ClassTester(INPUT_DICT_SEVERAL_SIMPLE_ATTRS)
# CLASSTESTER_OBJ_SINGLE_EMPTY_LIST_ATTR = ClassTester(INPUT_DICT_SINGLE_EMPTY_LIST_ATTR)
# CLASSTESTER_OBJ_SEVERAL_EMPTY_LIST_ATTRS = ClassTester(INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS)
# CLASSTESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR = ClassTester(INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR)
# CLASSTESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS = ClassTester(INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS)
# CLASSTESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR_W_DUP = ClassTester(
#     INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR_W_DUP
# )
# CLASSTESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP = ClassTester(
#     INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP
# )
# CLASSTESTER_OBJ_SINGLE_LIST_OF_DICTS = ClassTester(INPUT_DICT_SINGLE_LIST_OF_DICTS)
# CLASSTESTER_OBJ_SEVERAL_LISTS_OF_DICTS = ClassTester(INPUT_DICT_SEVERAL_LISTS_OF_DICTS)
# CLASSTESTER_OBJ_SINGLE_LIST_OF_DICTS_W_DUP = ClassTester(INPUT_DICT_SINGLE_LIST_OF_DICTS_W_DUP)
# CLASSTESTER_OBJ_SEVERAL_LISTS_OF_DICTS_W_DUP = ClassTester(
#     INPUT_DICT_SEVERAL_LISTS_OF_DICTS_W_DUP
# )

# class TestSetUniqueListAttributes:
#     @pytest.mark.parametrize(
#         "input_object, attributes_to_set",
#         [
#             (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, None),
#             (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, []),
#             (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test_0"]),
#             (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1"]),
#             (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1", "test_2"]),
#             (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2"]),
#             (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2", "test_1"]),
#         ],
#     )
#     def test_set_unique_list_attributes_of_existing_nonlist_attributes(
#         self, input_object, attributes_to_set
#     ):
#         """
#         Test for function that gets rid of duplicates within object attributes that are lists,
#         or sets attributes to empty list if not present within the object.
#         Cases where the attributes to set are existent and are NOT lists, no action done.
#         """
#         original_object = deepcopy(input_object)
#         result = set_unique_list_attributes(input_object, attributes_to_set)
#         assert result is None
#         assert vars(input_object) == vars(original_object)  # no attributes changed
#         #TODO: double check the above "vars" functionality

#     @pytest.mark.parametrize(
#         "input_object, attributes_to_set, orig_lengths, reset_lengths",
#         [
#             (CLASSTESTER_OBJ_SINGLE_EMPTY_LIST_ATTR, ["list_empty_0"], [0], [0]),
#             (
#                 CLASSTESTER_OBJ_SEVERAL_EMPTY_LIST_ATTRS,
#                 ["list_empty_0", "list_empty_1", "list_empty_2"],
#                 [0, 0, 0],
#                 [0, 0, 0],
#             ),
#             (CLASSTESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR, ["list_simple_0"], [3], [3]),
#             (
#                 CLASSTESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS,
#                 ["list_simple_0", "list_simple_1"],
#                 [3, 3],
#                 [3, 3],
#             ),
#             (CLASSTESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR_W_DUP, ["list_simple_0"], [5], [4]),
#             (
#                 CLASSTESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP,
#                 ["list_simple_0", "list_simple_2", "list_simple_1"],
#                 [4, 3, 3],
#                 [3, 2, 3],
#             ),
#             (CLASSTESTER_OBJ_SINGLE_LIST_OF_DICTS, ["list_of_dicts"], [3], [3]),
#             (
#                 CLASSTESTER_OBJ_SEVERAL_LISTS_OF_DICTS,
#                 ["list_of_dicts_1", "list_of_dicts_0"],
#                 [3, 3],
#                 [3, 3],
#             ),
#             (CLASSTESTER_OBJ_SINGLE_LIST_OF_DICTS_W_DUP, ["list_of_dicts"], [6], [3]),
#             (
#                 CLASSTESTER_OBJ_SEVERAL_LISTS_OF_DICTS_W_DUP,
#                 [
#                     "list_of_dicts_1",
#                     "list_of_dicts_0",
#                     "list_of_dicts_2",
#                     "list_of_dicts_3",
#                 ],
#                 [3, 3, 5, 3],
#                 [3, 3, 2, 1],
#             ),
#         ],
#     )
#     def test_set_unique_list_attributes_of_existing_list_attributes(
#         self, input_object, attributes_to_set, orig_lengths, reset_lengths
#     ):
#         """
#         Test for function that gets rid of duplicates within object attributes that are lists,
#         or sets attributes to empty list if not present within the object.
#         Cases where the attributes to set are existent and are lists.
#         """
#         # import pdb; pdb.set_trace()
#         # check original length of attributes_to_set
#         for idx, attribute in enumerate(attributes_to_set):
#             assert len(getattr(input_object, attribute)) == orig_lengths[idx]

#         result = set_unique_list_attributes(input_object, attributes_to_set)

#         # check length of "reset" attributes_to_set
#         for idx, attribute in enumerate(attributes_to_set):
#             assert len(getattr(input_object, attribute)) == reset_lengths[idx]

#         assert result is None

#     @pytest.mark.parametrize(
#         "input_object, attributes_to_set, num_added_attributes",
#         [
#             (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test_0"], 0),
#             (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test_1"], 1),
#             (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_2", "test_3"], 1),
#             (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_5", "test_0", "test_4"], 2),
#             (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2"], 0),
#             (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2", "test_1"], 0),
#         ],
#     )
#     def test_set_unique_list_attributes_of_nonexistent_attributes(
#         self, input_object, attributes_to_set, num_added_attributes
#     ):
#         """
#         Test for function that gets rid of duplicates within object attributes that are lists,
#         or sets attributes to empty list if not present within the object.
#         Cases where the attributes to set are nonexistent, so they are added with the value [].
#         """
#         # TODO: this changes the objects permanently since I'm setting attrs
#         # but I don't think this will affect further testing (specifically, fourth example)

#         original_attributes_set = set(dir(input_object))
#         num_original_attributes = len(original_attributes_set)

#         result = set_unique_list_attributes(input_object, attributes_to_set)
#         assert result is None

#         reset_attributes_set = set(dir(input_object))
#         num_reset_attributes = len(reset_attributes_set)

#         assert num_added_attributes == (num_reset_attributes - num_original_attributes)

#         added_attributes = reset_attributes_set.difference(original_attributes_set)
#         for attribute in added_attributes:
#             assert attribute in attributes_to_set
#             assert getattr(input_object, attribute) == []

#     # TODO: add a test for mixed cases? (nonexistent + lists + empties, etc.)