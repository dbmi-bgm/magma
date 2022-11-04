#!/usr/bin/env python3

#################################################################
#   Libraries
#################################################################
import pytest
# from copy import deepcopy

from magma.topological_sort import *

#################################################################
#   Vars
#################################################################
INPUT_DICT_SINGLE_EMPTY_LIST_ATTR = {"list_empty_0": []}
INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS = {
    "list_empty_0": [],
    "list_empty_1": [],
    "list_empty_2": [],
}

LIST_OF_EMPTY_DICTS = [INPUT_DICT_SINGLE_EMPTY_LIST_ATTR, INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS]


MWF_UUID_0 = "test_mwf_uuid_0"
MWF_UUID_1 = "test_mwf_uuid_1"
MWF_UUID_2 = "test_mwf_uuid_2"
MWF_UUID_3 = "test_mwf_uuid_3"
MWF_UUID_4 = "test_mwf_uuid_4"
MWF_UUID_5 = "test_mwf_uuid_5"
MWF_UUID_6 = "test_mwf_uuid_6"
MWF_UUID_7 = "test_mwf_uuid_7"
MWF_UUID_8 = "test_mwf_uuid_8"
MWF_UUID_9 = "test_mwf_uuid_9"
MWF_NAME_A = "A"
MWF_NAME_B = "B"
MWF_NAME_C = "C"
MWF_NAME_D = "D"
MWF_NAME_E = "E"
MWF_NAME_F = "F"
MWF_NAME_G = "G"
MWF_NAME_H = "H"
MWF_NAME_I = "I"
MWF_NAME_J = "J"

SIMPLE_META_WORKFLOW_DICT_0 = {
    "meta_workflow": MWF_UUID_0,
    "name": MWF_NAME_A
}
SIMPLE_META_WORKFLOW_DICT_1 = {
    "meta_workflow": MWF_UUID_1,
    "name": MWF_NAME_B
}
SIMPLE_META_WORKFLOW_DICT_2 = {
    "meta_workflow": MWF_UUID_2,
    "name": MWF_NAME_C
}
SIMPLE_META_WORKFLOW_DICT_3 = {
    "meta_workflow": MWF_UUID_3,
    "name": MWF_NAME_D
}
SIMPLE_META_WORKFLOW_DICT_4 = {
    "meta_workflow": MWF_UUID_4,
    "name": MWF_NAME_E
}
SIMPLE_META_WORKFLOW_DICT_5 = {
    "meta_workflow": MWF_UUID_5,
    "name": MWF_NAME_F
}
SIMPLE_META_WORKFLOW_DICT_6 = {
    "meta_workflow": MWF_UUID_6,
    "name": MWF_NAME_G
}
SIMPLE_META_WORKFLOW_DICT_7 = {
    "meta_workflow": MWF_UUID_7,
    "name": MWF_NAME_H
}
SIMPLE_META_WORKFLOW_DICT_8 = {
    "meta_workflow": MWF_UUID_8,
    "name": MWF_NAME_I
}
SIMPLE_META_WORKFLOW_DICT_9 = {
    "meta_workflow": MWF_UUID_9,
    "name": MWF_NAME_J
}

# META_WORKFLOWS_ARRAY_SINGLE_ITEM = [SIMPLE_META_WORKFLOW_DICT_0]
# SINGLE_ITEM_META_WORKFLOWS_DICT = {"meta_workflows": META_WORKFLOWS_ARRAY_SINGLE_ITEM}

# META_WORKFLOWS_ARRAY_SEVERAL_ITEMS = [SIMPLE_META_WORKFLOW_DICT_0, SIMPLE_META_WORKFLOW_DICT_1, SIMPLE_META_WORKFLOW_DICT_2]
# SEVERAL_ITEMS_META_WORKFLOWS_DICT = {"meta_workflows": META_WORKFLOWS_ARRAY_SEVERAL_ITEMS}

# META_WORKFLOWS_ARRAY_SEVERAL_ITEMS_W_DUPLICATES = [SIMPLE_META_WORKFLOW_DICT_0, SIMPLE_META_WORKFLOW_DICT_2, SIMPLE_META_WORKFLOW_DICT_1, SIMPLE_META_WORKFLOW_DICT_2]
# SEVERAL_ITEMS_W_DUPLICATES_META_WORKFLOWS_DICT = {"meta_workflows": META_WORKFLOWS_ARRAY_SEVERAL_ITEMS_W_DUPLICATES}

# EMPTY_META_WORKFLOWS_DICT = {"meta_workflows": []}


#################################################################
#   Tests
#################################################################

@pytest.mark.parametrize(
    "list_of_dicts, key_to_check, return_value",
    [
        ([], None, True),
        ([], "key", True),  # kind of weird edge case, but not a biggie (TODO:)
        (
            LIST_OF_EMPTY_DICTS,
            "list_empty_0",
            True,
        ),
        #TODO: come back to this and finish
        # (
        #     [
        #         INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR,
        #         INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS,
        #         INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS,
        #         INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR_W_DUP,
        #         INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP,
        #     ],
        #     "list_simple_0",
        #     True,
        # ),
        # (
        #     [INPUT_DICT_SINGLE_LIST_OF_DICTS],
        #     "list_of_dicts_0",
        #     False,
        # ),
        # (
        #     [
        #         INPUT_DICT_SINGLE_LIST_OF_DICTS,
        #         INPUT_DICT_SEVERAL_LISTS_OF_DICTS,
        #         INPUT_DICT_SINGLE_LIST_OF_DICTS_W_DUP,
        #     ],
        #     "list_of_dicts",
        #     False,
        # ),
    ],
)
def test_check_presence_of_key(
    list_of_dicts, key_to_check, return_value
):
    """
    Test for function checking that all dictionaries in a given list have the
    specified key, no errors raised.
    """
    result = check_presence_of_key(list_of_dicts, key_to_check)
    assert result == return_value


# #TODO: will be generalizing this function later
# class TestSetDependencyListValues:
#     @pytest.mark.parametrize(
#         "list_of_dicts, name_of_step_key, name_of_dependencies_key, orig_lengths, reset_lengths",
#         [
#             ([INPUT_DICT_SINGLE_SIMPLE_ATTR], ["list_empty_0"], [0], [0]),
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
#     def test_set_list_attributes_of_existing_list_attributes(
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

#         result = set_list_attributes(input_object, attributes_to_set)

#         # check length of "reset" attributes_to_set
#         for idx, attribute in enumerate(attributes_to_set):
#             assert len(getattr(input_object, attribute)) == reset_lengths[idx]

#         assert result == None
