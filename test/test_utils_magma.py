#################################################################
#   Libraries
#################################################################
import pytest
from copy import deepcopy

from magma.utils import *
# from magma.metawfl_handler import MetaWorkflowStep, MetaWorkflowHandler

#################################################################
#   Vars
#################################################################

# dummy class for creating simple objects
class Tester:
    """
    Class for creation of simple objects, based on an input dictionary
    """
    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: dictionary defining the basic attributes of object to be created
        :type input_dict: dict
        """
        for key in input_dict:
            setattr(self, key, input_dict[key])

# Stop pytest from collecting class Tester as test (prevent warning)
Tester.__test__ = False

#TODO: is there a way to functionalize this?
# input dicts to create Tester objects
INPUT_DICT_SINGLE_SIMPLE_ATTR = {"test_0": 0}
INPUT_DICT_SEVERAL_SIMPLE_ATTRS = {"test_0": 0, "test_1": 1, "test_2": 2}
INPUT_DICT_SINGLE_EMPTY_LIST_ATTR = {"list_empty_0": []}
INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS = {"list_empty_0": [], "list_empty_1": [], "list_empty_2": []}
INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR = {"list_simple_0": [1, 2, 3]}
INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS = {"list_simple_0": [1, 2, 3], "list_simple_1": ["a", "b", "c"]}
INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR_W_DUP = {"list_simple_0": [1, 2, 3, 4, 3]}
INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP = {"list_simple_0": [1, "a", 3, 3], "list_simple_1": ["a", "b", "c"], "list_simple_2": ["c", 1, "c"]}

# Tester objects
TESTER_OBJ_SINGLE_SIMPLE_ATTR = Tester(INPUT_DICT_SINGLE_SIMPLE_ATTR)
TESTER_OBJ_SEVERAL_SIMPLE_ATTRS = Tester(INPUT_DICT_SEVERAL_SIMPLE_ATTRS)
TESTER_OBJ_SINGLE_EMPTY_LIST_ATTR = Tester(INPUT_DICT_SINGLE_EMPTY_LIST_ATTR)
TESTER_OBJ_SEVERAL_EMPTY_LIST_ATTRS = Tester(INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS)
TESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR = Tester(INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR)
TESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS = Tester(INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS)
TESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR_W_DUP = Tester(INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR_W_DUP)
TESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP = Tester(INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP)

# TITLE = "Test MetaWorkflow Handler"
# NAME = "test_metawf_handler"
# VERSION = "v1"
# DESCRIPTION = "Test metaworkflow handler"
# PROJECT = "test_project"
# INSTITUTION = "test_institution"
# MWFH_UUID_0 =  "test_mwfh_uuid_0"
# ALIASES = ["cgap:test_metawf_handler"]
# MWF_UUID_0 = "test_mwf_uuid_0"
# MWF_UUID_1 = "test_mwf_uuid_1"
# MWF_UUID_2 = "test_mwf_uuid_2"
# MWF_UUID_3 = "test_mwf_uuid_3"
# MWF_UUID_4 = "test_mwf_uuid_4"
# MWF_UUID_5 = "test_mwf_uuid_5"
# MWF_NAME_A = "A"
# MWF_NAME_B = "B"
# MWF_NAME_C = "C"
# MWF_NAME_D = "D"
# MWF_NAME_E = "E"
# MWF_NAME_F = "F"

# ITEMS_FOR_CREATION_PROPERTY_TRACE_0 = "sample_processing.samples"
# ITEMS_FOR_CREATION_UUID_0 = "test_sample_uuid_0"
# ITEMS_FOR_CREATION_UUID_1 = "test_sample_uuid_1"



# SIMPLE_META_WORKFLOW_DICT_0 = {
#     "meta_workflow": MWF_UUID_0,
#     "name": MWF_NAME_A
# }
# SIMPLE_META_WORKFLOW_DICT_1 = {
#     "meta_workflow": MWF_UUID_1,
#     "name": MWF_NAME_B
# }
# SIMPLE_META_WORKFLOW_DICT_2 = {
#     "meta_workflow": MWF_UUID_2,
#     "name": MWF_NAME_C
# }

# META_WORKFLOWS_ARRAY_SINGLE_ITEM = [SIMPLE_META_WORKFLOW_DICT_0]
# SINGLE_ITEM_META_WORKFLOWS_DICT = {"meta_workflows": META_WORKFLOWS_ARRAY_SINGLE_ITEM}

# META_WORKFLOWS_ARRAY_SEVERAL_ITEMS = [SIMPLE_META_WORKFLOW_DICT_0, SIMPLE_META_WORKFLOW_DICT_1, SIMPLE_META_WORKFLOW_DICT_2]
# SEVERAL_ITEMS_META_WORKFLOWS_DICT = {"meta_workflows": META_WORKFLOWS_ARRAY_SEVERAL_ITEMS}

# META_WORKFLOWS_ARRAY_SEVERAL_ITEMS_W_DUPLICATES = [SIMPLE_META_WORKFLOW_DICT_0, SIMPLE_META_WORKFLOW_DICT_2, SIMPLE_META_WORKFLOW_DICT_1, SIMPLE_META_WORKFLOW_DICT_2]
# SEVERAL_ITEMS_W_DUPLICATES_META_WORKFLOWS_DICT = {"meta_workflows": META_WORKFLOWS_ARRAY_SEVERAL_ITEMS_W_DUPLICATES}

# EMPTY_META_WORKFLOWS_DICT = {"meta_workflows": []}

# SIMPLE_MWFH_DICT = {
#         "title": TITLE,
#         "name": NAME,
#         "version": VERSION,
#         "description": DESCRIPTION,
#         "project": PROJECT,
#         "institution": INSTITUTION,
#         "uuid": MWFH_UUID_0,
#         "aliases": ALIASES
#     }

# SIMPLE_MWFH_DICT_WITH_EMPTY_META_WORKFLOWS_LIST = deepcopy(SIMPLE_MWFH_DICT)
# SIMPLE_MWFH_DICT_WITH_EMPTY_META_WORKFLOWS_LIST.update(EMPTY_META_WORKFLOWS_DICT)

# SIMPLE_MWFH_DICT_WITH_SINGLE_ITEM_META_WORKFLOWS_LIST = deepcopy(SIMPLE_MWFH_DICT)
# SIMPLE_MWFH_DICT_WITH_SINGLE_ITEM_META_WORKFLOWS_LIST.update(SINGLE_ITEM_META_WORKFLOWS_DICT)

# SIMPLE_MWFH_DICT_WITH_SEVERAL_ITEMS_META_WORKFLOWS_LIST = deepcopy(SIMPLE_MWFH_DICT)
# SIMPLE_MWFH_DICT_WITH_SEVERAL_ITEMS_META_WORKFLOWS_LIST.update(SEVERAL_ITEMS_META_WORKFLOWS_DICT)

# SIMPLE_MWFH_DICT_WITH_SEVERAL_ITEMS_W_DUPLICATES_META_WORKFLOWS_LIST = deepcopy(SIMPLE_MWFH_DICT)
# SIMPLE_MWFH_DICT_WITH_SEVERAL_ITEMS_W_DUPLICATES_META_WORKFLOWS_LIST.update(SEVERAL_ITEMS_W_DUPLICATES_META_WORKFLOWS_DICT)

# # import pdb; pdb.set_trace()
# SIMPLE_MWFH_OBJECT_WITH_EMPTY_META_WORKFLOWS_LIST = MetaWorkflowHandler(SIMPLE_MWFH_DICT_WITH_EMPTY_META_WORKFLOWS_LIST)
# SIMPLE_MWFH_OBJECT_WITH_SINGLE_ITEM_META_WORKFLOWS_LIST = MetaWorkflowHandler(SIMPLE_MWFH_DICT_WITH_SINGLE_ITEM_META_WORKFLOWS_LIST)
# SIMPLE_MWFH_OBJECT_WITH_SEVERAL_ITEMS_META_WORKFLOWS_LIST = MetaWorkflowHandler(SIMPLE_MWFH_DICT_WITH_SEVERAL_ITEMS_META_WORKFLOWS_LIST)
# SIMPLE_MWFH_OBJECT_WITH_SEVERAL_ITEMS_W_DUPLICATES_META_WORKFLOWS_LIST = MetaWorkflowHandler(SIMPLE_MWFH_DICT_WITH_SEVERAL_ITEMS_W_DUPLICATES_META_WORKFLOWS_LIST)

#################################################################
#   Tests
#################################################################

@pytest.mark.parametrize(
    "variable, intended_type, return_value",
    [
        (2, int, True),
        (-2, int, True),
        (float('inf'), float, True),
        (complex(1, 1.0), complex, True),
        (True, bool, True),
        (False, bool, True),
        (None, type(None), True),
        (None, object, True),
        ('a', str, True),
        ('a', object, True),
        ("test", str, True),
        ("test", object, True),
        ((1, 2), tuple, True),
        ((1, 2), object, True),
        ([], list, True),
        ([], object, True),
        (set(), set, True),
        (set(), object, True),
        ([1, "test"], list, True),
        ([1, "test"], object, True),
        ({}, dict, True),
        ({}, object, True),
        ({"hi": 1}, dict, True),
        ({"hi": 1}, object, True),
        (2, list, False),
        (float('inf'), int, False),
        (complex(1, 1.0), float, False),
        (True, str, False),
        (None, bool, False),
        ('a', int, False),
        ("test", list, False),
        ((1, 2), set, False),
        (set(), tuple, False),
        ([1, "test"], dict, False),
        ({"hi": 1}, list, False)
    ]
)
def test_check_list_elements_type(variable, intended_type, return_value):
    """
    Test for function checking if a variable is of a specified type.
    """
    result = check_variable_type(variable, intended_type)
    assert result == return_value

   

class TestCheckListElementsType:
    @pytest.mark.parametrize(
        "list_to_check, intended_type, return_value",
        [
            ([], str, True),
            ([], int, True),
            ([], list, True),
            ([], object, True),
            (["id"], str, True),
            (["1", "test", "2"], str, True),
            ([1, 2, 3, 4], int, True),
            ([[1], [2], ["test", "2"], []], list, True),
            ([["1", "2", "3", "4", "5"], ["6"]], str, False),
            ([["1", "2", "3", "4", "5"], "6"], list, False),
            ([None, "test"], str, False),
            ([1, "test"], int, False)
        ]
    )
    def test_check_list_elements_type_no_errors(self, list_to_check, intended_type, return_value):
        """
        Test for function checking that all elements of a list are of a specified type,
        no errors raised.
        """
        result = check_list_elements_type(list_to_check, intended_type)
        assert result == return_value


    @pytest.mark.parametrize(
        "list_to_check, intended_type",
        [
            (1, str),
            ("test", list),
            (None, str)
        ]
    )
    def test_check_list_elements_type_typeerror(self, list_to_check, intended_type):
        """
        Test for function checking if all elements of a list are strings,
        TypeError raised when list elements are not of the intended type.
        """
        with pytest.raises(TypeError) as type_err_info:
            check_list_elements_type(list_to_check, intended_type)
        assert str(type_err_info.value) == "list_to_check argument must be of type {0}".format(str(list))

class TestCheckPresenceOfAttributes:
    @pytest.mark.parametrize(
        "input_object, attributes_to_check",
        [
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, None),
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, []),
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test_0"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1", "test_2"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2", "test_1"])
        ]
    )
    def test_check_presence_of_attributes_no_errors(self, input_object, attributes_to_check):
        """
        Test for function checking that specified attributes are part of a given object,
        no errors raised.
        """
        result = check_presence_of_attributes(input_object, attributes_to_check)
        assert result == None

    @pytest.mark.parametrize(
        "input_object, attributes_to_check",
        [
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, 1),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, "incorrect_input_type"),
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test", 4])
        ]
    )
    def test_check_presence_of_attributes_type_errors(self, input_object, attributes_to_check):
        """
        Test for function checking that specified attributes are part of a given object,
        TypeError raised because of incorrect argument type.
        """
        with pytest.raises(TypeError):
            check_presence_of_attributes(input_object, attributes_to_check)

    @pytest.mark.parametrize(
        "input_object, attributes_to_check",
        [
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, ["not_present"]),
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, ["not_present_0", "not_present_1"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1", "not_present"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "not_present", "test_1", "test_2"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "not_present_0", "test_2", "not_present_1"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["not_present", "test_0", "test_2", "test_1"])
        ]
    )
    def test_check_presence_of_attributes_value_errors(self, input_object, attributes_to_check):
        """
        Test for function checking that specified attributes are part of a given object,
        ValueError raised.
        """
        with pytest.raises(ValueError) as value_err_info:
            check_presence_of_attributes(input_object, attributes_to_check)
        assert "Object validation error" in str(value_err_info.value)

class TestSetListAttributes:
    @pytest.mark.parametrize(
        "input_object, attributes_to_set",
        [
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, None),
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, []),
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test_0"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1", "test_2"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2"]),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2", "test_1"])
        ]
    )
    def test_set_list_attributes_of_existing_nonlist_attributes(self, input_object, attributes_to_set):
        """
        Test for function that gets rid of duplicates within object attributes that are lists,
        or sets attributes to empty list if not present within the object.
        Cases where the attributes to set are existent and are NOT lists, no action done.
        """
        original_object = deepcopy(input_object)
        result = set_list_attributes(input_object, attributes_to_set)
        assert result == None
        assert vars(input_object) == vars(original_object) # no attributes changed

    @pytest.mark.parametrize(
        "input_object, attributes_to_set",
        [
            (TESTER_OBJ_SINGLE_SIMPLE_ATTR, 1),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, "incorrect_input_type"),
            (TESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["hi", 2])
        ]
    )
    def test_set_list_attributes_type_errors(self, input_object, attributes_to_set):
        """
        Test for function setting list attributes,
        TypeError raised because of incorrect argument type.
        """
        with pytest.raises(TypeError):
            set_list_attributes(input_object, attributes_to_set)

    @pytest.mark.parametrize(
        "input_object, attributes_to_set, orig_lengths, reset_lengths",
        [
            (TESTER_OBJ_SINGLE_EMPTY_LIST_ATTR, ["list_empty_0"], [0], [0]),
            (TESTER_OBJ_SEVERAL_EMPTY_LIST_ATTRS, ["list_empty_0", "list_empty_1", "list_empty_2"], [0, 0, 0], [0, 0, 0]),
            (TESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR, ["list_simple_0"], [3], [3]),
            (TESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS, ["list_simple_0", "list_simple_1"], [3, 3], [3, 3]),
            (TESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR_W_DUP, ["list_simple_0"], [5], [4]),
            (TESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP, ["list_simple_0", "list_simple_2", "list_simple_1"], [4, 3, 3], [3, 2, 3])
        ]
    )
    def test_set_list_attributes_of_existing_list_attributes(self, input_object, attributes_to_set, orig_lengths, reset_lengths):
        """
        Test for function that gets rid of duplicates within object attributes that are lists,
        or sets attributes to empty list if not present within the object.
        Cases where the attributes to set are existent and are lists.
        """
        # import pdb; pdb.set_trace()
        # check original length of attributes_to_set
        for ind, attribute in enumerate(attributes_to_set):
            assert len(getattr(input_object, attribute)) == orig_lengths[ind]

        result = set_list_attributes(input_object, attributes_to_set)

        # check length of "reset" attributes_to_set
        for idx, attribute in enumerate(attributes_to_set):
            assert len(getattr(input_object, attribute)) == reset_lengths[idx]

        assert result == None


# #     # then do non lists
# #     # then do nonexistent
# #     # then do mixed