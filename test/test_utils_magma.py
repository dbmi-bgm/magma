#!/usr/bin/env python3

#################################################################
#   Libraries
#################################################################
import pytest
from copy import deepcopy

from magma.utils import *

#################################################################
#   Vars
#################################################################

# dummy class for creating simple objects
class ClassTester:
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

# TODO: is there a way to functionalize this?
# input dicts to create ClassTester objects
INPUT_DICT_SINGLE_SIMPLE_ATTR = {"test_0": 0}
INPUT_DICT_SINGLE_SIMPLE_ATTR_1 = {"test_1": 0}
INPUT_DICT_SINGLE_SIMPLE_ATTR_2 = {"test_2": 0}
INPUT_DICT_SEVERAL_SIMPLE_ATTRS = {"test_0": 0, "test_1": 1, "test_2": 2}
INPUT_DICT_SINGLE_EMPTY_LIST_ATTR = {"list_empty_0": []}
INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS = {
    "list_empty_0": [],
    "list_empty_1": [],
    "list_empty_2": [],
}
INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR = {"list_simple_0": [1, 2, 3]}
INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS = {
    "list_simple_0": [1, 2, 3],
    "list_simple_1": ["a", "b", "c"],
}
INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR_W_DUP = {"list_simple_0": [1, 2, 3, 4, 3]}
INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP = {
    "list_simple_0": [1, "a", 3, 3],
    "list_simple_1": ["a", "b", "c"],
    "list_simple_2": ["c", 1, "c"],
}

LIST_OF_EMPTY_DICTS = [INPUT_DICT_SINGLE_EMPTY_LIST_ATTR, INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS]
LIST_OF_SIMPLE_ATTR_DICTS = [
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
        INPUT_DICT_SINGLE_SIMPLE_ATTR_1,
        INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
    ]
LIST_OF_SIMPLE_ATTR_DICTS_REORDERED = [
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
        INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
        INPUT_DICT_SINGLE_SIMPLE_ATTR_1,
    ]
LIST_OF_SIMPLE_ATTR_DICTS_W_DUP = [
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
        INPUT_DICT_SINGLE_SIMPLE_ATTR_1,
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
        INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
    ]
LIST_OF_SIMPLE_ATTR_DICTS_W_DUP_2 = [
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
        INPUT_DICT_SINGLE_SIMPLE_ATTR_1,
        INPUT_DICT_SINGLE_SIMPLE_ATTR,
    ]
LIST_OF_SIMPLE_ATTR_DICTS_W_DUP_3 = [
        INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
        INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
        INPUT_DICT_SINGLE_SIMPLE_ATTR_2,
    ]

INPUT_DICT_SINGLE_LIST_OF_DICTS = {
    "list_of_dicts": LIST_OF_SIMPLE_ATTR_DICTS
}
INPUT_DICT_SEVERAL_LISTS_OF_DICTS = {
    "list_of_dicts_0": LIST_OF_SIMPLE_ATTR_DICTS,
    "list_of_dicts_1": LIST_OF_SIMPLE_ATTR_DICTS_REORDERED,
}
INPUT_DICT_SINGLE_LIST_OF_DICTS_W_DUP = {
    "list_of_dicts": LIST_OF_SIMPLE_ATTR_DICTS_W_DUP
}
INPUT_DICT_SEVERAL_LISTS_OF_DICTS_W_DUP = {
    "list_of_dicts_0": LIST_OF_SIMPLE_ATTR_DICTS,
    "list_of_dicts_1": LIST_OF_SIMPLE_ATTR_DICTS_REORDERED,
    "list_of_dicts_2": LIST_OF_SIMPLE_ATTR_DICTS_W_DUP_2,
    "list_of_dicts_3": LIST_OF_SIMPLE_ATTR_DICTS_W_DUP_3
}


# ClassTester objects
CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR = ClassTester(INPUT_DICT_SINGLE_SIMPLE_ATTR)
CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS = ClassTester(INPUT_DICT_SEVERAL_SIMPLE_ATTRS)
CLASSTESTER_OBJ_SINGLE_EMPTY_LIST_ATTR = ClassTester(INPUT_DICT_SINGLE_EMPTY_LIST_ATTR)
CLASSTESTER_OBJ_SEVERAL_EMPTY_LIST_ATTRS = ClassTester(INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS)
CLASSTESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR = ClassTester(INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR)
CLASSTESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS = ClassTester(INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS)
CLASSTESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR_W_DUP = ClassTester(
    INPUT_DICT_SINGLE_SIMPLE_LIST_ATTR_W_DUP
)
CLASSTESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP = ClassTester(
    INPUT_DICT_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP
)
CLASSTESTER_OBJ_SINGLE_LIST_OF_DICTS = ClassTester(INPUT_DICT_SINGLE_LIST_OF_DICTS)
CLASSTESTER_OBJ_SEVERAL_LISTS_OF_DICTS = ClassTester(INPUT_DICT_SEVERAL_LISTS_OF_DICTS)
CLASSTESTER_OBJ_SINGLE_LIST_OF_DICTS_W_DUP = ClassTester(INPUT_DICT_SINGLE_LIST_OF_DICTS_W_DUP)
CLASSTESTER_OBJ_SEVERAL_LISTS_OF_DICTS_W_DUP = ClassTester(
    INPUT_DICT_SEVERAL_LISTS_OF_DICTS_W_DUP
)

#################################################################
#   Tests
#################################################################

@pytest.mark.parametrize(
    "variable, intended_type, return_value",
    [
        (2, int, True),
        (-2, int, True),
        (float("inf"), float, True),
        (complex(1, 1.0), complex, True),
        (True, bool, True),
        (False, bool, True),
        (None, type(None), True),
        (None, object, True),
        ("a", str, True),
        ("a", object, True),
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
        (float("inf"), int, False),
        (complex(1, 1.0), float, False),
        (True, str, False),
        (None, bool, False),
        ("a", int, False),
        ("test", list, False),
        ((1, 2), set, False),
        (set(), tuple, False),
        ([1, "test"], dict, False),
        ({"hi": 1}, list, False),
    ],
)
def test_check_variable_type(variable, intended_type, return_value):
    """
    Test for function checking if a variable is of a specified type.
    """
    result = check_variable_type(variable, intended_type)
    assert result == return_value


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
        ([1, "test"], int, False),
    ],
)
def test_check_list_elements_type_no_errors(
    list_to_check, intended_type, return_value
):
    """
    Test for function checking that all elements of a list are of a specified type,
    no errors raised.
    """
    result = check_list_elements_type(list_to_check, intended_type)
    assert result == return_value


class TestCheckPresenceOfAttributes:
    @pytest.mark.parametrize(
        "input_object, attributes_to_check",
        [
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, None),
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, []),
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test_0"]),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1"]),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1", "test_2"]),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2"]),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2", "test_1"]),
        ],
    )
    def test_check_presence_of_attributes_no_errors(
        self, input_object, attributes_to_check
    ):
        """
        Test for function checking that specified attributes are part of a given object,
        no errors raised.
        """
        result = check_presence_of_attributes(input_object, attributes_to_check)
        assert result == None

    @pytest.mark.parametrize(
        "input_object, attributes_to_check",
        [
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, ["not_present"]),
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, ["not_present_0", "not_present_1"]),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1", "not_present"]),
            (
                CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS,
                ["test_0", "not_present", "test_1", "test_2"],
            ),
            (
                CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS,
                ["test_0", "not_present_0", "test_2", "not_present_1"],
            ),
            (
                CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS,
                ["not_present", "test_0", "test_2", "test_1"],
            ),
        ],
    )
    def test_check_presence_of_attributes_value_errors(
        self, input_object, attributes_to_check
    ):
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
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, None),
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, []),
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test_0"]),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1"]),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_1", "test_2"]),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2"]),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2", "test_1"]),
        ],
    )
    def test_set_list_attributes_of_existing_nonlist_attributes(
        self, input_object, attributes_to_set
    ):
        """
        Test for function that gets rid of duplicates within object attributes that are lists,
        or sets attributes to empty list if not present within the object.
        Cases where the attributes to set are existent and are NOT lists, no action done.
        """
        original_object = deepcopy(input_object)
        result = set_list_attributes(input_object, attributes_to_set)
        assert result == None
        assert vars(input_object) == vars(original_object)  # no attributes changed
        #TODO: double check the above "vars" functionality

    @pytest.mark.parametrize(
        "input_object, attributes_to_set, orig_lengths, reset_lengths",
        [
            (CLASSTESTER_OBJ_SINGLE_EMPTY_LIST_ATTR, ["list_empty_0"], [0], [0]),
            (
                CLASSTESTER_OBJ_SEVERAL_EMPTY_LIST_ATTRS,
                ["list_empty_0", "list_empty_1", "list_empty_2"],
                [0, 0, 0],
                [0, 0, 0],
            ),
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR, ["list_simple_0"], [3], [3]),
            (
                CLASSTESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS,
                ["list_simple_0", "list_simple_1"],
                [3, 3],
                [3, 3],
            ),
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_LIST_ATTR_W_DUP, ["list_simple_0"], [5], [4]),
            (
                CLASSTESTER_OBJ_SEVERAL_SIMPLE_LIST_ATTRS_W_DUP,
                ["list_simple_0", "list_simple_2", "list_simple_1"],
                [4, 3, 3],
                [3, 2, 3],
            ),
            (CLASSTESTER_OBJ_SINGLE_LIST_OF_DICTS, ["list_of_dicts"], [3], [3]),
            (
                CLASSTESTER_OBJ_SEVERAL_LISTS_OF_DICTS,
                ["list_of_dicts_1", "list_of_dicts_0"],
                [3, 3],
                [3, 3],
            ),
            (CLASSTESTER_OBJ_SINGLE_LIST_OF_DICTS_W_DUP, ["list_of_dicts"], [6], [3]),
            (
                CLASSTESTER_OBJ_SEVERAL_LISTS_OF_DICTS_W_DUP,
                [
                    "list_of_dicts_1",
                    "list_of_dicts_0",
                    "list_of_dicts_2",
                    "list_of_dicts_3",
                ],
                [3, 3, 5, 3],
                [3, 3, 2, 1],
            ),
        ],
    )
    def test_set_list_attributes_of_existing_list_attributes(
        self, input_object, attributes_to_set, orig_lengths, reset_lengths
    ):
        """
        Test for function that gets rid of duplicates within object attributes that are lists,
        or sets attributes to empty list if not present within the object.
        Cases where the attributes to set are existent and are lists.
        """
        # import pdb; pdb.set_trace()
        # check original length of attributes_to_set
        for idx, attribute in enumerate(attributes_to_set):
            assert len(getattr(input_object, attribute)) == orig_lengths[idx]

        result = set_list_attributes(input_object, attributes_to_set)

        # check length of "reset" attributes_to_set
        for idx, attribute in enumerate(attributes_to_set):
            assert len(getattr(input_object, attribute)) == reset_lengths[idx]

        assert result == None

    @pytest.mark.parametrize(
        "input_object, attributes_to_set, num_added_attributes",
        [
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test_0"], 0),
            (CLASSTESTER_OBJ_SINGLE_SIMPLE_ATTR, ["test_1"], 1),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_2", "test_3"], 1),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_5", "test_0", "test_4"], 2),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2"], 0),
            (CLASSTESTER_OBJ_SEVERAL_SIMPLE_ATTRS, ["test_0", "test_2", "test_1"], 0),
        ],
    )
    def test_set_list_attributes_of_nonexistent_attributes(
        self, input_object, attributes_to_set, num_added_attributes
    ):
        """
        Test for function that gets rid of duplicates within object attributes that are lists,
        or sets attributes to empty list if not present within the object.
        Cases where the attributes to set are nonexistent, so they are added with the value [].
        """
        # TODO: this changes the objects permanently since I'm setting attrs
        # but I don't think this will affect further testing (specifically, fourth example)

        original_attributes_set = set(dir(input_object))
        num_original_attributes = len(original_attributes_set)

        result = set_list_attributes(input_object, attributes_to_set)
        assert result == None

        reset_attributes_set = set(dir(input_object))
        num_reset_attributes = len(reset_attributes_set)

        assert num_added_attributes == (num_reset_attributes - num_original_attributes)

        added_attributes = reset_attributes_set.difference(original_attributes_set)
        for attribute in added_attributes:
            assert attribute in attributes_to_set
            assert getattr(input_object, attribute) == []

    # TODO: add a test for mixed cases? (nonexistent + lists + empties, etc.)