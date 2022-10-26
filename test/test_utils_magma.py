#################################################################
#   Libraries
#################################################################
import pytest

from magma.utils import check_variable_type, check_list_elements_type

#################################################################
#   Vars
#################################################################

#################################################################
#   Tests
#################################################################

class TestCheckVariableType:
    @pytest.mark.parametrize(
        "variable, intended_type",
        [
            (2, int),
            (-2, int),
            (float('inf'), float),
            (complex(1, 1.0), complex),
            (True, bool),
            (False, bool),
            (None, type(None)),
            (None, object),
            ('a', str),
            ('a', object),
            ("test", str),
            ("test", object),
            ((1, 2), tuple),
            ((1, 2), object),
            ([], list),
            ([], object),
            (set(), set),
            (set(), object),
            ([1, "test"], list),
            ([1, "test"], object),
            ({}, dict),
            ({}, object),
            ({"hi": 1}, dict),
            ({"hi": 1}, object)
        ]
    )
    def test_check_list_elements_type_no_errors(self, variable, intended_type):
        """
        Test for function checking if a variable is of a specified type,
        no errors raised.
        """
        result = check_variable_type(variable, intended_type)
        assert result == None

    @pytest.mark.parametrize(
        "variable, intended_type",
        [
            (2, list),
            (float('inf'), int),
            (complex(1, 1.0), float),
            (True, str),
            (None, bool),
            ('a', int),
            ("test", list),
            ((1, 2), set),
            (set(), tuple),
            ([1, "test"], dict),
            ({"hi": 1}, list)
        ]
    )
    def test_check_variable_type_typeerror(self, variable, intended_type):
        """
        Test for function checking if a variable is of a specified type,
        TypeError raised.
        """
        with pytest.raises(TypeError) as type_err_info:
            check_variable_type(variable, intended_type)
        assert str(type_err_info.value) == "Input must be of type {0}".format(str(intended_type))

class TestListElementsType:
    @pytest.mark.parametrize(
        "list_to_check, intended_type",
        [
            ([], str),
            ([], int),
            ([], list),
            ([], object),
            (["id"], str),
            (["1", "test", "2"], str),
            ([1, 2, 3, 4], int),
            ([[1], [2], ["test", "2"], []], list)
        ]
    )
    def test_check_list_elements_type_no_errors(self, list_to_check, intended_type):
        """
        Test for function checking that all elements of a list are of a specified type,
        no errors raised.
        """
        result = check_list_elements_type(list_to_check, intended_type)
        assert result == None

    @pytest.mark.parametrize(
        "list_to_check",
        [
            ((["1", "2", "3", "4", "5"], ["6"])),
            (None),
            ("test")
        ]
    )
    def test_check_list_elements_type_listtocheck_not_list(self, list_to_check):
        """
        Test for function checking if all elements of a list are strings,
        TypeError raised (list_to_check not a list)
        """
        with pytest.raises(TypeError) as type_err_info:
            check_list_elements_type(list_to_check, str)
        assert str(type_err_info.value) == "Input must be of type {0}".format(str(list))


    @pytest.mark.parametrize(
        "list_to_check, intended_type",
        [
            ([["1", "2", "3", "4", "5"], ["6"]], str),
            ([["1", "2", "3", "4", "5"], "6"], list),
            ([None, "test"], str),
            ([1, "test"], int)
        ]
    )
    def test_check_list_elements_type_typeerror(self, list_to_check, intended_type):
        """
        Test for function checking if all elements of a list are strings,
        Exception raised
        """
        with pytest.raises(TypeError) as type_err_info:
            check_list_elements_type(list_to_check, intended_type)
        assert str(type_err_info.value) == "All elements in list must be of type {0}".format(str(intended_type))