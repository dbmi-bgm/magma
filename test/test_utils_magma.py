#################################################################
#   Libraries
#################################################################
import pytest

from magma.utils import check_list_is_all_strings, check_presence_of_attributes, set_list_attributes

#################################################################
#   Vars
#################################################################

#################################################################
#   Tests
#################################################################

#TODO: replace all check_list_is_all_strings tests accordingly

@pytest.mark.parametrize(
    "list_to_check,expected",
    [
        ([], None),
        (["id"], None),
        (["1", "test", "2"], None)
    ],
)
def test_check_list_is_all_strings_no_errors(list_to_check, expected):
    """
    Test for function checking if all elements of a list are strings,
    no errors raised
    """
    result = check_list_is_all_strings(list_to_check)
    assert result == expected

@pytest.mark.parametrize(
    "list_to_check",
    [
        ([["1", "2", "3", "4", "5"], ["6"]]),
        ([["1", "2", "3", "4", "5"], "6"]),
        ([None, "test"]),
        ([1, "test"])
    ],
)
def test_check_list_is_all_strings_with_exceptions(list_to_check):
    """
    Test for function checking if all elements of a list are strings,
    Exception raised
    """
    with pytest.raises(Exception) as excinfo:
        check_list_is_all_strings(list_to_check)
    assert str(excinfo.value) == "All elements in list must be strings"

@pytest.mark.parametrize(
    "list_to_check",
    [
        (),
        (None),
        (1),
        ("test"),
        (set())
    ],
)
def test_check_list_is_all_strings_with_type_error(list_to_check):
    """
    Test for function checking if all elements of a list are strings,
    TypeError raised (incorrect argument type passed)
    """
    with pytest.raises(TypeError):
        check_list_is_all_strings(list_to_check)