#!/usr/bin/env python3

#################################################################
#   Libraries
#################################################################
import pytest

from magma.validated_dictionary import ValidatedDictionary

#################################################################
#   Vars
#################################################################
EMPTY_INPUT_DICT = {}
SIMPLE_INPUT_DICT = {"attr_0": 0}
EXTENSIVE_INPUT_DICT = {
    "attr_0": 0,
    "attr_1": "foo",
    "attr_2": False,
    "attr_3": [0, 1, 2, 3],
    "attr_4": {
        "subattr_0": 0,
        "subattr_1": "bar"
    }
}

EMPTY_VALIDATED_DICT = ValidatedDictionary(EMPTY_INPUT_DICT)
SIMPLE_VALIDATED_DICT = ValidatedDictionary(SIMPLE_INPUT_DICT)
EXTENSIVE_VALIDATED_DICT = ValidatedDictionary(EXTENSIVE_INPUT_DICT)

#################################################################
#   Tests
#################################################################
class TestValidatedDictionary:
    @pytest.mark.parametrize(
        "validated_dictionary_object, input_dict",
        [
            (EMPTY_VALIDATED_DICT, EMPTY_INPUT_DICT),
            (SIMPLE_VALIDATED_DICT, SIMPLE_INPUT_DICT),
            (EXTENSIVE_VALIDATED_DICT, EXTENSIVE_INPUT_DICT)
        ]
    )
    def test_validated_dictionary_init(self, validated_dictionary_object, input_dict):
        """
        Test of the __init__ function of the ValidatedDictionary class
        """
        present_attributes = list(input_dict.keys())
        for attr in present_attributes:
            assert hasattr(validated_dictionary_object, attr) == True
            assert getattr(validated_dictionary_object, attr) == input_dict[attr]

    @pytest.mark.parametrize(
        "validated_dictionary_object, attributes_to_check",
        [
            (EMPTY_VALIDATED_DICT, ()),
            (SIMPLE_VALIDATED_DICT, ("attr_0",)),
            (EXTENSIVE_VALIDATED_DICT, ("attr_2",)),
            (EXTENSIVE_VALIDATED_DICT, ("attr_0", "attr_1", "attr_2", "attr_3", "attr_4"))
        ]
    )
    def test_validate_basic_attributes_no_errors(self, validated_dictionary_object, attributes_to_check):
        """
        Test for function checking that specified attributes are part of a given ValidatedDictionary object,
        no errors raised.
        """
        result = validated_dictionary_object._validate_basic_attributes(*attributes_to_check)
        assert result is None

    @pytest.mark.parametrize(
        "validated_dictionary_object, attributes_to_check",
        [
            (EMPTY_VALIDATED_DICT, ("not_present", "also_not_present")),
            (SIMPLE_VALIDATED_DICT, ("attr_0", "not_present")),
            (EXTENSIVE_VALIDATED_DICT, ("attr_0", "attr_1", "not_present", "attr_2"))
        ]
    )
    def test_validate_basic_attributes_value_errors(self, validated_dictionary_object, attributes_to_check):
        """
        Test for function checking that specified attributes are part of a given ValidatedDictionary object,
        ValueError raised.
        """
        with pytest.raises(ValueError) as value_err_info:
            validated_dictionary_object._validate_basic_attributes(*attributes_to_check)
        assert "Object validation error" in str(value_err_info.value)