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

LIST_OF_DICTS_EMPTY_LIST_ATTR = [INPUT_DICT_SINGLE_EMPTY_LIST_ATTR, INPUT_DICT_SEVERAL_EMPTY_LIST_ATTRS]

# of the form (mwf_uuid, mwf_name)
# used for factory fixture to generate lists of dicts
MWF_A = ["test_mwf_uuid_0", "A"]
MWF_B = ["test_mwf_uuid_1", "B"]
MWF_C = ["test_mwf_uuid_2", "C"]
MWF_D = ["test_mwf_uuid_3", "D"]
MWF_E = ["test_mwf_uuid_4", "E"]
MWF_F = ["test_mwf_uuid_5", "F"]
MWF_G = ["test_mwf_uuid_6", "G"]
MWF_H = ["test_mwf_uuid_7", "H"]
MWF_I = ["test_mwf_uuid_8", "I"]
MWF_J = ["test_mwf_uuid_9", "J"]

A = ["A"]
B = ["B"]
C = ["C"]
D = ["D"]
E = ["E"]
F = ["F"]
G = ["G"]
H = ["H"]
I = ["I"]
J = ["J"]

DEP_ON_A = [A]
DEP_ON_B = [B]
DEP_ON_C = [C]
DEP_ON_D = [D]
DEP_ON_E = [E]
DEP_ON_F = [F]
DEP_ON_G = [G]
DEP_ON_H = [H]
DEP_ON_I = [I]
DEP_ON_J = [J]
DEP_EMPTY = [[]]

EXISTING_MWF_NAMES = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]

SIMPLE_MWF_ORDERED_ARRAY = [MWF_A, MWF_B, MWF_C, MWF_D, MWF_E, MWF_F, MWF_G, MWF_H, MWF_I, MWF_J]

# a meta_workflow_dict generator of sorts
def meta_workflow_dict(simple_mwf_metadata_list):
    mwf_dict = {
        "meta_workflow": simple_mwf_metadata_list[0],
        "name": simple_mwf_metadata_list[1]
    }
    if len(simple_mwf_metadata_list) == 3:
        mwf_dict["dependencies"] = simple_mwf_metadata_list[2]
    return mwf_dict

@pytest.fixture
def list_of_dicts():
    def _create_list(array_of_mwf):
        created_list = []
        for simple_mwf_metadata_list in array_of_mwf:
            created_list.append(meta_workflow_dict(simple_mwf_metadata_list))
        return created_list
    return _create_list

#TODO: dawg idk how to draw these
# DAGs (directed acyclic graphs, can be typologically sorted)
# -----------------------------------------------------------
# DAG_0
# A     B -----> C
DEPENDENCIES_DAG_0 = [[], [], B]

# DAG_1
# B -----> D
# |     ⋀  ⋀
# |   /    |
# ⋁ /      |
# A <----- C 
DEPENDENCIES_DAG_1 = [B+C, [], [], A+B+C]

# DAG_2
# E -----> C
# |        |
# |        |
# ⋁        ⋁
# A -----> D 
# ⋀        |
# |        |
# |        ⋁
# F -----> B
DEPENDENCIES_DAG_2 = [E+F, D+F, E, C, [], []]

# DAG_3
# A -----> C ------> F
# |                / |
# |               /  |  
# ⋁             ⋁    ⋁  
# B ---------> E ---> D
#   \                ⋀
#    \    __________/
#     ⋁  /
#       G
DEPENDENCIES_DAG_3 = [[], A, A, G+E+F, B+F, C, B]

# DAG_4
# A ----> C ----> F
#                ⋀ 
#               /    
#             /      
# B ------> D -----> G ----> H
#   \                ⋀
#    \    __________/
#     ⋁  /
#       E
DEPENDENCIES_DAG_4 = [[], [], A, B, B, C+D, E+D, G]

# DAG_5
# A -----> B -----> E
# |        |
# |        ⋁
# |        D
# |   -> F
# ⋁ /       
# C -----> H
# | \        J
# |  \       ⋀ 
# |   \      |
# |     ---> G -----> I
# |                   ⋀ 
# |___________________|  
DEPENDENCIES_DAG_5 = [[], A, A, B, B, C, C, C, C+G, G]   


# Cyclic graphs, cannot be typologically sorted
# ----------------------------------------------
# CYCLIC_0
# A        B__
#          ⋀  \_____ 
#          |        |
#          |        ⋁
#          D <----- C 
DEPENDENCIES_CYCLIC_0 = [[], D, B, C]

# CYCLIC_1
# A -----> B
# ⋀        |
# |        |
# |        ⋁
# D <----- C 
DEPENDENCIES_CYCLIC_1 = [D, A, B, C]

# CYCLIC_2
# A -----> B ----> E
# ⋀        | ⋀      |
# |        |  \____|
# |        ⋁
# D <----- C 
DEPENDENCIES_CYCLIC_2 = [D, A+E, B, C, B]

# CYCLIC_3
# B -----> A  -----> D
# ⋀       | ⋀        |
# |       | |        |
# |       | |        ⋁
# C <-----   ------- E
DEPENDENCIES_CYCLIC_3 = [B+E, C, A, A, D]

# CYCLIC_4
# A -----> B -----> E
# |        |
# |        ⋁
# |        D
# |   -> F
# ⋁ /       
# C -----> H
# ⋀ \        J
# |  \       ⋀ 
# |   \      |
# |     ---> G -----> I
# |                   | 
# |___________________|  
DEPENDENCIES_CYCLIC_4 = [[], A, A+I, B, B, C, C, C, G, G] 


#################################################################
#   Tests
#################################################################

class TestCheckPresenceOfKey:
    @pytest.mark.parametrize(
        "empty_list_of_dicts, key_to_check, return_value",
        [
            ([], None, True),
            ([], "key", True),  # kind of weird edge case, but not a biggie (TODO:)
            (
                LIST_OF_DICTS_EMPTY_LIST_ATTR,
                "list_empty_0",
                True,
            )
        ],
    )
    def test_check_presence_of_key_empty_dicts(
        self, empty_list_of_dicts, key_to_check, return_value
    ):
        """
        Test for function checking that all dictionaries in a given list have the
        specified key, no errors raised, with empty list or list of empty dicts.
        """
        result = check_presence_of_key(empty_list_of_dicts, key_to_check)
        assert result == return_value

    @pytest.mark.parametrize(
        "array_of_mwf, key_to_check, return_value",
        [
            (
                SIMPLE_MWF_ORDERED_ARRAY,
                "name",
                True,
            ),
            (
                [MWF_A, MWF_B, MWF_C],
                "meta_workflow",
                True,
            ),
            (
                [MWF_J, MWF_I, MWF_H],
                "hi",
                False,
            )
        ],
    )
    def test_check_presence_of_key(
        self, list_of_dicts, array_of_mwf, key_to_check, return_value
    ):
        """
        Test for function checking that all dictionaries in a given list have the
        specified key, no errors raised, regular cases.
        """
        dict_list = list_of_dicts(array_of_mwf)
        result = check_presence_of_key(dict_list, key_to_check)
        assert result == return_value
        result2 = check_presence_of_key(dict_list + LIST_OF_DICTS_EMPTY_LIST_ATTR, "list_empty_0")
        assert result2 == False


@pytest.mark.parametrize(
    "array_of_mwf, return_value",
    [
        (
            SIMPLE_MWF_ORDERED_ARRAY,
            EXISTING_MWF_NAMES
        ),
        (
            [MWF_B, MWF_E, MWF_I, MWF_A],
            ["B", "E", "I", "A"]
        ),
        (
            [],
            []
        )
    ],
)
def test_generate_ordered_step_name_list(
    list_of_dicts, array_of_mwf, return_value
):
    """
    Test for function creating a list of values for a given key,
    using a list of dictionaries.
    """
    dict_list = list_of_dicts(array_of_mwf)
    result = generate_ordered_step_name_list(dict_list, "name")
    assert result == return_value


#TODO: will be generalizing this function later
class TestSetDependencyListValues:
    @pytest.mark.parametrize(
        "array_of_mwf, orig_dependencies, reset_dependencies",
        [
            (
                [MWF_A + DEP_ON_C, MWF_B + DEP_ON_A, MWF_C + DEP_ON_A], 
                [C, A, A],
                [C, A, A]
            ),
            (
                [MWF_A + DEP_ON_C, MWF_B + DEP_ON_A, MWF_C + DEP_ON_D], 
                [C, A, D],
                [C, A, []]
            ),
            (
                [MWF_A + DEP_ON_G, MWF_B + DEP_ON_A, MWF_C + DEP_ON_D], 
                [G, A, D],
                [[], A, []]
            ),
            (
                [MWF_A + [["B", "A"]], MWF_B + DEP_ON_A],
                [["B", "A"], A],
                [B, A]
            ),
            (
                [MWF_A + DEP_ON_C, MWF_B + DEP_ON_A, MWF_C + [["A", "C", "A"]]], 
                [C, A, ["A", "C", "A"]],
                [C, A, A]
            ),
            (
                [MWF_A + DEP_EMPTY],
                DEP_EMPTY,
                DEP_EMPTY
            )
        ],
    )
    def test_set_dependency_list_values_of_existing_dependencies(
        self, list_of_dicts, array_of_mwf, orig_dependencies, reset_dependencies
    ):
        """
        Test for function that gets rid of duplicates within object attributes that are lists,
        or sets attributes to empty list if not present within the object.
        Cases where the dependency lists are existent.
        """
        orig_dict_list = list_of_dicts(array_of_mwf)
        existing_step_names = generate_ordered_step_name_list(orig_dict_list, "name")

        reset_dict_list = set_dependency_list_values(orig_dict_list, "name", "dependencies", existing_step_names)

        for idx, dictionary in enumerate(orig_dict_list):
            assert dictionary["dependencies"] == orig_dependencies[idx]

        for idx, dictionary in enumerate(reset_dict_list):
            assert dictionary["dependencies"] == reset_dependencies[idx]


# TODO: dependencies originally not there --> create new dependencies list
    # non-list dependencies
    # case of [] no dicts at all
    @pytest.mark.parametrize(
        "array_of_mwf, idx_without_dependencies",
        [
            (SIMPLE_MWF_ORDERED_ARRAY, [*range(0, 10)]),
            ([MWF_A + ["hi"], MWF_B + DEP_ON_A], [0]),
            ([MWF_A + ["hi"], MWF_B], [0, 1])
        ],
    )
    def test_set_dependency_list_values_of_non_existing_dependencies(
        self, list_of_dicts, array_of_mwf, idx_without_dependencies
    ):
        """
        Test for function that gets rid of duplicates within object attributes that are lists,
        or sets attributes to empty list if not present within the object.
        Cases where the dependency lists are non-existent or not of type list.
        """
        orig_dict_list = list_of_dicts(array_of_mwf)
        existing_step_names = generate_ordered_step_name_list(orig_dict_list, "name")

        reset_dict_list = set_dependency_list_values(orig_dict_list, "name", "dependencies", existing_step_names)

        for idx in idx_without_dependencies:
            try:
                dependencies_value = orig_dict_list[idx]["dependencies"]
                assert isinstance(dependencies_value, list) == False
            except KeyError:
                pass # dicts at these indices originally didn't have dependencies attr

            # and assert that they were reset
            assert reset_dict_list[idx]["dependencies"] == []
    
    #TODO: add a test with a mix of the above two? or just assume it works (it does)