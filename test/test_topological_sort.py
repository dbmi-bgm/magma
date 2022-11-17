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
# used for factory fixture to generate lists of dicts (steps with dependencies array)
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

THREE_MWF = [MWF_A, MWF_B, MWF_C]
FOUR_MWF = [MWF_A, MWF_B, MWF_C, MWF_D]
FIVE_MWF = [MWF_A, MWF_B, MWF_C, MWF_D, MWF_E]
SIX_MWF = [MWF_A, MWF_B, MWF_C, MWF_D, MWF_E, MWF_F]
SEVEN_MWF = [MWF_A, MWF_B, MWF_C, MWF_D, MWF_E, MWF_F, MWF_G]
EIGHT_MWF = [MWF_A, MWF_B, MWF_C, MWF_D, MWF_E, MWF_F, MWF_G, MWF_H]
TEN_MWF = [MWF_A, MWF_B, MWF_C, MWF_D, MWF_E, MWF_F, MWF_G, MWF_H, MWF_I, MWF_J]


#TODO: add docstring of what this does -- for constructing testing graphs
def construct_array_of_mwf(mwf_metadata_list, dependencies_list):
    length = len(mwf_metadata_list)
    array_of_mwf = []
    for idx in range(length):
        array_of_mwf.append(mwf_metadata_list[idx] + dependencies_list[idx])
    return array_of_mwf


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

# had to make this fixture because can't use set() method to
# de-duplicate a list of dicts (an unhashable type)
@pytest.fixture
def non_duplicated_array():
    def _non_duplicate_array_creation(arr):
        non_dup_arr = []
        for item in arr:
            if item not in non_dup_arr:
                non_dup_arr.append(item)
        return non_dup_arr
    return _non_duplicate_array_creation

#TODO: dawg idk how to draw these
# DAGs (directed acyclic graphs, can be typologically sorted)
# TODO: briefly explain how the dependency arrays work
# and are used for construction of steps with dependencies mwf array
# -----------------------------------------------------------
# DAG_0
# A     B -----> C
DEPENDENCIES_DAG_0 = [DEP_EMPTY, DEP_EMPTY, DEP_ON_B]
DAG_0 = construct_array_of_mwf(THREE_MWF, DEPENDENCIES_DAG_0)

# DAG_1
# B -----> D
# |     ⋀  ⋀
# |   /    |
# ⋁ /      |
# A <----- C 

#TODO: do something about this nesting of different variables -- consider helper fxn?
#might make it even more confusing to do that though
DEPENDENCIES_DAG_1 = [[B+C], DEP_EMPTY, DEP_EMPTY, [A+B+C]]
DAG_1 = construct_array_of_mwf(FOUR_MWF, DEPENDENCIES_DAG_1)

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
DEPENDENCIES_DAG_2 = [[E+F], [D+F], DEP_ON_E, DEP_ON_C, DEP_EMPTY, DEP_EMPTY]
DAG_2 = construct_array_of_mwf(SIX_MWF, DEPENDENCIES_DAG_2)

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
DEPENDENCIES_DAG_3 = [DEP_EMPTY, DEP_ON_A, DEP_ON_A, [G+E+F], [B+F], DEP_ON_C, DEP_ON_B]
DAG_3 = construct_array_of_mwf(SEVEN_MWF, DEPENDENCIES_DAG_3)

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
DEPENDENCIES_DAG_4 = [DEP_EMPTY, DEP_EMPTY, DEP_ON_A, DEP_ON_B, DEP_ON_B, [C+D], [E+D], DEP_ON_G]
DAG_4 = construct_array_of_mwf(EIGHT_MWF, DEPENDENCIES_DAG_4)

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
DEPENDENCIES_DAG_5 = [DEP_EMPTY, DEP_ON_A, DEP_ON_A, DEP_ON_B, DEP_ON_B, DEP_ON_C, DEP_ON_C, DEP_ON_C, [C+G], DEP_ON_G]
DAG_5 = construct_array_of_mwf(TEN_MWF, DEPENDENCIES_DAG_5)   


# Cyclic graphs, cannot be typologically sorted
# ----------------------------------------------
# CYCLIC_0
# A        B__
#          ⋀  \_____ 
#          |        |
#          |        ⋁
#          D <----- C 
DEPENDENCIES_CYCLIC_0 = [DEP_EMPTY, DEP_ON_D, DEP_ON_B, DEP_ON_C]
CYCLIC_0 = construct_array_of_mwf(FOUR_MWF, DEPENDENCIES_CYCLIC_0)

# CYCLIC_1
# A -----> B
# ⋀        |
# |        |
# |        ⋁
# D <----- C 
DEPENDENCIES_CYCLIC_1 = [DEP_ON_D, DEP_ON_A, DEP_ON_B, DEP_ON_C]
CYCLIC_1 = construct_array_of_mwf(FOUR_MWF, DEPENDENCIES_CYCLIC_1)

# CYCLIC_2
# A -----> B ----> E
# ⋀        | ⋀      |
# |        |  \____|
# |        ⋁
# D <----- C 
DEPENDENCIES_CYCLIC_2 = [DEP_ON_D, [A+E], DEP_ON_B, DEP_ON_C, DEP_ON_B]
CYCLIC_2 = construct_array_of_mwf(FIVE_MWF, DEPENDENCIES_CYCLIC_2)

# CYCLIC_3
# B -----> A  -----> D
# ⋀       | ⋀        |
# |       | |        |
# |       | |        ⋁
# C <-----   ------- E
DEPENDENCIES_CYCLIC_3 = [[B+E], DEP_ON_C, DEP_ON_A, DEP_ON_A, DEP_ON_D]
CYCLIC_3 = construct_array_of_mwf(FIVE_MWF, DEPENDENCIES_CYCLIC_3)

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
DEPENDENCIES_CYCLIC_4 = [DEP_EMPTY, DEP_ON_A, [A+I], DEP_ON_B, DEP_ON_B, DEP_ON_C, DEP_ON_C, DEP_ON_C, DEP_ON_G, DEP_ON_G] 
CYCLIC_4 = construct_array_of_mwf(TEN_MWF, DEPENDENCIES_CYCLIC_4)


#################################################################
#   Tests
#################################################################

class TestCheckPresenceOfKey:
    @pytest.mark.parametrize(
        "empty_list_of_dicts, key_to_check, expected_result",
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
        self, empty_list_of_dicts, key_to_check, expected_result
    ):
        """
        Test for function checking that all dictionaries in a given list have the
        specified key, no errors raised, with empty list or list of empty dicts.
        """
        result = check_presence_of_key(empty_list_of_dicts, key_to_check)
        assert result == expected_result

    @pytest.mark.parametrize(
        "array_of_mwf, key_to_check, expected_result",
        [
            (
                TEN_MWF,
                "name",
                True,
            ),
            (
                THREE_MWF,
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
        self, list_of_dicts, array_of_mwf, key_to_check, expected_result
    ):
        """
        Test for function checking that all dictionaries in a given list have the
        specified key, no errors raised, regular cases.
        """
        dict_list = list_of_dicts(array_of_mwf)
        result = check_presence_of_key(dict_list, key_to_check)
        assert result == expected_result
        result2 = check_presence_of_key(dict_list + LIST_OF_DICTS_EMPTY_LIST_ATTR, "list_empty_0")
        assert result2 == False


@pytest.mark.parametrize(
    "array_of_mwf, expected_result",
    [
        (
            TEN_MWF,
            EXISTING_MWF_NAMES
        ),
        (
            [MWF_B, MWF_E, MWF_I, MWF_A],
            ["B", "E", "I", "A"] # B+E+I+A
        ),
        (
            [],
            []
        )
    ],
)
def test_generate_ordered_step_name_list(
    list_of_dicts, array_of_mwf, expected_result
):
    """
    Test for function creating a list of values for a given key,
    using a list of dictionaries.
    """
    dict_list = list_of_dicts(array_of_mwf)
    result = generate_ordered_step_name_list(dict_list, "name")
    assert result == expected_result


#TODO: will be generalizing this function later
#TODO: use your new tester helper function for constructing array_of_mwf
class TestSetDependencyListValues:
    @pytest.mark.parametrize(
        "array_of_mwf, orig_dependencies, reset_dependencies",
        [
            (
                # no changes made
                [MWF_A + DEP_ON_C, MWF_B + DEP_ON_A, MWF_C + DEP_ON_A], 
                [C, A, A],
                [C, A, A]
            ),
            (
                # get rid of dependency on nonexistent step
                [MWF_A + DEP_ON_C, MWF_B + DEP_ON_A, MWF_C + DEP_ON_D], 
                [C, A, D],
                [C, A, []]
            ),
            (
                # get rid of dependency on nonexistent steps
                [MWF_A + DEP_ON_G, MWF_B + DEP_ON_A, MWF_C + DEP_ON_D], 
                [G, A, D],
                [[], A, []]
            ),
            (
                # get rid of self-dependencies
                [MWF_A + [B + A], MWF_B + DEP_ON_A],
                [B + A, A],
                [B, A]
            ),
            (
                # get rid of duplicate dependencies 
                [MWF_A + DEP_ON_C, MWF_B + DEP_ON_A, MWF_C + [A + C + A]], 
                [C, A, A + C + A],
                [C, A, A]
            ),
            (
                # no dependencies = no change, just set dependencies to empty list
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
            (TEN_MWF, [*range(0, 10)]),
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
        Cases where the dependency lists are non-existent or not of type list,
        so fxn should either set to empty list (non-existent dependencies) or
        raise Key Error when dependencies is not of type list.
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

@pytest.mark.parametrize(
    "array_of_mwf, name_to_search, expected_step, expected_index",
    [
        ([MWF_A, MWF_B, MWF_C], "test_mwf_uuid_1", MWF_B, 1),
        ([MWF_A, MWF_B, MWF_C], "test_mwf_uuid_6", MWF_B, 1) # raises Exception
    ],
)
def test_find_step_with_given_name(
    list_of_dicts, array_of_mwf, name_to_search, expected_step, expected_index
):
    try:
        steps_with_dependencies = list_of_dicts(array_of_mwf)
        index, step = find_step_with_given_name(steps_with_dependencies, "meta_workflow", name_to_search)
    except Exception as exception_info:
        assert f"Node named {name_to_search} is a nonexistent step" == str(exception_info)
    else:
        assert index == expected_index
        assert step == meta_workflow_dict(expected_step)


class TestTopologicalSortDFSHelper:
    @pytest.mark.parametrize(
        "dag_array_of_mwf, starting_idx, expected_queue_by_index",
        [
            (DAG_0, 0, [0]),
            (DAG_0, 1, [1]),
            (DAG_0, 2, [1, 2]),
            (DAG_1, 0, [1, 2, 0]),
            (DAG_1, 3, [1, 2, 0, 3]),
            (DAG_3, 0, [0]),
            (DAG_3, 4, [0, 1, 2, 5, 4]),
            (CYCLIC_0, 0, [0]) # won't detect cycles in disconnected graphs, but overall toposort will
        ],
    )
    def test_topological_sort_helper_no_cycles(
        self, list_of_dicts, non_duplicated_array, dag_array_of_mwf, starting_idx, expected_queue_by_index
    ):
        graph = list_of_dicts(dag_array_of_mwf)
        starting_node = graph[starting_idx]
        starting_queue = []

        # TODO: make this a fixture?
        length = len(graph)
        visited_temporary = [False]*length
        visited_permanent = [False]*length

        #TODO: also make this a fixture?
        expected_queue = []
        expected_visited_permanent = [False]*length
        for i in expected_queue_by_index:
            expected_queue.append(graph[i])
            expected_visited_permanent[i] = True

        #TODO: make global constants NAME and DEPENDENCIES keys
        resulting_queue = topological_sort_dfs_helper(graph, starting_node, starting_idx, "name", "dependencies", visited_temporary, visited_permanent, starting_queue)
        assert resulting_queue == expected_queue
        assert visited_permanent == expected_visited_permanent

        # check that there are no duplicates in returned queue
        non_dup_resulting_queue = non_duplicated_array(resulting_queue)
        assert resulting_queue == non_dup_resulting_queue


    @pytest.mark.parametrize(
        "cyclic_graph_array_of_mwf, starting_idx, node_at_cycle_detection",
        [
            (CYCLIC_0, 1, "B"),
            (CYCLIC_2, 0, "A"), # just illustrating the nature of DFS w CYCLIC_2
            (CYCLIC_2, 1, "B"), 
            (CYCLIC_2, 4, "B"),
            (CYCLIC_3, 4, "A"),
            (CYCLIC_4, 6, "G") # same here
        ],
    )
    def test_topological_sort_helper_cycles(
        self, list_of_dicts, cyclic_graph_array_of_mwf, starting_idx, node_at_cycle_detection
    ):
        graph = list_of_dicts(cyclic_graph_array_of_mwf)
        starting_node = graph[starting_idx]
        starting_queue = []

        # TODO: make this a fixture? (same as prior test, also follow toposort tests)
        length = len(graph)
        visited_temporary = [False]*length
        visited_permanent = [False]*length

        #TODO: make global constants NAME and DEPENDENCIES keys
        with pytest.raises(Exception) as exception_info:
            topological_sort_dfs_helper(graph, starting_node, starting_idx, "name", "dependencies", visited_temporary, visited_permanent, starting_queue)
        assert f"Cycle in graph: node {node_at_cycle_detection}" in str(exception_info.value)

# TODO: if you make topological sort a class, you can test that visited_permanent is all True
class TestTopologicalSort:
    @pytest.mark.parametrize(
        "dag_array_of_mwf, expected_queue_by_index",
        [
            # TODO: illustrate with different starting indices, to show that
            # there exist several valid orderings, based on DFS beginning node
            # may make new DAGs w same dependencies but different ordering of the array
            (DAG_0, [0, 1, 2]),
            (DAG_1, [1, 2, 0, 3]),
            (DAG_2, [4, 5, 0, 2, 3, 1]),
            (DAG_3, [0, 1, 2, 6, 5, 4, 3]),
            (DAG_4, [0, 1, 2, 3, 4, 5, 6, 7]),
            (DAG_5, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        ],
    )
    def test_topological_sort_no_cycles(
        self, list_of_dicts, non_duplicated_array, dag_array_of_mwf, expected_queue_by_index
    ):
        graph = list_of_dicts(dag_array_of_mwf)

        # TODO: make this a fixture? (same as above tests)
        length = len(graph)

        #TODO: also make this a fixture?
        expected_queue = []
        for i in expected_queue_by_index:
            expected_queue.append(graph[i])

        #TODO: make global constants NAME and DEPENDENCIES keys
        resulting_queue = topological_sort(graph, "name", "dependencies")
        assert resulting_queue == expected_queue

        # assert that all nodes have indeed been visited
        #TODO: add this when toposort has been made a class
        # expected_visited_permanent = [True]*length
        # assert visited_permanent == expected_visited_permanent

        # check that there are no duplicates in returned queue
        non_dup_resulting_queue = non_duplicated_array(resulting_queue)
        assert resulting_queue == non_dup_resulting_queue

    #TODO: again - maybe rearrange cyclic graph nodes to show it works in whatever order
    @pytest.mark.parametrize(
        "cyclic_graph_array_of_mwf, node_at_cycle_detection",
        [
            (CYCLIC_0, "B"),
            (CYCLIC_1, "A"),
            (CYCLIC_2, "A"),
            (CYCLIC_3, "A"),
            (CYCLIC_4, "C")
        ],
    )
    def test_topological_sort_cycles(
        self, list_of_dicts, cyclic_graph_array_of_mwf, node_at_cycle_detection
    ):
        graph = list_of_dicts(cyclic_graph_array_of_mwf)

        #TODO: make global constants NAME and DEPENDENCIES keys
        with pytest.raises(Exception) as exception_info:
            topological_sort(graph, "name", "dependencies")
        assert f"Cycle in graph: node {node_at_cycle_detection}" in str(exception_info.value)