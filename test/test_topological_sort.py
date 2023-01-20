#!/usr/bin/env python3

#################################################################
#   Libraries
#################################################################
import pytest

from magma.metawfl_handler import MetaWorkflowStep
from magma.topological_sort import TopologicalSortHandler
from dcicutils.misc_utils import CycleError

#################################################################
#   Vars
#################################################################

A_name = "A"
B_name = "B"
C_name = "C"
D_name = "D"
E_name = "E"

# of the form [mwf_uuid, mwf_name]
# used for factory (defined below) to generate lists of dicts (steps with dependencies array)
MWF_A = ["test_mwf_uuid_0", A_name]
MWF_B = ["test_mwf_uuid_1", B_name]
MWF_C = ["test_mwf_uuid_2", C_name]
MWF_D = ["test_mwf_uuid_3", D_name]
MWF_E = ["test_mwf_uuid_4", E_name]

A = [A_name]
B = [B_name]
C = [C_name]
D = [D_name]
E = [E_name]

DEP_ON_A = [A]
DEP_ON_B = [B]
DEP_ON_C = [C]
DEP_ON_D = [D]
DEP_ON_E = [E]
DEP_EMPTY = [[]]

THREE_MWF = [MWF_A, MWF_B, MWF_C]
FOUR_MWF = [MWF_A, MWF_B, MWF_C, MWF_D]
FIVE_MWF = [MWF_A, MWF_B, MWF_C, MWF_D, MWF_E]


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

    # just to be able to create MetaWorkflowStep objects without error
    mwf_dict["items_for_creation_uuid"] = "foo"
    mwf_dict["duplication_flag"] = False 
    return mwf_dict

def create_input_meta_workflows_dict(array_of_mwf):
    input_meta_workflows_dict = {}
    for mwf in array_of_mwf:
        mwf_dictionary = meta_workflow_dict(mwf)
        mwf_name = mwf_dictionary["name"]
        input_meta_workflows_dict[mwf_name] = MetaWorkflowStep(mwf_dictionary)
    return input_meta_workflows_dict


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
# A -----> B ----> E
# ⋀        | ⋀      |
# |        |  \____|
# |        ⋁
# D <----- C 
DEPENDENCIES_CYCLIC_1 = [DEP_ON_D, [A+E], DEP_ON_B, DEP_ON_C, DEP_ON_B]
CYCLIC_1 = construct_array_of_mwf(FIVE_MWF, DEPENDENCIES_CYCLIC_1)


#################################################################
#   Tests
#################################################################
class TestTopologicalSortHandler:
    @pytest.mark.parametrize(
        "array_of_mwf, input_graph_to_topological_sort",
        [
            (DAG_0, {A_name: {}, B_name: {}, C_name: {B_name}}),
            (DAG_1, {A_name: {B_name, C_name}, B_name: {}, C_name: {}, D_name: {A_name, B_name, C_name}}),
            (CYCLIC_0, {A_name: {}, B_name: {D_name}, C_name: {B_name}, D_name: {C_name}})
        ],
    )
    def test_create_topo_sort_graph_input(self, array_of_mwf, input_graph_to_topological_sort):
        # TODO: could make these next two lines a fxn because i reuse over and over
        input_mwf_dict = create_input_meta_workflows_dict(array_of_mwf)
        sorter = TopologicalSortHandler(input_mwf_dict)
        assert sorter.graph == input_graph_to_topological_sort

    @pytest.mark.parametrize(
        "array_of_mwf, possible_sorted_lists",
        [
            (DAG_0, [[A_name, B_name, C_name]]),
            (DAG_1, [[B_name, C_name, A_name, D_name], [C_name, B_name, A_name, D_name]])
        ],
    )
    def test_sorted_graph_list(self, array_of_mwf, possible_sorted_lists):
        input_mwf_dict = create_input_meta_workflows_dict(array_of_mwf)
        sorter = TopologicalSortHandler(input_mwf_dict)
        assert sorter.sorted_graph_list() in possible_sorted_lists

    @pytest.mark.parametrize(
        "array_of_mwf",
        [
            (CYCLIC_0), (CYCLIC_1)
        ],
    )
    def test_sorted_graph_list(self, array_of_mwf):
        with pytest.raises(CycleError) as cycle_err_info:
            input_mwf_dict = create_input_meta_workflows_dict(array_of_mwf)
            sorter = TopologicalSortHandler(input_mwf_dict)
            sorter.sorted_graph_list()
        assert "nodes are in a cycle" in str(cycle_err_info.value)