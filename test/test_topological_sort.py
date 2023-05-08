#!/usr/bin/env python3

#################################################################
#   Libraries
#################################################################
import pytest

from magma.metawfl_handler import MetaWorkflowStep
from magma.topological_sort import TopologicalSortHandler
from magma.magma_constants import *
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
# used for functions defined below to generate lists of dicts 
# (steps with dependencies array)
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


def construct_array_of_meta_workflows(meta_workflow_metadata_list, dependencies_list):
    """
    Function to constructs a list of lists for MetaWorkflow steps.
    Used to generate dictionaries of MetaWorkflow steps in the
    below function meta_workflow_dict.

    :param meta_workflow_metadata_list: list of the form [meta_workflow_linkTo, meta_workflow_name]
    :type meta_workflow_metadata_list: list
    :param dependencies_list: list of dependencies. Index-matched to meta_workflow_metadata_list
    :type dependencies_list: list
    :return: list of aggregated meta_workflows with their metadata needed for creation,
        of the form [meta_workflow_linkTo_1, meta_workflow_name_1, [dependencies_1],...]
    :rtype: list
    """
    length = len(meta_workflow_metadata_list)
    array_of_meta_workflows = []
    for idx in range(length):
        array_of_meta_workflows.append(meta_workflow_metadata_list[idx] + dependencies_list[idx])
    return array_of_meta_workflows


# a meta_workflow_dict generator of sorts
def meta_workflow_dict(simple_meta_workflow_metadata_list):
    """
    Constructs dictionary of MetaWorkflow Step metadata, given a list
    of the metadata.
    Attributes used here are based on MetaWorkflow Handler schema in CGAP portal.

    :param simple_meta_workflow_metadata_list: list of the form 
        [meta_workflow_linkTo, meta_workflow_name, [meta_workflow_dependencies]]
    :type simple_meta_workflow_metadata_list: list
    :return: dictionary representing a MetaWorkflow Step
    :rtype: dict
    """
    meta_workflow_dict = {
        META_WORKFLOW: simple_meta_workflow_metadata_list[0],
        NAME: simple_meta_workflow_metadata_list[1]
    }
    if len(simple_meta_workflow_metadata_list) == 3:
        meta_workflow_dict[DEPENDENCIES] = simple_meta_workflow_metadata_list[2]

    # just to be able to create MetaWorkflowStep objects without error
    meta_workflow_dict[ITEMS_FOR_CREATION_UUID] = "foo"
    meta_workflow_dict[DUP_FLAG] = False 
    return meta_workflow_dict

def create_input_meta_workflows_dict(array_of_meta_workflows):
    """
    Returns simulation of meta_workflows dictionary of the form
    {meta_workflow_name_1: MetaWorkflowStep object 1, ...}
    (defined in a MetaWorkflow Handler)

    :param array_of_meta_workflows: list of the form 
        [[meta_workflow_linkTo_1, meta_workflow_name_1, [meta_workflow_1_dependencies]], ...]
    :type array_of_meta_workflows: list
    :return: dictionary of MetaWorkflow name-MetaWorkflowStep object key-value pairs
    :rtype: dict
    """
    input_meta_workflows_dict = {}
    for meta_workflow_list in array_of_meta_workflows:
        meta_workflow_dictionary = meta_workflow_dict(meta_workflow_list)
        meta_workflow_name = meta_workflow_dictionary[NAME]
        input_meta_workflows_dict[meta_workflow_name] = MetaWorkflowStep(meta_workflow_dictionary)
    return input_meta_workflows_dict


# DAGs (directed acyclic graphs, can be typologically sorted)
# Dependency arrays are index-matched to a list of MetaWorkflow metadata
# See functions above for further detail
# -----------------------------------------------------------
# DAG_0
# A     B -----> C
DEPENDENCIES_DAG_0 = [DEP_EMPTY, DEP_EMPTY, DEP_ON_B]
DAG_0 = construct_array_of_meta_workflows(THREE_MWF, DEPENDENCIES_DAG_0)

# DAG_1
# B -----> D
# |     ⋀  ⋀
# |   /    |
# ⋁ /      |
# A <----- C 
DEPENDENCIES_DAG_1 = [[B+C], DEP_EMPTY, DEP_EMPTY, [A+B+C]]
DAG_1 = construct_array_of_meta_workflows(FOUR_MWF, DEPENDENCIES_DAG_1)


# Cyclic graphs, cannot be typologically sorted
# ----------------------------------------------
# CYCLIC_0
# A        B__
#          ⋀  \_____ 
#          |        |
#          |        ⋁
#          D <----- C 
DEPENDENCIES_CYCLIC_0 = [DEP_EMPTY, DEP_ON_D, DEP_ON_B, DEP_ON_C]
CYCLIC_0 = construct_array_of_meta_workflows(FOUR_MWF, DEPENDENCIES_CYCLIC_0)

# CYCLIC_1
# A -----> B ----> E
# ⋀        | ⋀      |
# |        |  \____|
# |        ⋁
# D <----- C 
DEPENDENCIES_CYCLIC_1 = [DEP_ON_D, [A+E], DEP_ON_B, DEP_ON_C, DEP_ON_B]
CYCLIC_1 = construct_array_of_meta_workflows(FIVE_MWF, DEPENDENCIES_CYCLIC_1)


#################################################################
#   Tests
#################################################################
class TestTopologicalSortHandler:
    @pytest.mark.parametrize(
        "array_of_meta_workflows, input_graph_to_topological_sort",
        [
            (DAG_0, {A_name: {}, B_name: {}, C_name: {B_name}}),
            (DAG_1, {A_name: {B_name, C_name}, B_name: {}, C_name: {}, D_name: {A_name, B_name, C_name}}),
            (CYCLIC_0, {A_name: {}, B_name: {D_name}, C_name: {B_name}, D_name: {C_name}})
        ],
    )
    def test_create_topo_sort_graph_input(self, array_of_meta_workflows, input_graph_to_topological_sort):
        """
        Tests conversion of MetaWorkflow Steps dict from MetaWorkflow Handler to 
        appropriately formatted input graph for a TopologicalSorter object.
        """
        # TODO: could make these next two lines a fxn because i reuse over and over
        input_meta_workflow_dict = create_input_meta_workflows_dict(array_of_meta_workflows)
        sorter = TopologicalSortHandler(input_meta_workflow_dict)
        assert sorter.graph == input_graph_to_topological_sort

    @pytest.mark.parametrize(
        "array_of_meta_workflows, possible_sorted_lists",
        [
            (DAG_0, [[A_name, B_name, C_name], [B_name, A_name, C_name], [B_name, C_name, A_name]]),
            (DAG_1, [[B_name, C_name, A_name, D_name], [C_name, B_name, A_name, D_name]])
        ],
    )
    def test_sorted_graph_list(self, array_of_meta_workflows, possible_sorted_lists):
        """
        Tests topological sorting of sortable MetaWorkflow steps.
        """
        input_meta_workflow_dict = create_input_meta_workflows_dict(array_of_meta_workflows)
        sorter = TopologicalSortHandler(input_meta_workflow_dict)
        assert sorter.sorted_graph_list() in possible_sorted_lists

    @pytest.mark.parametrize(
        "array_of_meta_workflows",
        [
            (CYCLIC_0), (CYCLIC_1)
        ],
    )
    def test_sorted_graph_list_cycle_error(self, array_of_meta_workflows):
        """
        Tests attempts to topologically sort MetaWorkflow steps with circular dependencies.
        Raises CycleError.
        """
        with pytest.raises(CycleError) as cycle_err_info:
            input_meta_workflow_dict = create_input_meta_workflows_dict(array_of_meta_workflows)
            sorter = TopologicalSortHandler(input_meta_workflow_dict)
            sorter.sorted_graph_list()
        assert "nodes are in a cycle" in str(cycle_err_info.value)