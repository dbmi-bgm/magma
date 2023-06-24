#!/usr/bin/env python3

################################################
#   Libraries
################################################
from dcicutils.misc_utils import TopologicalSorter
from magma.magma_constants import DEPENDENCIES

################################################
#   Functions
################################################
class TopologicalSortHandler(object):

    def __init__(self, meta_workflows_dict):
        """
        Constructor method, initialize object and attributes.
        Calls method to create graph input (dict) for TopologicalSorter class,
        then sorts this graph, or raises CycleError if sort not possible.

        :param meta_workflows_dict: input dictionary of meta_workflows from MetaWorkflowHandler
        :type meta_workflows_dict: dict
        """
        # Create graph for TopologicalSorter
        self.graph = self._create_topo_sort_graph_input(meta_workflows_dict)

        # Create the sorter itself
        self.sorter = TopologicalSorter(self.graph)

    def _create_topo_sort_graph_input(self, meta_workflows_dict):
        """
        Using the meta_workflows_dict defined in the MetaWorkflow Handler,
        convert to appropriate form to input into a TopologicalSorter.

        :param meta_workflows_dict: input dictionary of meta_workflows from MetaWorkflowHandler
        :type meta_workflows_dict: dict
        :return: graph input dict for TopologicalSorter
        :rtype: dict
        """
        # the graph dict should be of the form {mwf_name: set(dependencies),...}
        graph = {}
        # the meta_workflows_dict is of the form {mwf_name: MetaWorkflowStep object,...}
        for mwf_step_name, mwf_step_obj in meta_workflows_dict.items():
            dependencies = getattr(mwf_step_obj, DEPENDENCIES)
            # if there are dependencies for this step, add to the input graph
            if dependencies:
                graph[mwf_step_name] = set(dependencies)
            else:
                graph[mwf_step_name] = {}
        return graph

    def sorted_graph_list(self):
        """
        Using the TopologicalSorter object, sorts input graph
        and returns list of meta_workflow names in a valid
        topological ordering.

        :return: list of meta_workflow names, ordered
        :rtype: list[str]
        """
        sorted_meta_workflows_list  = list(self.sorter.static_order())
        return sorted_meta_workflows_list