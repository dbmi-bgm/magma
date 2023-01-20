#!/usr/bin/env python3

################################################
#   Libraries
################################################
from copy import deepcopy

from dcicutils.misc_utils import TopologicalSorter

################################################
#   Functions
################################################
class TopologicalSortHandler(object):

    META_WORKFLOW_DEPENDENCIES_ATTR = "dependencies"

    def __init__(self, meta_workflows_dict):
        """
        Constructor method, initialize object and attributes.
        Calls method to create graph input for TopologicalSorter from dcicutils

        :param meta_workflows_dict: input dictionary of meta_workflows from MetaWorkflowHandler
        :type meta_workflows_dict: dict
        """
        # Create graph for TopologicalSorter
        self.graph = self._create_topo_sort_graph_input(meta_workflows_dict)

        # Create the sorter itself
        self.sorter = TopologicalSorter(self.graph)

    def _create_topo_sort_graph_input(self, meta_workflows_dict):
        graph = {}
        # the dict is of the form {mwf_name: MetaWorkflowStep object,...}
        for mwf_step_name, mwf_step_obj in meta_workflows_dict.items():
            dependencies = getattr(mwf_step_obj, self.META_WORKFLOW_DEPENDENCIES_ATTR)
            # if there are dependencies for this step, add to the input graph
            if dependencies:
                graph[mwf_step_name] = set(dependencies)
            else:
                graph[mwf_step_name] = {}
        return graph

    def sorted_graph_list(self):
        sorted_meta_workflows_list  = list(self.sorter.static_order())
        return sorted_meta_workflows_list