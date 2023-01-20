#!/usr/bin/env python3

################################################
#   Libraries
################################################
from copy import deepcopy

from magma.validated_dictionary import ValidatedDictionary
from magma.topological_sort import TopologicalSortHandler

################################################
#   MetaWorkflowStep
################################################
class MetaWorkflowStep(ValidatedDictionary):
    """
    Class to represent a MetaWorkflow object,
    as a step within a MetaWorkflow Handler object
    """

    META_WORKFLOW_ATTR = "meta_workflow"
    NAME_ATTR = "name"
    DUP_FLAG_ATTR = "duplication_flag"
    ITEMS_CREATION_PROP_TRACE = "items_for_creation_property_trace"
    ITEMS_CREATION_UUID = "items_for_creation_uuid"

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: a MetaWorkflow step (object) and accompanying info within handler, defined by json file
        :type input_dict: dict
        """
        super().__init__(input_dict)

        # Validate presence of basic attributes of this MetaWorkflow step
        self._validate_basic_attributes(self.META_WORKFLOW_ATTR, self.NAME_ATTR, self.DUP_FLAG_ATTR)

    def _validate_basic_attributes(self, *list_of_attributes):
        """
        Validation of the JSON input for the MetaWorkflow step.
        Checks that necessary MetaWorkflow attributes are present for this MetaWorkflow step.
        """
        super()._validate_basic_attributes(*list_of_attributes)
        # str, must be unique TODO: name filling in ff
        try:
            # set None for [default] arg to not throw AttributeError
            if not getattr(self, self.ITEMS_CREATION_UUID, None):
                getattr(self, self.ITEMS_CREATION_PROP_TRACE)
        except AttributeError as e:
            raise AttributeError("Object validation error, {0}\n"
                            .format(e.args[0]))

        # for items for creation, this object can only have
        # either the UUID or property trace, but not both
        if hasattr(self, self.ITEMS_CREATION_PROP_TRACE) and hasattr(self, self.ITEMS_CREATION_UUID):
            raise AttributeError("Object validation error, 'MetaWorkflowStep' object cannot have both of the following attributes: 'items_for_creation_property_trace' and 'items_for_creation_uuid'")


################################################
#   MetaWorkflowHandler
################################################
class MetaWorkflowHandler(ValidatedDictionary):
    """
    Class representing a MetaWorkflow Handler object,
    a list of MetaWorkflows with specified dependencies
    """

    UUID_ATTR = "uuid"
    META_WORKFLOWS_ATTR = "meta_workflows"
    META_WORKFLOW_NAME_ATTR = "name"
    META_WORKFLOW_DEPENDENCIES_ATTR = "dependencies"

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: MetaWorkflow Handler object, defined by json file from portal
        :type input_dict: dict
        """
        ### Basic attributes ###
        super().__init__(input_dict)
        
        super()._validate_basic_attributes(self.UUID_ATTR)

        ### Calculated attributes ###
        # set meta_workflows attribute
        self._set_meta_workflows_dict()

        # order the meta_workflows list based on dependencies TODO: use setattr instead?
        self.ordered_meta_workflows = self._create_ordered_meta_workflows_list()

#         # using ordered metaworkflows list, create a list of objects using class MetaWorkflowStep
#         # this validates basic attributes needed for each metaworkflow step
#         self.ordered_meta_workflow_steps = self._create_meta_workflow_step_objects()

    def _set_meta_workflows_dict(self):
        """
        Checks for meta_workflows attribute. 

        If nonexistent, set as an empty dictionary
        If present, copy that list temporarily and redefine as a dictionary
        of the form {meta_workflow_name: meta_workflow_step,....} 
        getting rid of duplicates in the process (by MetaWorkflow name)
        # TODO: this method doesn't allow for metaworkflows of the same name 
        # to be utilized in the same handler, even if they have distinct dependencies
        # check if this is disastrous lol

        :return: None, if all MetaWorkflowSteps are created successfully
        """
        if not hasattr(self, self.META_WORKFLOWS_ATTR):
            # if not present, set attribute as empty dictionary
            setattr(self, self.META_WORKFLOWS_ATTR, {})
        else:
            orig_mwf_list_copy = deepcopy(getattr(self, self.META_WORKFLOWS_ATTR))

            temp_mwf_step_dict = {}

            for mwf in orig_mwf_list_copy:
                # create MetaWorkflowStep object for this metaworkflow
                mwf_step = MetaWorkflowStep(mwf)

                # then add to the meta_workflows dictionary
                # of the form {mwf["name"]: MetaWorkflowStep(mwf)}
                temp_mwf_step_dict.setdefault(mwf["name"], mwf_step)

            # reset the "meta_workflows" attribute as an empty dictionary (rather than array)
            setattr(self, self.META_WORKFLOWS_ATTR, temp_mwf_step_dict)

    def _create_ordered_meta_workflows_list(self):
        # create "graph" that will be passed into the topological sorter
        # graph = self._create_topo_sort_graph()
        meta_workflows_dict = getattr(self, self.META_WORKFLOWS_ATTR)
        sorter = TopologicalSortHandler(meta_workflows_dict)

        # # now topologically sort the steps
        return sorter.sorted_graph_list()
