#!/usr/bin/env python3

################################################
#   Libraries
################################################
from copy import deepcopy

from magma.validated_dictionary import ValidatedDictionary
from magma.topological_sort import TopologicalSortHandler
from magma.magma_constants import *
from dcicutils.misc_utils import CycleError

################################################
#   Custom Exception classes
################################################
class MetaWorkflowStepCycleError(CycleError):
    """Custom exception for cycle error tracking."""
    pass

class MetaWorkflowStepDuplicateError(ValueError):
    """Custom ValueError when MetaWorkflows don't have unique name attributes."""
    pass

class MetaWorkflowStepSelfDependencyError(ValueError):
    """Custom ValueError when MetaWorkflow Step has a dependency on itself."""
    pass

################################################
#   MetaWorkflowStep
################################################
class MetaWorkflowStep(ValidatedDictionary):
    """
    Class to represent a MetaWorkflow,
    as a step within a MetaWorkflow Handler object
    """

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: a dictionary of MetaWorkflow step metadata
        :type input_dict: dict
        """
        super().__init__(input_dict)

        # Validate presence of basic attributes of this MetaWorkflow step
        self._validate_basic_attributes(META_WORKFLOW, NAME)

        self._check_self_dependency()

    def _validate_basic_attributes(self, *list_of_attributes):
        """
        Validation of the input dictionary for the MetaWorkflow step.
        Checks that necessary MetaWorkflow attributes are present for this MetaWorkflow step.
        
        :param list_of_attributes: attributes that are checked
        :type list_of_attributes: str(s)
        :return: None, if all specified attributes are present
        :raises ValueError: if this object doesn't have a specified attribute
        :raises AttributeError: if not one (and only one) of items_for_creation attributes is present
        """
        super()._validate_basic_attributes(*list_of_attributes)
        
        ## Check that one (and only one) of the following attributes is defined on this step:
        ## ITEMS_FOR_CREATION_UUID or ITEMS_FOR_CREATION_PROP_TRACE
        try:
            # set None for [default] arg to not throw AttributeError
            #TODO: handle this within ff instead? It is CGAP portal-specific
            if not getattr(self, ITEMS_FOR_CREATION_UUID, None):
                getattr(self, ITEMS_FOR_CREATION_PROP_TRACE)
        except AttributeError as e:
            raise AttributeError("Object validation error, {0}\n"
                            .format(e.args[0]))

        # for items for creation, this object can only have
        # either the UUID or property trace, but not both
        if hasattr(self, ITEMS_FOR_CREATION_PROP_TRACE) and hasattr(self, ITEMS_FOR_CREATION_UUID):
            raise AttributeError("Object validation error, 'MetaWorkflowStep' object cannot have both of the following attributes: 'items_for_creation_property_trace' and 'items_for_creation_uuid'")

    def _check_self_dependency(self):
        """
        Check that this MetaWorkflow Step object doesn't have a self-dependency.
        
        :return: None, if no self-dependencies present
        :raises MetaWorkflowStepSelfDependencyError: if there is a self-dependency
        """
        if hasattr(self, DEPENDENCIES):
            dependencies = getattr(self, DEPENDENCIES)
            for dependency in dependencies:
                if dependency == getattr(self, NAME):
                    raise MetaWorkflowStepSelfDependencyError(f'"{dependency}" has a self dependency.')


################################################
#   MetaWorkflowHandler
################################################
class MetaWorkflowHandler(ValidatedDictionary):
    """
    Class representing a MetaWorkflow Handler object,
    including a list of MetaWorkflows with specified dependencies & other metadata
    """

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: MetaWorkflow Handler dict, defined by json file from CGAP portal
        :type input_dict: dict
        """
        ### Basic attributes ###
        super().__init__(input_dict)
        
        super()._validate_basic_attributes(UUID)

        ### Calculated attributes ###
        # set meta_workflows attribute 
        # Using meta_workflows array of dicts from CGAP MetaWorkflow Handler
        # create dict of the form {meta_workflow_name: MetaWorkflow Step object}
        self._set_meta_workflows_dict()

        # Create ordered MetaWorkflows name list based on dependencies
        # This ordered list is what's used to create the array of MetaWorkflow Runs in Run handler
        self.ordered_meta_workflows = self._create_ordered_meta_workflows_list()

    def _set_meta_workflows_dict(self):
        """
        Checks for meta_workflows attribute (an array of MetaWorkflows and their metadata) from CGAP portal. 

        If nonexistent, set handler's meta_workflows attribute as an empty dictionary
        If present, copy that list temporarily and redefine meta_workflows attribute
        as a dictionary of the form {meta_workflow_name: MetaWorkflow Step object,....} 
        checking for duplicate steps in the process (i.e. non-unique MetaWorkflow names)

        :return: None, if all MetaWorkflowSteps are created successfully
        :raises MetaWorkflowStepDuplicateError: if there are duplicate MetaWorkflows, by name
        """
        if not hasattr(self, META_WORKFLOWS):
            # if not present, set attribute as empty dictionary
            setattr(self, META_WORKFLOWS, {})
        else:
            orig_meta_workflow_list_copy = deepcopy(getattr(self, META_WORKFLOWS))

            temp_meta_workflow_step_dict = {}

            for meta_workflow in orig_meta_workflow_list_copy:
                # create MetaWorkflowStep object for this MetaWorkflow
                meta_workflow_step = MetaWorkflowStep(meta_workflow)

                # then add to the meta_workflows dictionary
                # of the form {meta_workflow["name"]: MetaWorkflowStep(meta_workflow)}
                if temp_meta_workflow_step_dict.setdefault(meta_workflow["name"], meta_workflow_step) != meta_workflow_step:
                    raise MetaWorkflowStepDuplicateError(f'"{meta_workflow["name"]}" is a duplicate MetaWorkflow, \
                        all MetaWorkflow names must be unique.')

            # redefine the "meta_workflows" attribute to this generated dictionary of MetaWorkflowStep objects
            setattr(self, META_WORKFLOWS, temp_meta_workflow_step_dict)

    def _create_ordered_meta_workflows_list(self):
        """
        Using dictionary of MetaWorkflow name and their corresponding MetaWorkflowStep objects,
        generate ordered list of MetaWorkflows, by name.
        Uses TopologicalSorter to order these steps based on their defined dependencies.

        :return: list of valid topological sorting of MetaWorkflows (by name)
        :rtype: list[str]
        :raises MetaWorkflowStepCycleError: if there are cyclic dependencies among MetaWorkflow steps
                i.e. no valid topological sorting of steps
        """
        meta_workflows_dict = getattr(self, META_WORKFLOWS)

        try:
            # create "graph" that will be passed into the topological sorter
            sorter = TopologicalSortHandler(meta_workflows_dict)
            # now topologically sort the steps
            return sorter.sorted_graph_list()
        except CycleError:
            raise MetaWorkflowStepCycleError()
