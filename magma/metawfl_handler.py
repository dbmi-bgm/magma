#!/usr/bin/env python3

################################################
#   Libraries
################################################
# from magma import metawfl #TODO: do this in FF

################################################
#   TODO: functions for dcic utils -- move later
################################################
from magma.utils import check_presence_of_attributes, set_unique_list_attributes
from magma.topological_sort import generate_ordered_steps_list

################################################
#   ValidatedDictionary
################################################
class ValidatedDictionary(object):
    """
    Parent class for MetaWorkflowStep and MetaWorkflowHandler classes.
    Takes in an input dictionary, and validates basic attributes.
    """

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: input dictionary, defined by json file, which defines basic attributes of this object
        :type input_dict: dict
        """
        # Set basic (non-calculated) attributes #
        for key in input_dict:
            setattr(self, key, input_dict[key])

    def _validate_basic_attributes(self, list_of_attributes=None):
        """
        Validation of the JSON input for this object.
        Checks that given attributes are present in the created object.
        """
        check_presence_of_attributes(self, list_of_attributes)

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
    LIST_OF_ATTRS = [META_WORKFLOW_ATTR, NAME_ATTR, DUP_FLAG_ATTR]

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: a MetaWorkflow step (object) and accompanying info within handler, defined by json file
        :type input_dict: dict
        """
        super().__init__(input_dict)

        # Validate presence of basic attributes of this MetaWorkflow step
        self._validate_basic_attributes(self.LIST_OF_ATTRS)

    def _validate_basic_attributes(self, list_of_attributes):
        """
        Validation of the JSON input for the MetaWorkflow step.
        Checks that necessary MetaWorkflow attributes are present for this MetaWorkflow step.
        """
        super()._validate_basic_attributes(list_of_attributes)
        # str, must be unique TODO: name filling in ff
        try:
            #TODO: what about if both are present? UUID is taken as default for now
            # set None for [default] arg to not throw AttributeError
            if not getattr(self, self.ITEMS_CREATION_UUID, None):
                # import pdb; pdb.set_trace()
                getattr(self, self.ITEMS_CREATION_PROP_TRACE)
        except AttributeError as e:
            raise ValueError("JSON validation error, {0}\n"
                            .format(e.args[0]))



################################################
#   MetaWorkflowHandler
################################################
class MetaWorkflowHandler(ValidatedDictionary):
    """
    Class representing a MetaWorkflow Handler object,
    a list of MetaWorkflows with specified dependencies
    """

    UUID_ATTR = "uuid"
    META_WORKFLOW_NAME_ATTR = "name"
    META_WORKFLOW_DEPENDENCIES_ATTR = "dependencies"
    META_WORKFLOWS_ATTR = "meta_workflows"
    LIST_OF_ATTRS = [UUID_ATTR]

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: MetaWorkflow Handler object, defined by json file from portal
        :type input_dict: dict
        """
        ### Basic attributes ###
        super().__init__(input_dict)
        
        super()._validate_basic_attributes(self.LIST_OF_ATTRS)

        ### Calculated attributes ###
        # to check for non-existent meta_workflows attribute
        self._set_meta_workflows_list()

        # order the meta_workflows list based on dependencies
        # ordered_meta_workflows = generate_ordered_steps_list(self.meta_workflows, self.META_WORKFLOW_NAME_ATTR, self.META_WORKFLOW_DEPENDENCIES_ATTR)
        self.ordered_meta_workflows = self._create_ordered_meta_workflows_list()

        # using ordered metaworkflows list, create a list of objects using class MetaWorkflowStep
        # this validates basic attributes needed for each metaworkflow step
        self.ordered_meta_workflow_steps = self._create_meta_workflow_step_objects()

    def _set_meta_workflows_list(self):
        """
        Checks for meta_workflows attribute, gets rid of duplicates,
        else sets as empty list if not present
        """
        set_unique_list_attributes(self, [self.META_WORKFLOWS_ATTR])

    def _create_ordered_meta_workflows_list(self):
        return generate_ordered_steps_list(self.meta_workflows, self.META_WORKFLOW_NAME_ATTR, self.META_WORKFLOW_DEPENDENCIES_ATTR)

    def _create_meta_workflow_step_objects(self):
        meta_workflow_step_list = []
        for meta_workflow in self.ordered_meta_workflows:
            meta_workflow_step_object = MetaWorkflowStep(meta_workflow)
            meta_workflow_step_list.append(meta_workflow_step_object)
        return meta_workflow_step_list
