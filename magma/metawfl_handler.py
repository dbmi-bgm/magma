#!/usr/bin/env python3

################################################
#   Libraries
################################################
from magma.validated_dictionary import ValidatedDictionary
# from magma.topological_sort import generate_ordered_steps_list

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


# ################################################
# #   MetaWorkflowHandler
# ################################################
# class MetaWorkflowHandler(ValidatedDictionary):
#     """
#     Class representing a MetaWorkflow Handler object,
#     a list of MetaWorkflows with specified dependencies
#     """

#     UUID_ATTR = "uuid"
#     META_WORKFLOWS_ATTR = "meta_workflows"
#     META_WORKFLOW_NAME_ATTR = "name"
#     META_WORKFLOW_DEPENDENCIES_ATTR = "dependencies"

#     def __init__(self, input_dict):
#         """
#         Constructor method, initialize object and attributes.

#         :param input_dict: MetaWorkflow Handler object, defined by json file from portal
#         :type input_dict: dict
#         """
#         ### Basic attributes ###
#         super.__init__(input_dict)
        
#         self._validate_basic_attributes(self.UUID_ATTR)

#         ### Calculated attributes ###
#         # to check for non-existent meta_workflows attribute
#         # if present, get rid of duplicates (by MetaWorkflow name)
#         self._set_meta_workflows_list()

#         # order the meta_workflows list based on dependencies
#         self.ordered_meta_workflows = self._create_ordered_meta_workflows_list()

#         # using ordered metaworkflows list, create a list of objects using class MetaWorkflowStep
#         # this validates basic attributes needed for each metaworkflow step
#         self.ordered_meta_workflow_steps = self._create_meta_workflow_step_objects()

#     def _set_meta_workflows_list(self):
#         """
#         Checks for meta_workflows attribute, 
#         sets as empty list if not present,
#         else gets rid of duplicates (by metaworkflow name)
#         """
#         if not hasattr(self, self.META_WORKFLOWS_ATTR):
#             # if not present, set attribute as empty list
#             setattr(self, self.META_WORKFLOWS_ATTR, [])
#         else:
#             attrib = getattr(self, self.META_WORKFLOWS_ATTR)

#             # then get rid of duplicates, if present
#             # non_dup_attrib = []
#             # for item in attrib:
#             #     if item not in non_dup_attrib:
#             #         non_dup_attrib.append(item)
#             # setattr(self, self.META_WORKFLOWS_ATTR, non_dup_attrib)

#     def _create_ordered_meta_workflows_list(self):
#         return generate_ordered_steps_list(self.meta_workflows, self.META_WORKFLOW_NAME_ATTR, self.META_WORKFLOW_DEPENDENCIES_ATTR)

#     def _create_meta_workflow_step_objects(self):
#         meta_workflow_step_list = []
#         for meta_workflow in self.ordered_meta_workflows:
#             meta_workflow_step_object = MetaWorkflowStep(meta_workflow)
#             meta_workflow_step_list.append(meta_workflow_step_object)
#         return meta_workflow_step_list

#         #TODO: check that there are no duplictes in ordered metaworkflows -- does this throw error or nah? TBD.
