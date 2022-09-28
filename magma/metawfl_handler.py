#!/usr/bin/env python3

################################################
#   Libraries
################################################
import copy
from metawfl import MetaWorkflow

################################################
#   MetaWorkflowStep
################################################
#TODO: should i put this within metaworkflowhandler class? for ease of imports is the only
# reason I can think of as reasoning for doing so

class MetaWorkflowStep(object):
    """
    Class to represent a MetaWorkflow object,
    as a step within a MetaWorkflow Handler object
    """

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: a MetaWorkflow step and accompanying info within handler, defined by json file
        :type input_dict: dict
        """
        ### Basic (non-calculated) attributes ###
        for key in input_dict:
            setattr(self, key, input_dict[key])

        # Validate presence of basic attributes of this MetaWorkflow step
        self._validate_basic_attributes()

        #TODO: import and call magma mwf to initialize the mwf within the handler
        # THEN check the dependencies
        # also need to fill in the names for the mwfs

        ### Calculated attributes ###
        # Nodes set, for building graph structure based on dependencies
        self._nodes = set() #step_objects for steps that depend on current step
        
        # Names (strings) of MetaWorkflow steps that this MetaWorkflow is dependent on
        if getattr(self, 'dependencies', None): # set None for [default] arg to not throw AttributeError
            self.dependencies = set(self.dependencies) # convert to set to not have duplicates
        else:
            self.dependencies = set()

        #TODO: case where a metaworkflow is repeated downstream? does this ever happen?


    def _validate_basic_attributes(self):
        """
        Validation of the JSON input for the MetaWorkflow step
        Checks that necessary MetaWorkflow attributes are present for this MetaWorkflow step
        """
        try:
            getattr(self, "meta_workflow") #str, must not be unique TODO: check this
            getattr(self, "name") #str, must be unique TODO: name filling in ff
            getattr(self, "duplication_flag") #bool
        except AttributeError as e:
            raise ValueError("JSON validation error, {0}\n"
                                .format(e.args[0]))

        #TODO: is there another way to integrate this other than this weird nested try except
        try:
            getattr(self, "items_for_creation_property_trace")
        except AttributeError:
            try:
                getattr(self, "items_for_creation_uuid")
            except AttributeError as e:
                raise ValueError("JSON validation error, {0}\n"
                                .format(e.args[0]))



################################################
#   MetaWorkflowHandler
################################################
class MetaWorkflowHandler(object):
    """
    Class representing a MetaWorkflow Handler object,
    a list of MetaWorkflows with specified dependencies
    """

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: MetaWorkflow Handler object dictionary, defined by json file
        :type input_dict: dict
        """

        ### Basic attributes ###
        # Required: project, institution
        # Identifying: uuid, aliases, accession
        # Commonly present: title, name, description, meta_workflows (list)
        # see cgap_portal meta_workflow_handler schema for more info
        for key in input_dict:
            setattr(self, key, input_dict[key])

        # Validate presence of basic attributes of this MetaWorkflow Handler
        self._validate_basic_attributes()

        # check for meta_workflows attribute, set empty if it is not present
        if getattr(self, 'meta_workflows', None): # set None for [default] arg to not throw AttributeError
            self.meta_workflows = self.meta_workflows # convert to set to not have duplicates
        else:
            self.meta_workflows = []

#         # Calculated attributes
#         self.steps = {} #{step_obj.name: step_obj, ...}
#         self._end_workflows = None

#         # Calculate attributes
#         self._validate_basic_attributes()
#         self._read_steps()


    def _validate_basic_attributes(self):
        """
        """
        try:
            getattr(self, 'uuid') #str, must be unique
            getattr(self, 'input') #list
            # getattr(self, 'meta_workflows') #list -- TODO: what if it's empty? -- took care of that in init
            #TODO: check project and institution? I think the schema takes care of that
        except AttributeError as e:
            raise ValueError('JSON validation error, {0}\n'
                                .format(e.args[0]))

