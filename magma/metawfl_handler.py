#!/usr/bin/env python3

################################################
#   Libraries
################################################
# from magma import metawfl #TODO: do this in FF

################################################
#   TODO: functions for dcic utils -- move later
################################################
from magma.utils import check_presence_of_attributes, set_list_attributes
from magma.topological_sort import generate_ordered_steps_list


#TODO: make parent class maybe

################################################
#   MetaWorkflowStep
################################################

class MetaWorkflowStep(object):
    """
    Class to represent a MetaWorkflow object,
    as a step within a MetaWorkflow Handler object
    """

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: a MetaWorkflow step (object) and accompanying info within handler, defined by json file
        :type input_dict: dict
        """
        ### Basic (non-calculated) attributes ###
        for key in input_dict:
            setattr(self, key, input_dict[key])

        # Validate presence of basic attributes of this MetaWorkflow step
        self._validate_basic_attributes()

        # Get rid of dependency duplicates -- TODO: already done within mwf?

        # Initialize Metaworkflow (magma (ff?)) using embedded call to metaworkflow LinkTo
        #TODO: do in magma ff?
        # metaworkflow_linkto = getattr(self, "meta_workflow") #TODO: embedding API -- how to test??


        #TODO: import and call magma mwf to initialize the mwf within the handler
        # THEN check the dependencies
        # also need to fill in the names for the mwfs

        #TODO: check that names of metaworkflow steps are unique -- also
        # use setdefault for filling in names (in ff? or here?) -- rather, check circularity

        #TODO: case where a metaworkflow is repeated downstream? does this ever happen?


    def _validate_basic_attributes(self): #TODO: create this as part of the utility function?
        """
        Validation of the JSON input for the MetaWorkflow step

        Checks that necessary MetaWorkflow attributes are present for this MetaWorkflow step
        """
        check_presence_of_attributes(self, ["meta_workflow", "name", "duplication_flag"])
        # str, must be unique TODO: name filling in ff
        try:
            # set None for [default] arg to not throw AttributeError
            if getattr(self, "items_for_creation_property_trace", True):
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

        :param input_dict: MetaWorkflow Handler object, defined by json file from portal
        :type input_dict: dict
        """

        ### Basic attributes ###

        for key in input_dict:
            setattr(self, key, input_dict[key])

        # Validate presence of basic attributes of this MetaWorkflow Handler
        # - Required: project, institution TODO: taken care of in schema
        # - Identifying: uuid, aliases, accession TODO: is this kinda the same as required?
        # - Commonly present: title, name, description, meta_workflows (list)
        # See cgap_portal meta_workflow_handler schema for more info.
        check_presence_of_attributes(self, ["uuid"])

        ### Calculated attributes ###

        # to check for non-existent meta_workflows attribute
        self._set_meta_workflows_list()

        # order the meta_workflows list based on dependencies
        ordered_meta_workflows = generate_ordered_steps_list(self.meta_workflows, "name", "dependencies")
        self.ordered_meta_workflows = ordered_meta_workflows
        #TODO: should i make this a new calculated attribute, rather than redefining? YES

        # create MetaWorkflow object for each metaworkflow step in meta_workflows
        #TODO: do in magma-ff? because requires pulling metadata using UUID
        #self.create_meta_workflow_steps()

    def _set_meta_workflows_list(self):
        """
        Checks for meta_workflows attribute, gets rid of duplicates,
        else sets as empty list if not present
        TODO: better to throw error if duplicates are present?
        """
        set_list_attributes(self, ["meta_workflows"])

    # def create_meta_workflow_steps(self): #TODO: in magma ff?
    #     meta_workflows_list = getattr(self, "meta_workflows") # list
    #     for meta_workflow_step in meta_workflows_list:

    #TODO: getting global input of first step ## getattr(self, 'input') # list
