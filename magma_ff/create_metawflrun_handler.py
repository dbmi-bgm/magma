#!/usr/bin/env python3

################################################
#   Libraries
################################################
import datetime
import json
import uuid

from dcicutils import ff_utils

# magma
from magma_ff.metawfl_handler import MetaWorkflowHandler
from magma_ff.metawflrun_handler import MetaWorkflowRunHandler
from magma_ff.utils import make_embed_request
from magma.magma_constants import *


################################################
#   Custom Exception class(es)
################################################
class MetaWorkflowRunHandlerCreationError(Exception):
    """Custom Exception when MetaWorkflow Run Handler encounters error during creation."""
    pass

################################################
#   MetaWorkflow Run Handler from Item
################################################
class MetaWorkflowRunHandlerFromItem:
    """
    Base class to hold common methods required to create and POST a
    MetaWorkflowRun Handler, and PATCH the Item used to create it (the "associated item").
    """

    # for embed requests
    ASSOCIATED_ITEM_FIELDS = [
        "project",
        "institution",
        "uuid",
        # "meta_workflow_runs.uuid",
        # "meta_workflow_runs.meta_workflow", 
        # "meta_workflow_runs.final_status"
        # TODO: these last three are for the case of reintegrating duplication flag
    ]

    META_WORKFLOW_HANDLER_FIELDS = [
        "uuid",
        "title",
        "meta_workflows",
        "meta_workflows.*"
    ]

    # TODO: is this correct?? also, will we end up patching on assoc item??
    # TODO: if so, create a schema mixin (seems unnecessary, for now)
    self.META_WORKFLOW_RUN_HANDLER_ENDPOINT = "meta-workflow-run-handlers"

    def __init__(self, associated_item_identifier, meta_workflow_handler_identifier, auth_key):
        """
        Initialize the MWF Run Handler object, set all attributes.

        :param associated_item_identifier: Item identifier (UUID, @id, or accession)
            on which this MetaWorkflow Run Handler is being created
        :type associated_item_identifier: str
        :param meta_workflow_handler_identifier: Associated MetaWorkflow Handler identifier
            (UUID, @id, or accession) -- TODO: does embed request work with an accession (yes)
        :type meta_workflow_handler_identifier: str
        :param auth_key: Portal authorization key
        :type auth_key: dict
        :raises MetaWorkflowRunHandlerCreationError: If required item (associated_item) cannot
            be found on environment of authorization key
        """
        self.auth_key = auth_key

        # Acquire associated item fields needed to create the Run Handler
        self.associated_item_dict = make_embed_request(
            associated_item_identifier,
            self.ASSOCIATED_ITEM_FIELDS,
            self.auth_key,
            single_item=True
        )
        if not self.associated_item_dict:
            raise MetaWorkflowRunHandlerCreationError(
                "No Item found for given 'associated_item' identifier: %s" % associated_item_identifier
            )

        # Acquired fields from associated MetaWorkflow Handler needed to create the Run Handler
        # TODO: a check to make sure it is indeed of mwf handler type? does this function exist on ff_utils?
        # same for above associated item request
        self.meta_workflow_handler_dict = make_embed_request(
            meta_workflow_handler_identifier,
            self.META_WORKFLOW_HANDLER_FIELDS,
            self.auth_key,
            single_item=True
        )
        if not self.meta_workflow_handler_dict:
            raise MetaWorkflowRunHandlerCreationError(
                "No MetaWorkflow Handler found for given 'meta_workflow_handler' identifier: %s"
                % meta_workflow_handler_identifier
            ) 

        # Using associated item and associated MetaWorkflow Handler fields acquired, 
        # define some basic attrs for the Run Handler: project, institution, associated_item, meta_workflow_handler
        self.project = self.associated_item_dict.get(PROJECT) # project is same as associated item
        self.institution = self.associated_item_dict.get(INSTITUTION) # institution is same as associated item
        self.associated_item_id = self.associated_item_dict.get(UUID) # get uuid of associated_item
        self.meta_workflow_handler_id = self.meta_workflow_handler_dict.get(UUID) # get uuid of the template mwf handler

        self.meta_workflow_run_handler_uuid = str(uuid.uuid4())

        # And now create the actual MetaWorkflow Run Handler using the instance vars defined above
        # This returns the complete, populated MetaWorkflow Run Handler dictionary that can be POSTed
        self.meta_workflow_run_handler = self.create_meta_workflow_run_handler()


    def create_meta_workflow_run_handler(self):
        """
        Create MetaWorkflowRun Handler dictionary, which can later be POSTed to the CGAP portal.

        :return: MetaWorkflowRun Handler dictionary (for the portal JSON object)
        :rtype: dict
        """

        # Create basic MetaWorkflow Run Handler dictionary, using instance variables
        meta_workflow_run_handler = {
            PROJECT: self.project,
            INSTITUTION: self.institution,
            UUID: self.meta_workflow_run_handler_uuid,
            ASSOCIATED_META_WORKFLOW_HANDLER: self.meta_workflow_handler_id,
            ASSOCIATED_ITEM: self.associated_item_id,
            FINAL_STATUS: PENDING
        }
        # Create  the title of the Run Handler, based on associated MetaWorkflow Handler's title
        # and the timestamp at the time of creation of this class instance
        meta_workflow_handler_title = self.meta_workflow_handler_dict.get(TITLE)
        if meta_workflow_handler_title:
            creation_date = datetime.date.today().isoformat()
            title = "MetaWorkflowRun Handler %s created %s" % (
                meta_workflow_handler_title,
                creation_date
            )
            meta_workflow_run_handler[TITLE] = title


        # now call helper method to populate and create the meta_workflow_runs array
        meta_workflow_runs_array = self.create_meta_workflow_runs_array()

        meta_workflow_run_handler[META_WORKFLOW_RUNS] = meta_workflow_runs_array
        #TODO: check for whether this is empty or nah? no for now
        # putting the burden of this error on the user

        # return the completed MetaWorkflow Run Handler dictionary, which follows the CGAP schema
        return meta_workflow_run_handler

    def create_meta_workflow_runs_array(self):
        """
        Creates meta_workflow_runs array for a MetaWorkflowRun Handler dictionary.
        These objects are in correct order due to topological sorting in
        the MetaWorkflowHandler class, and uses the associated MetaWorkflow Handler's
        ordered_meta_workflows array as a template.

        :return: array of meta_workflow_runs metadata, following CGAP schema
        :rtype: list[dict]
        """

        # Create MetaWorkflowHandler object
        # This ensures all necessary attrs are present in the following Run Handler creation
        # and that MetaWorkflow Steps are topologically sorted
        associated_meta_workflow_handler_object = MetaWorkflowHandler(self.meta_workflow_handler_dict)
        

        # Extract the ordered list of MetaWorkflows
        try:
            ordered_meta_workflows = getattr(associated_meta_workflow_handler_object, ORDERED_META_WORKFLOWS)
        except AttributeError as attr_err:
            raise MetaWorkflowRunHandlerCreationError(
                "MetaWorkflow Handler does not contain ordered MetaWorkflow steps: \n%s" % str(attr_err)
            )
        else: # edge case: ordered_meta_workflows is of NoneType
            if ordered_meta_workflows is None:
                raise MetaWorkflowRunHandlerCreationError(
                "MetaWorkflow Handler 'ordered_meta_workflows' attribute is of NoneType \n%s"
            )
        
        
        # Will eventually be the completed pending meta_workflow_runs array, in order
        ordered_meta_workflow_runs = [] 

        # Go through the ordered MetaWorkflow steps to populate basic MetaWorkflow Runs
        for meta_workflow_step_obj in ordered_meta_workflows:
            # will become the populated MetaWorkflowRun step object
            meta_workflow_run_step_obj = {}

            # Attrs that stay the same and are passed in: name, dependencies
            meta_workflow_run_step_obj[NAME] = meta_workflow_step_obj[NAME]
            meta_workflow_run_step_obj[DEPENDENCIES] = meta_workflow_step_obj[DEPENDENCIES]

            ## Handle conversion of MetaWorkflow items_for_creation_(uuid/prop_trace)
            ## to items_for_creation (just LinkTos)

            # if items_for_creation_uuid, just copy over
            if ITEMS_FOR_CREATION_UUID in meta_workflow_step_obj.keys():
                meta_workflow_run_step_obj[ITEMS_FOR_CREATION] = meta_workflow_step_obj[ITEMS_FOR_CREATION_UUID]
            # otherwise, dealing with property traces. Make necessary embed requests
            # and convert property trace(s) to uuid(s)
            else:
                items_for_creation_uuids = []
                for item_prop_trace in meta_workflow_step_obj[ITEMS_FOR_CREATION_PROP_TRACE]:
                    item_uuid = make_embed_request(
                        self.associated_item_id,
                        [item_prop_trace.uuid], # TODO: will this actually work -- test manually
                        self.auth_key,
                        single_item=True
                    )
                    if not item_uuid:
                        raise MetaWorkflowRunHandlerCreationError(
                            "Invalid property trace '%s' on item with the following ID: %s"
                            % (item_prop_trace, associated_item_id)
                        ) 
                    items_for_creation_uuids.append(item_uuid)
                meta_workflow_run_step_obj[ITEMS_FOR_CREATION] = items_for_creation_uuids

            # Basic dict for current MetaWorkflow Run step complete. Now append.
            ordered_meta_workflow_runs.append(meta_workflow_run_step_obj)

        return ordered_meta_workflow_runs


    def post_meta_workflow_run_handler(self):
        """
        Posts meta_workflow_run_handler dict to CGAP portal.

        :raises: Exception when the dict cannot be POSTed. Could be due to schema incongruencies, for example.
        """
        try:
            ff_utils.post_metadata(
                self.meta_workflow_run_handler,
                self.META_WORKFLOW_RUN_HANDLER_ENDPOINT,
                key=self.auth_key,
            )
        except Exception as error_msg:
            raise MetaWorkflowRunHandlerCreationError(
                "MetaWorkflowRunHandler not POSTed: \n%s" % str(error_msg)
            )