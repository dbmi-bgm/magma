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

################################################
#   Constants
################################################
# UUID = "uuid"
#TODO: make a file of these

MWFR_TO_HANDLER_STEP_STATUS_DICT = {
    "pending": "pending",
    "running": "running",
    "completed": "completed",
    "failed": "failed",
    "inactive": "pending",
    "stopped": "stopped",
    "quality metric failed": "failed"
}

################################################
#   Custom Exception class(es)
################################################
class MetaWorkflowRunHandlerCreationError(Exception):
    pass

################################################
#   MetaWorkflow Run Handler from Item
################################################
class MetaWorkflowRunHandlerFromItem:
    """
    Base class to hold common methods required to create and POST a
    MetaWorkflowRun Handler, and PATCH the Item used to create it (the "associated item").
    """
    # Schema constants
    PROJECT = "project"
    INSTITUTION = "institution"
    UUID = "uuid"
    TITLE = "title"
    ASSOCIATED_META_WORKFLOW_HANDLER = "meta_workflow_handler"
    ASSOCIATED_ITEM = "associated_item"
    FINAL_STATUS = "final_status"
    META_WORKFLOW_RUNS = "meta_workflow_runs"
    
    # specific to a mwf run step
    META_WORKFLOW_RUN = "meta_workflow_run"
    NAME = "name"
    MWFR_STATUS = "status"
    DEPENDENCIES = "dependencies"
    ITEMS_FOR_CREATION = "items_for_creation"
    ERROR = "error"
    DUP_FLAG = "duplication_flag"

    # mwf step (from template mwf handler)
    MWF_UUID = "meta_workflow"
    ITEMS_FOR_CREATION_UUID = "items_for_creation_uuid"
    ITEMS_FOR_CREATION_PROP_TRACE = "items_for_creation_property_trace"
    
    PENDING = "pending"
    FAILED = "failed"

    # for embed requests
    #TODO: use from constants file plz
    ASSOC_ITEM_FIELDS = [
        "project",
        "institution",
        "uuid",
        "meta_workflow_runs.uuid",
        "meta_workflow_runs.meta_workflow", #TODO: this is sometimes an @id??
        "meta_workflow_runs.final_status"
    ]

    # MWFH_FIELDS = [
    #     "uuid",
    #     "meta_workflows",
    #     "meta_workflows.items_for_creation_property_trace", #TODO: same as above??
    #     "meta_workflows.items_for_creation_uuid"
    # ]


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
            (UUID, @id, or accession) -- TODO: does embed request work with an accession
        :type meta_workflow_handler_identifier: str
        :param auth_key: Portal authorization key
        :type auth_key: dict
        :raises MetaWorkflowRunHandlerCreationError: If required item (associated_item) cannot
            be found on environment of authorization key
        """
        self.auth_key = auth_key

        self.associated_item_attributes = make_embed_request(
            associated_item_identifier,
            self.ASSOC_ITEM_FIELDS,
            self.auth_key,
            single_item=True
        )
        if not self.associated_item_attributes:
            raise MetaWorkflowRunHandlerCreationError(
                "No Item found for given 'associated item' identifier: %s" % associated_item_identifier
            )

        # check that the specified identifier for the associated MWF Handler does indeed exist on portal
        # TODO: a check to make sure it is indeed of mwf handler type? does this function exist on ff_utils?
        # same for above associated item request
        #TODO: is this even necessary?? is it too complicated of a call to
        # just check it exists? what about just a get request?
        # self.meta_workflow_handler_json = make_embed_request(
        #     meta_workflow_handler_identifier,
        #     self.MWFH_FIELDS,
        #     self.auth_key,
        #     single_item=True
        # )
        self.meta_workflow_handler_json = ff_utils.get_metadata(
            meta_workflow_handler_identifier, 
            key=self.auth_key, 
            add_on="frame=raw" #TODO: or request object view
        )
        if not self.meta_workflow_handler_json:
            raise MetaWorkflowRunHandlerCreationError(
                "No MetaWorkflow Handler found for given identifier: %s"
                % meta_workflow_handler_identifier
            ) 

        # now fill in the rest of the attributes of this MWF Run Handler
        self.project = self.associated_item_attributes.get(self.PROJECT) # project is same as associated item
        self.institution = self.associated_item_attributes.get(self.INSTITUTION) # institution is same as associated item
        self.associated_item_id = self.associated_item_attributes.get(self.UUID) # get uuid of associated item
        self.meta_workflow_handler_id = self.meta_workflow_handler_json.get(self.UUID) # get uuid of the template mwf handler
        self.meta_workflow_run_handler_uuid = str(uuid.uuid4()) #TODO: put exception to catch duplicates? i think the portal handles this

        # and now create the actual MetaWorkflow Run Handler
        # this returns the dict itself, not just an ID
        # this attribute is later used to run the thang
        self.meta_workflow_run_handler = self.create_meta_workflow_run_handler()


    def create_meta_workflow_run_handler(self):
        """
        Create MetaWorkflowRun Handler, which will later be POSTed to the CGAP portal.

        :return: MetaWorkflowRun Handler dictionary (for the portal JSON object)
        :rtype: dict
        """

        #TODO: check Doug's prior comments on title
        meta_workflow_handler_title = self.meta_workflow_handler_json.get(self.TITLE)
        creation_date = datetime.date.today().isoformat()
        title = "MetaWorkflowRun Handler %s created %s" % (
            meta_workflow_handler_title,
            creation_date
        )

        meta_workflow_run_handler = {
            self.PROJECT: self.project,
            self.INSTITUTION: self.institution,
            self.UUID: self.meta_workflow_run_handler_uuid,
            self.TITLE: title,
            self.ASSOCIATED_META_WORKFLOW_HANDLER: self.meta_workflow_handler_id,
            self.ASSOCIATED_ITEM: self.associated_item_id,
            self.FINAL_STATUS: self.PENDING
        }

        # now call helper function to populate and create the MetaWorkflow Runs
        meta_workflow_runs_array = self.create_meta_workflow_runs_array()

        meta_workflow_run_handler[self.META_WORKFLOW_RUNS] = meta_workflow_runs_array
        #TODO: check for whether this is empty or nah? no for now
        # putting the burden of this error on the user

        # return the completed MWFR Handler dictionary, which follows the CGAP schema
        return meta_workflow_run_handler

    def create_meta_workflow_runs_array(self):
        # create MetaWorkflowHandler object
        associated_meta_workflow_handler_object = MetaWorkflowHandler(self.meta_workflow_handler_json)
        # this'll make sure all necessary attrs are present in the following run handler creation

        # then extract the ordered list of metaworkflows
        #TODO: add ordered_meta_workflows to constants file
        # and error catching with this call
        ordered_meta_workflows = getattr(associated_meta_workflow_handler_object, "ordered_meta_workflows")
        
        ordered_meta_workflow_runs = [] # will eventually be the completed pending MWFRs array, in order
        for meta_workflow_step_obj in ordered_meta_workflows:
            meta_workflow_run_step_obj = {} # will become the populated MWFR step object

            # mwfr attrs: meta_workflow_run
            # attrs that stay the same and are passed in: name, dependencies
            meta_workflow_run_step_obj[self.NAME] = meta_workflow_step_obj[self.NAME]
            meta_workflow_run_step_obj[self.DEPENDENCIES] = meta_workflow_step_obj[self.DEPENDENCIES]

            # handle items_for_creation attribute
            if self.ITEMS_FOR_CREATION_UUID in meta_workflow_step_obj.keys():
                meta_workflow_run_step_obj[self.ITEMS_FOR_CREATION] = meta_workflow_step_obj[self.ITEMS_FOR_CREATION_UUID]
            else: # make embed requests as necessary
                items_for_creation_uuids = []
                for item_prop_trace in meta_workflow_step_obj[self.ITEMS_FOR_CREATION_PROP_TRACE]:
                    item_uuid = make_embed_request(
                        self.associated_item_id,
                        [item_prop_trace],
                        self.auth_key,
                        single_item=True
                    ) #TODO: add check
                    items_for_creation_uuids.append(item_uuid)
                meta_workflow_run_step_obj[self.ITEMS_FOR_CREATION] = items_for_creation_uuids


            ordered_meta_workflow_runs.append(meta_workflow_run_step_obj)

        return ordered_meta_workflow_runs


    def post_meta_workflow_run_handler(self):
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