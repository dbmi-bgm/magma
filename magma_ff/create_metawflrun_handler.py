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
from magma_ff.create_metawfr import create_meta_workflow_run, MetaWorkflowRunCreationError

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
    
    # specific to a mwf run step #TODO: called later on in this class, right? right.
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
            add_on="frame=raw"
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

        #TODO: this is to check for duplicating metaworkflows
        existing_meta_workflow_runs_on_assoc_item = self.associated_item_attributes.get(self.META_WORKFLOW_RUNS, [])
        # above returns [] if no existing mwfr, else returns list of linktos
        existing_mwfs = {}
        existing_mwfrs = {}
        for mwfr in existing_meta_workflow_runs_on_assoc_item:
            existing_mwfs[mwfr["meta_workflow"]] = mwfr["uuid"]
            existing_mwfrs[mwfr["uuid"]] = mwfr["final_status"]

        self.existing_meta_workflows_on_assoc_item = existing_mwfs
        self.statuses_of_existing_mwfrs = existing_mwfrs

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
        #TODO: check for whether this is empty or nah?

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
                    )
                    items_for_creation_uuids.append(item_uuid)
                meta_workflow_run_step_obj[self.ITEMS_FOR_CREATION] = items_for_creation_uuids

            # now handle duplication flag (TODO: todo at the end --> rename -- make new if exists)
            try:
                meta_workflow_linkto = generated_mwfr_obj[self.UUID]
                # if False and a mwfr for that mwf template exists, use existing one regardless of status
                # i.e. do not duplicate the existing mwfr and linkTo the existing one
                # TODO: copy over the status, right?
                if (meta_workflow_step_obj[self.DUP_FLAG] == False) \
                    and (meta_workflow_linkto in self.existing_meta_workflows_on_assoc_item.keys()):
                    meta_workflow_run_step_obj[self.META_WORKFLOW_RUN] =  self.existing_meta_workflows_on_assoc_item[meta_workflow_linkto] # the linkTo
                    curr_mwfr_uuid = meta_workflow_run_step_obj[self.META_WORKFLOW_RUN]
                    meta_workflow_run_step_obj[self.MWFR_STATUS] = MWFR_TO_HANDLER_STEP_STATUS_DICT[self.statuses_of_existing_mwfrs[curr_mwfr_uuid]]  # copy over its status
                else: # if True, make a new MWFR for the MWF template regardless of if one exists
                    # or it could be False, but if there's no existing mwfr for this mwf, make new one
                    generated_mwfr_obj = create_meta_workflow_run(self.associated_item_id, meta_workflow_step_obj[self.MWF_UUID], self.auth_key)
                    meta_workflow_run_step_obj[self.META_WORKFLOW_RUN] = meta_workflow_linkto # the linkTo
                    meta_workflow_run_step_obj[self.MWFR_STATUS] = self.PENDING
            except MetaWorkflowRunCreationError as err:
                # here the error attribute is handled, if applicable
                #TODO: not saving full traceback here
                # also TODO: catching and not reraising the error. is this correct?
                meta_workflow_run_step_obj[self.MWFR_STATUS] = self.FAILED
                meta_workflow_run_step_obj[self.ERROR] = err

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