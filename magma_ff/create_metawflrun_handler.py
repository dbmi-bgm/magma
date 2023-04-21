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
    # Schema constants #TODO: make these constants throughout all files? and where to put that file?
    # or a file with different constant classes?
    PROJECT = "project"
    INSTITUTION = "institution"
    UUID = "uuid"
    TITLE = "title"
    ASSOCIATED_META_WORKFLOW_HANDLER = "meta_workflow_handler"
    ASSOCIATED_ITEM = "associated_item"
    FINAL_STATUS = "final_status"
    META_WORKFLOW_RUNS = "meta_workflow_runs"
    
    # specific to a mwf run step #TODO: called later on in this class, right?
    META_WORKFLOW_RUN = "meta_workflow_run"
    NAME = "name"
    MWFR_STATUS = "status"
    DEPENDENCIES = "dependencies"
    ITEMS_FOR_CREATION = "items_for_creation"
    ERROR = "error"
    
    PENDING = "pending"

    def __init__(self, associated_item_identifier, meta_workflow_handler_identifier, auth_key):
        """
        Initialize the MWF Run Handler object, set all attributes.

        :param associated_item_identifier: Item identifier (UUID, @id, or accession)
            on which this MetaWorkflow Run Handler is being created
        :type associated_item_identifier: str
        :param meta_workflow_handler_identifier: Associated MetaWorkflow Handler identifier
            (UUID, @id, or accession)
        :type meta_workflow_handler_identifier: str
        :param auth_key: Portal authorization key
        :type auth_key: dict
        :raises MetaWorkflowRunHandlerCreationError: If required item (associated_item) cannot
            be found on environment of authorization key
        """
        self.auth_key = auth_key
        # this calls for the specified metadata on the associated_item of this MWF Run Handler to be created
        #TODO: use normal get request (ffutils get metadata)
        # embedding pulls outta postgres, which is slower than elasticsearch
        # use embedding for the property tracing and duplication flag checks
        #TODO: make this change for the mwfr data structure too?

        self.associated_item_json = self.get_item_properties(associated_item_identifier)
        if not self.associated_item_json: # TODO: restructure so this creation error is in method get_item_properties
            raise MetaWorkflowRunHandlerCreationError(
                "No Item found for given 'associated item' identifier: %s" % associated_item_identifier
            )

        # check that the specified identifier for the associated MWF Handler does indeed exist on portal
        #TODO: a check to make sure it is indeed of mwf handler type? does this function exist on ff_utils?
        self.meta_workflow_handler_json = self.get_item_properties(meta_workflow_handler_identifier)
        if not self.meta_workflow_handler_json:
            raise MetaWorkflowRunHandlerCreationError(
                "No MetaWorkflow Handler found for given identifier: %s"
                % meta_workflow_handler_identifier
            )

        # now fill in the rest of the attributes of this MWF Run Handler
        self.project = self.associated_item_json.get(self.PROJECT) # project is same as associated item
        self.institution = self.associated_item_json.get(self.INSTITUTION) # institution is same as associated item
        self.associated_item_id = self.associated_item_json.get(self.UUID) # get uuid of associated item
        self.meta_workflow_handler_id = self.meta_workflow_handler_json.get(self.UUID) # get uuid of the template mwf handler
        self.meta_workflow_run_handler_uuid = str(uuid.uuid4()) #TODO: put exception to catch duplicates? i think the portal handles this

        #TODO: this is to check for duplicating metaworkflows
        existing_meta_workflow_runs_linktos = self.associated_item_json.get(self.META_WORKFLOW_RUNS, [])
        # above returns [] if no existing mwfr, else returns list of linktos

        # this is a dict of linkTos and corresponding aliases {linkTo: [aliases]}
        # self.existing_meta_workflow_runs = self.extract_mwfr_names(existing_meta_workflow_runs_linktos)
        self.existing_meta_workflows_on_assoc_item = self.extract_mwf_linktos(existing_meta_workflow_runs_linktos)

        # and now create the actual MetaWorkflow Run Handler
        # this returns the dict itself, not just an ID
        # this attribute is later used to run the thang
        self.meta_workflow_run_handler = self.create_meta_workflow_run_handler()

    # def extract_mwfr_names(self, existing_linktos_list):
    #     linkto_alias_dict = {}
    #     for linkto in existing_linktos_list:
    #         #TODO: does embed request work with @ids and uuids
    #         #TODO: match user submitted names to existing aliases...or....
    #         # because there is no existing "name" attr on mwfr schema at the moment
    #         # also is it common for an item to have several aliases
    #         aliases = make_embed_request(linkto, ["aliases"], self.auth_key, single_item=True)
    #         if not aliases:
    #             aliases = []
    #         linkto_alias_dict[linkto] = aliases
    #     return linkto_alias_dict

    def extract_mwf_linktos(self, existing_meta_workflow_runs_linktos):
        existing_mwfs = []
        for mwfr_id in existing_meta_workflow_runs_linktos:
            corresponding_mwf = make_embed_request(mwfr_id, ["meta_workflow"], self.auth_key, single_item=True)
            if not corresponding_mwf:
                continue #TODO: error check tho??
            existing_mwfs.append(corresponding_mwf)
        return existing_mwfs

    def get_item_properties(self, item_identifier):
        """
        Retrieve item from given environment without raising
        Exception if not found, rather, returns None.

        :param item_identifier: Item identifier (UUID, @id, or accession) on the portal
        :type item_identifier: str
        :return: Raw view of item if found
        :rtype: dict or None
        """
        # TODO: same as create_metawfr.py --> make a generalized function?
        try:
            result = ff_utils.get_metadata(
                item_identifier, key=self.auth_key, add_on="frame=raw"
            )
        except Exception:
            result = None
        return result

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
        # TODO: handle duplication flag??
        # TODO: should duplication only happen when the status of the original
        # mwfr is not successful?
        meta_workflow_runs_array = self.create_meta_workflow_runs_array()

        meta_workflow_run_handler[self.META_WORKFLOW_RUNS] = meta_workflow_runs_array
        #TODO: check for whether this is empty or nah?

        # return the completed MWFR Handler dictionary, following the CGAP schema
        #TODO: or the object itself??
        return meta_workflow_run_handler

    def create_meta_workflow_runs_array(self):
        # create MetaWorkflowHandler object
        associated_meta_workflow_handler_object = MetaWorkflowHandler(self.meta_workflow_handler_json)

        # then extract the ordered list of metaworkflows
        #TODO: constants list, and error catching with this call
        ordered_meta_workflows = getattr(associated_meta_workflow_handler_object, "ordered_meta_workflows")
        
        for meta_workflow_step_obj in ordered_meta_workflows:

            # mwf attrs: meta_workflow, name, items_for_creation (proptrace/uuid), dependencies, duplication_flag
            # mwfr attrs: meta_workflow_run, name, status, dependencies, items_for_creation, error
            # attrs that stay the same and are passed in: name, dependencies
            # run attrs that are automatically set already: status (pending)



        # and there is where you can check the duplication flag thing
        # and also items for creation prop trace?

        #TODO: item for creation prop trace
        #TODO: handle duplication flag
        # TODO: case where mwf run already exists? and dup flag = F? reset the run? or just redefine? yikes
        pass

    # TODO: for POST and PATCH, will there be changes to schemas other than handlers
    # in order to accomodate this? like maybe within the mixins schemas file
    # which can then be easily integrated within other schemas in the future?
    # because the mwfr handler will now be living on whatever item, rather than
    # a sample or a sample processing