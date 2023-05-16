#!/usr/bin/env python3

################################################
#   Libraries
################################################
from datetime import date
import json
import uuid

from dcicutils import ff_utils
from functools import cached_property

# magma
from magma_ff.metawfl_handler import MetaWorkflowHandler

# from magma_ff.metawflrun_handler import MetaWorkflowRunHandler
from magma_ff.utils import make_embed_request, JsonObject
from magma.magma_constants import *


################################################
#   Custom Exception class
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
        "title",  # TODO: test when no title present
        "meta_workflows",
        "meta_workflows.*",
    ]

    # TODO: is this correct?? also, will we end up patching on assoc item??
    # TODO: if so, create a schema mixin (seems unnecessary, for now)
    META_WORKFLOW_RUN_HANDLER_ENDPOINT = "meta-workflow-run-handlers"

    def __init__(
        self, associated_item_identifier, meta_workflow_handler_identifier, auth_key
    ):
        """
        Initialize the MWF Run Handler object, set basic attributes.

        :param associated_item_identifier: Item identifier (UUID, @id, or accession)
            on which this MetaWorkflow Run Handler is being created
        :type associated_item_identifier: str
        :param meta_workflow_handler_identifier: Associated MetaWorkflow Handler identifier
            (UUID, @id, or accession) -- TODO: does embed request work with an accession (yes)
        :type meta_workflow_handler_identifier: str
        :param auth_key: Portal authorization key
        :type auth_key: dict
        """
        if associated_item_identifier is None:
            raise MetaWorkflowRunHandlerCreationError(
                        f"Invalid argument: 'associated_item_identifier' is {str(associated_item_identifier)}"
                    )
        if meta_workflow_handler_identifier is None:
            raise MetaWorkflowRunHandlerCreationError(
                        f"Invalid argument: 'meta_workflow_handler_identifier' is {str(meta_workflow_handler_identifier)}"
                    )
        if auth_key is None:
            raise MetaWorkflowRunHandlerCreationError(
                        f"Invalid argument: 'auth_key' is {str(auth_key)}"
                    )

        self.auth_key = auth_key
        self.associated_item_identifier = associated_item_identifier
        self.meta_workflow_handler_identifier = meta_workflow_handler_identifier

    def create_meta_workflow_run_handler(self):
        """
        Create MetaWorkflowRun Handler dictionary, which can later be POSTed to the CGAP portal.

        :return: MetaWorkflowRun Handler dictionary (for the portal JSON object)
        :rtype: dict
        """

        # Create basic MetaWorkflow Run Handler dictionary, using instance variables
        meta_workflow_run_handler = {
            PROJECT: self.get_project,
            INSTITUTION: self.get_institution,
            UUID: str(uuid.uuid4()),
            ASSOCIATED_META_WORKFLOW_HANDLER: self.meta_workflow_handler_identifier,
            ASSOCIATED_ITEM: self.associated_item_identifier,
            FINAL_STATUS: PENDING,
        }
        # Create  the title of the Run Handler, based on associated MetaWorkflow Handler's title
        # and the timestamp at the time of creation of this class instance
        meta_workflow_handler_title = self.retrieved_meta_workflow_handler.get(TITLE)
        if meta_workflow_handler_title:
            creation_date = date.today()
            # creation_date = datetime.date.today().isoformat()
            title = f"MetaWorkflowRun Handler {meta_workflow_handler_title} created {creation_date.isoformat()}"
            meta_workflow_run_handler[TITLE] = title

        # now call helper method to create and populate the meta_workflow_runs array
        meta_workflow_run_handler[
            META_WORKFLOW_RUNS
        ] = self._create_meta_workflow_runs_array()
        # TODO: check for whether this is empty or nah? I'm not for now
        # putting the burden of this error on the user
        # see my note in magma/metawfl_handler.py regarding this

        # return the completed MetaWorkflow Run Handler dictionary, which follows the CGAP schema
        self.meta_workflow_run_handler = meta_workflow_run_handler
        return meta_workflow_run_handler

    def _create_meta_workflow_runs_array(self):
        """
        Creates meta_workflow_runs array for a MetaWorkflowRun Handler dictionary.
        These objects are in correct order due to topological sorting in
        the MetaWorkflowHandler class, and uses the associated MetaWorkflow Handler's
        ordered_meta_workflows array as a template.

        :return: array of meta_workflow_runs metadata, following Run Handler CGAP schema
        :rtype: list[dict]
        """

        # Will eventually be the meta_workflow_runs array, with the runs in order
        ordered_meta_workflow_runs = []

        # Go through the ordered MetaWorkflow steps to populate basic MetaWorkflow Runs
        for meta_workflow_step_name in self.ordered_meta_workflow_names:

            # self.meta_workflow_steps is a dict of step dicts, keys are step names
            meta_workflow_step = self.meta_workflow_steps[meta_workflow_step_name]
            
            # will become the populated MetaWorkflowRun step object
            meta_workflow_run_step = {}

            # Attrs that stay the same: name, dependencies
            meta_workflow_run_step[NAME] = getattr(meta_workflow_step, NAME)
            meta_workflow_run_step[DEPENDENCIES] = getattr(meta_workflow_step, DEPENDENCIES)

            # Handle conversion of MetaWorkflow items_for_creation_(uuid/prop_trace)
            # to MetaWorkflow Run items_for_creation with embed requests
            meta_workflow_run_step[ITEMS_FOR_CREATION] = self._embed_items_for_creation(
                meta_workflow_step
            )

            # Basic dict for current MetaWorkflow Run step complete. Now append.
            ordered_meta_workflow_runs.append(meta_workflow_run_step)

        return ordered_meta_workflow_runs

    def _embed_items_for_creation(self, meta_workflow_step):
        """
        From a MetaWorkflow Step, extract the items_for_creation attribute, which
        may be uuids or property traces (in relation to the associated item).

        If uuids, return this list of uuids.
        If property traces, use embed requests to convert to identifiers.

        :param meta_workflow_step: object containing a MetaWorkflow Step's metadata
        :type meta_workflow_step: dict
        :return: list of items_for_creation identifiers
        :rtype: list[str]
        :raises MetaWorkflowRunHandlerCreationError: if a property trace cannot be embedded
        """
        # if items_for_creation_uuid, just copy over
        # if ITEMS_FOR_CREATION_UUID in meta_workflow_step.keys():
        if getattr(meta_workflow_step, ITEMS_FOR_CREATION_UUID, None):
            return getattr(meta_workflow_step, ITEMS_FOR_CREATION_UUID)
        # otherwise, dealing with property traces. Make necessary embed requests
        # and convert property trace(s) to uuid(s)
        else:
            property_traces = getattr(meta_workflow_step, ITEMS_FOR_CREATION_PROP_TRACE, None)
            if not isinstance(property_traces, list):
                item_uuid = make_embed_request(
                    self.associated_item_identifier,
                    property_traces
                    + ".uuid",  # TODO: are we assuming the user will include ".uuid" or @id as part of prop trace?
                    self.auth_key,
                    single_item=True,
                )
                if not item_uuid:
                    raise MetaWorkflowRunHandlerCreationError(
                        f"Invalid property trace '{property_traces}' on item with the following ID: {self.associated_item_identifier}"
                    )
                return item_uuid


            items_for_creation_uuids = []
            for item_prop_trace in property_traces:
                item_uuid = make_embed_request(
                    self.associated_item_identifier,
                    item_prop_trace
                    + ".uuid",  # TODO: are we assuming the user will include ".uuid" or @id as part of prop trace?
                    self.auth_key,
                    single_item=True,
                )
                if not item_uuid:
                    raise MetaWorkflowRunHandlerCreationError(
                        f"Invalid property trace '{item_prop_trace}' on item with the following ID: {self.associated_item_identifier}"
                    )
                items_for_creation_uuids.append(item_uuid)
            return items_for_creation_uuids

    def post_meta_workflow_run_handler(self):
        """
        Posts meta_workflow_run_handler dict to CGAP portal.

        :raises: Exception when the dict cannot be POSTed. Could be due to schema incongruencies, for example.
        """
        try:
            ff_utils.post_metadata(
                self.meta_workflow_run_handler, #TODO: add check to see if this exists?
                self.META_WORKFLOW_RUN_HANDLER_ENDPOINT,
                key=self.auth_key,
            )
        except Exception as error_msg:
            raise MetaWorkflowRunHandlerCreationError(
                f"MetaWorkflowRunHandler not POSTed: \n{str(error_msg)}"
            ) from error_msg

    # TODO: PATCH associated item's meta_workflow_runs array?
    # I've chosen to do this in the running function instead

    @cached_property
    def retrieved_associated_item(self):
        """
        Acquire associated item fields needed to create the Run Handler
        """
        associated_item = make_embed_request(
            self.associated_item_identifier,
            self.ASSOCIATED_ITEM_FIELDS,
            self.auth_key,
            single_item=True,
        )
        if not associated_item:
            raise MetaWorkflowRunHandlerCreationError(
                f"No Item found for given 'associated_item' identifier: {self.associated_item_identifier}"
            )
        return associated_item

    @cached_property
    def retrieved_meta_workflow_handler(self):
        """
        Acquire fields from associated MetaWorkflow Handler needed to create the Run Handler
        """
        # TODO: a check to make sure it is indeed of mwf handler type? does this function exist on ff_utils?
        # same for above associated item request
        meta_workflow_handler = make_embed_request(
            self.meta_workflow_handler_identifier,
            self.META_WORKFLOW_HANDLER_FIELDS,
            self.auth_key,
            single_item=True,
        )
        if meta_workflow_handler:
            raise MetaWorkflowRunHandlerCreationError(
                f"No MetaWorkflow Handler found for given 'meta_workflow_handler' identifier: {self.meta_workflow_handler_identifier}"
            )
        return meta_workflow_handler

    @cached_property # made cached because topological sort can return different valid results
    def meta_workflow_handler_instance(self):
        """
        Creates MetaWorkflowHandler object.
        This induces topological sort of steps and validation of attributes.
        """
        return MetaWorkflowHandler(self.retrieved_meta_workflow_handler)

    @property
    def ordered_meta_workflow_names(self):
        """
        Initializes a MetaWorkflowHandler object, which topologically sorts its
        MetaWorkflow steps and contains attribute of these steps in order,
        the ordered_meta_workflows array.

        :returns: ordered_meta_workflows attribute
        :rtype: list[str]
        """
        # Extract the ordered list of MetaWorkflow names
        return  getattr(
                self.meta_workflow_handler_instance, ORDERED_META_WORKFLOWS
            )

    @property
    def meta_workflow_steps(self):
        """
        Initializes a MetaWorkflowHandler object, which topologically sorts its
        MetaWorkflow steps and contains attribute of these steps in order,
        the meta_workflows array.

        :returns: meta_workflows attribute
        :rtype: list[dict]
        """
        # Create MetaWorkflowHandler object
        # This ensures all necessary attrs are present in the following Run Handler creation
        # and that MetaWorkflow Steps are topologically sorted
        associated_meta_workflow_handler_object = MetaWorkflowHandler(
            self.retrieved_meta_workflow_handler
        )

        # Extract the ordered list of MetaWorkflow names
        return  getattr(
                associated_meta_workflow_handler_object, META_WORKFLOWS
            )

    @property
    def get_project(self):
        """Retrieves project attribute from the associated item."""
        return self.retrieved_associated_item.get(PROJECT)

    @property
    def get_institution(self):
        """Retrieves institution attribute from the associated item."""
        return self.retrieved_associated_item.get(INSTITUTION)

####################################################
#   Wrapper Fxn: MetaWorkflow Run Handler from Item
####################################################
def create_meta_workflow_run_handler(
    associated_item_identifier: str,
    meta_workflow_handler_identifier: str,
    auth_key: JsonObject,
    post: bool = True,
) -> JsonObject:
    """Create a MetaWorkflowRunHandler for the given associated item and MetaWorkflow Handler.

    POST MetaWorkflowRun as instructed.

    :param associated_item_identifier: Identifier (e.g. UUID, @id) for item from
        which to create the MetaWorkflowRun Handler
    :param meta_workflow_handler_identifier: Identifier for the MetaWorkflow Handler
        from which to create the MetaWorkflowRun Handler
    :param auth_key: Authorization keys for C4 account
    :param post: Whether to POST the MetaWorkflowRun Handler created
    :returns: MetaWorkflowRun Handler created
    """
    meta_workflow_run_handler_creator = MetaWorkflowRunHandlerFromItem(associated_item_identifier, meta_workflow_handler_identifier, auth_key)
    run_handler = meta_workflow_run_handler_creator.create_meta_workflow_run_handler()
    if post:
        meta_workflow_run_handler_creator.post_meta_workflow_run_handler()
    return run_handler
