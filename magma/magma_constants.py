#!/usr/bin/env python3

#################################################################
#   Vars
#################################################################
TITLE = "title"

# MetaWorkflow Handler attributes
PROJECT = "project"
INSTITUTION = "institution"
UUID = "uuid"
META_WORKFLOWS = "meta_workflows"
ORDERED_META_WORKFLOWS = "ordered_meta_workflows"
META_WORKFLOW = "meta_workflow"
NAME = "name"
DEPENDENCIES = "dependencies"
ITEMS_FOR_CREATION_PROP_TRACE = "items_for_creation_property_trace"
ITEMS_FOR_CREATION_UUID = "items_for_creation_uuid"

# MetaWorkflow Run Handler attributes
COST = "cost"
STATUS = "status"
FINAL_STATUS = "final_status"
ASSOCIATED_META_WORKFLOW_HANDLER = "meta_workflow_handler"
ASSOCIATED_ITEM = "associated_item"
META_WORKFLOW_RUN = "meta_workflow_run"
META_WORKFLOW_RUNS = "meta_workflow_runs"
ITEMS_FOR_CREATION = "items_for_creation"
ERROR = "error"
# statuses
PENDING = "pending"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
STOPPED = "stopped"


#TODO: the following is here in case dup flag is added in the future
# MWFR_TO_HANDLER_STEP_STATUS_DICT = {
#     "pending": "pending",
#     "running": "running",
#     "completed": "completed",
#     "failed": "failed",
#     "inactive": "pending",
#     "stopped": "stopped",
#     "quality metric failed": "failed"
# }
