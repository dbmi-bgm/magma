#!/usr/bin/env python3

################################################
#   Libraries
################################################

# magma
from magma.metawfl_handler import MetaWorkflowRunHandler as MetaWorkflowRunHandlerFromMagma

# from magma import metawfl #TODO: do this in FF
# from magma_ff.utils import make_embed_request #check_status, chunk_ids

################################################
#   MetaWorkflow Handler, Fourfront
################################################
class MetaWorkflowRunHandler(MetaWorkflowRunHandlerFromMagma):

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: MetaWorkflow Handler object defined by json file, from portal
        :type input_dict: dict
        """
        super().__init__(input_dict)

    #TODO: update cost