#!/usr/bin/env python3

################################################
#   Libraries
################################################

# magma
from magma.metawfl_handler import MetaWorkflowHandler as MetaWorkflowHandlerFromMagma

################################################
#   MetaWorkflow Handler, Fourfront
################################################
class MetaWorkflowHandler(MetaWorkflowHandlerFromMagma):

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: MetaWorkflow Handler object defined by json file, from portal
        :type input_dict: dict
        """
        super().__init__(input_dict)

        #TODO: name filling with property traces
        #change design so mwf handler from magma only has uuids
        #prop trace handled here (change may be within mwf steps)