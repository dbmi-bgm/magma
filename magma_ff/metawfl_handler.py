#!/usr/bin/env python3

################################################
#   Libraries
################################################
import copy

# magma
from magma.metawfl_handler import MetaWorkflowHandler as MetaWorkflowHandlerFromMagma
from magma_ff.parser import ParserFF

################################################
#   MetaWorkflow Handler, Fourfront
################################################
class MetaWorkflowHandler(MetaWorkflowHandlerFromMagma):

    def __init__(self, input_json):
        """
        Constructor method, initialize object and attributes.

        :param input_json: MetaWorkflow Handler object defined by json file, from portal
        :type input_json: dict
        """
        input_json_ = copy.deepcopy(input_json)

        # To handle compatibility between portal and magma json formats
        # TODO: necessary?
        ParserFF(input_json_).arguments_to_json()

        super().__init__(input_json_)

        #TODO: name filling with property traces
    #end def

#end class