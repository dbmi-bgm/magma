import copy
from magma.metawflrun import MetaWorkflowRun as MetaWorkflowRunFromMagma
from .parser_ff import ParserFF


class MetaWorkflowRun(MetaWorkflowRunFromMagma):
    def __init__(self, input_json):
        input_json_copy = copy.deepcopy(input_json)
        ParserFF(input_json_copy).arguments_to_json()

        super().__init__(input_json_copy)
