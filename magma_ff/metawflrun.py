import copy
from magma.metawflrun import MetaWorkflowRun as MetaWorkflowRunFromMagma
from magma_ff.parser_ff import ParserFF


class MetaWorkflowRun(MetaWorkflowRunFromMagma):
    def __init__(self, input_json):
        input_json_ = copy.deepcopy(input_json)
        ParserFF(input_json_).arguments_to_json()

        super().__init__(input_json_)
