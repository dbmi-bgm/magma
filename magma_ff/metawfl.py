import copy
from magma.metawfl import MetaWorkflow as MetaWorkflowFromMagma
from magma_ff.parser_ff import ParserFF


class MetaWorkflow(MetaWorkflowFromMagma):
    def __init__(self, input_json):
        input_json_ = copy.deepcopy(input_json)
        ParserFF(input_json_).arguments_to_json()

        super().__init__(input_json_)
