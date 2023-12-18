#!/usr/bin/env python3

################################################
#
#   Functions to create a MetaWorkflowRun
#
################################################

################################################
#   Libraries
################################################
import json, uuid
from dcicutils import ff_utils

# magma
from magma_smaht.metawfl import MetaWorkflow
from magma_smaht.metawflrun import MetaWorkflowRun


################################################
#   Functions
################################################
def mwfr_from_input(metawf_uuid, input, input_arg, ff_key,
                    consortia=['smaht'],
                    submission_centers=['smaht_dac']):
    """Create a MetaWorkflowRun[json] from the given MetaWorkflow[portal]
    and input arguments.

    :param metawf_uuid: MetaWorkflow[portal] UUID
    :type metawf_uuid: str
    :param input: Input arguments as list, where each argument is a dictionary
    :type list(dict)
    :param input_arg: argument_name of the input argument to use
        to calculate input structure
    :type str
    :param ff_key: Portal authorization key
    :type ff_key: dict

        e.g. input,
            input = [{
                    'argument_name': 'ARG_NAME',
                    'argument_type': 'file',
                    'files':[{'file': 'UUID', 'dimension': str(0)}]
                    }, ...]
    """

    metawf_meta = ff_utils.get_metadata(metawf_uuid, add_on='frame=raw&datastore=database', key=ff_key)

    for arg in input:
        if arg['argument_name'] == input_arg:
            input_structure = arg['files']

    mwf = MetaWorkflow(metawf_meta)
    mwfr = mwf.write_run(input_structure)

    mwfr['uuid'] = str(uuid.uuid4())
    mwfr['consortia'] = consortia
    mwfr['submission_centers'] = submission_centers
    mwfr['input'] = input

    return mwfr
