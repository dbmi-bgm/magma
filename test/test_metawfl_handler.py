#################################################################
#   Libraries
#################################################################
import pytest
import json

from magma import metawfl_handler as mwfh

#TODO: how is the json object passed to magma? as list? or dict?

with open('test/files/test_METAWFL_HANDLER.json') as json_file:
    data = json.load(json_file)

print(data)
print(type(data))

mwfh.MetaWorkflowHandler(data)