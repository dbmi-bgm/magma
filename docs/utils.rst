============
utils module
============

This is a module to work with *MetaWorkflow* and *MetaWorkflowRun* objects.

Import the library
++++++++++++++++++

.. code-block:: python

    # utils require wfl and run modules
    #   wfl -> MetaWorkflow
    #   run -> MetaWorkflowRun
    from magma import wfl
    from magma import run

    from magma import utils

Usage
+++++

InputGenerator object
^^^^^^^^^^^^^^^^^^^^^

*InputGenerator* object allows to combine and use *MetaWorkflow* and *MetaWorkflowRun* objects to map arguments and create input and patching objects in json format for workflow-runs.

Initialize InputGenerator object
********************************

.. code-block:: python

    import json

    # Read input json, meta-workflow
    with open('.json') as json_file:
        data_wfl = json.load(json_file)

    # Read input json, meta-workflow-run
    with open('.run.json') as json_file:
        data_wflrun = json.load(json_file)

    # Creates MetaWorkflow object
    wfl_obj = wfl.MetaWorkflow(data_wfl)

    # Creates MetaWorkflowRun object
    wflrun_obj = run.MetaWorkflowRun(data_wflrun)

    # Create InputGenerator object
    ingen_obj = utils.InputGenerator(wfl_obj, wflrun_obj)

Create input json to run
************************

The method ``ingen_obj.input_generator()`` returns a generator of ``input, workflow_runs`` in json format.
``input`` stores necessary information to run a workflow-run and can be used as input for *tibanna*.
``workflow_runs`` stores updated information for workflow-runs.

.. code-block:: python

    for input_json, workflow_runs in ingen_obj.input_generator():
      # input_json -> json input for tibanna zebra
      # workflow_runs -> list of workflow-runs as dictionaries for patching
      # DO something
