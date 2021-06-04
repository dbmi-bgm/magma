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

The method ``ingen_obj.input_generator()`` returns a generator of ``input_json, update_json`` in json format:

  - ``input_json`` stores necessary information to run a workflow-run and can be used as input for *Tibanna*.

  - ``update_json`` stores updated information for workflow-runs and final_status.

.. code-block:: python

    for input_json, update_json in ingen_obj.input_generator():
        # input_json -> json input for tibanna zebra
        # update_json -> {
        #             'workflow_runs': [...],
        #             'final_status': 'STATUS'
        #            }
        # DO something

RunUpdate object
^^^^^^^^^^^^^^^^

*RunUpdate* object allows to update and combine *MetaWorkflowRun* objects.

Initialize RunUpdate object
***************************

.. code-block:: python

    # Read input json, meta-workflow-run
    with open('.run.json') as json_file:
        data_wflrun = json.load(json_file)

    # Creates MetaWorkflowRun object
    wflrun_obj = run.MetaWorkflowRun(data_wflrun)

    # Create RunUpdate object
    runupd_obj = utils.RunUpdate(wflrun_obj)

Methods
*******

The method ``runupd_obj.reset_steps(steps_name<str list>)`` resets *WorkflowRun* objects corresponding to step-workflows specified in *steps_name*.
Resets all workflow-runs associated to specified step-workflows.
Returns updated workflow-runs and final_status information as json.

The method ``runupd_obj.reset_shards(shards_name<str list>)`` resets *WorkflowRun* objects corresponding to workflow-runs specified in *shards_name*.
Resets only workflow-runs specified by shards.
Returns updated workflow-runs and final_status information as json.

The method ``runupd_obj.import_steps(wflrun_obj<MetaWorkflowRun obj>, steps_name<str list>)`` updates current *MetaWorkflowRun* object information, imports and use information from specified *wflrun_obj*.
Updates *WorkflowRun* objects up to all steps specified in *steps_name*.
Returns updated meta-workflow-run as json.
