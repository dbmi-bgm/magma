============
utils module
============

This is a module to work with

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

.. code-block:: python

    #
    for input_json, workflow_runs in ingen_obj.input_generator():
      # input_json -> json input for tibanna zebra
      # workflow_runs -> list of workflow-runs as dictionaries for patching
      # DO something


Check Status and Output
************************

.. code-block:: python

    from magma import utils
    cs = utils.CheckStatus({}, 'fourfront-cgap')

    cs.get_status('RBwlMTyOWvpZ')
    #'complete'

    cs.get_output('RBwlMTyOWvpZ')
    #[{'workflow_argument_name': 'sorted_bam',
    #  'uuid': '07ae8a9c-260e-4b1b-80ae-ae59a624d746'}]
