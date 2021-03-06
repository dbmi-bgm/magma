========
Examples
========

Example 1.
**********

This is a real example on how to create a new MetaWorkflowRun[json] from a MetaWorkflow[json], and import steps information from an old MetaWorkflowRun[json].
The code use magma_ff for compatibility with the portal.

.. code-block:: python

    # Libraries
    from magma_ff import metawfl as wfl
    from magma import metawflrun as run
    # Using metawflrun from magma allows to keep the original json
    #     as it is returned by the portal,
    #     to apply the parser use magma_ff instead
    from magma_ff import runupdate as runupd

    # Get MetaWorkflow[json] from the portal
    #   --> wfl_json

    # Create MetaWorkflow object
    wfl_obj = wfl.MetaWorkflow(wfl_json)

    # Get old MetaWorkflowRun[json] from the portal
    #   --> run_json_toimport

    # Create MetaWorkflowRun object for old MetaWorkflowRun[json]
    run_obj_toimport = run.MetaWorkflowRun(run_json_toimport)

    # Create the new MetaWorkflowRun[json] from MetaWorkflow object
    input_structure = [['a'],['b'],['c']]
    run_json = wfl_obj.write_run(input_structure)

    # Create MetaWorkflowRun object for new MetaWorkflowRun[json]
    run_obj = run.MetaWorkflowRun(run_json)

    # Create RunUpdate object
    runupd_obj = runupd.RunUpdate(run_obj)

    # Import information
    steps_name = ['fastqc-r1', 'fastqc-r2', 'workflow_samplegeno', 'cgap-bamqc', 'workflow_granite-mpileupCounts']
    run_json_updated = runupd_obj.import_steps(run_obj_toimport, steps_name)
