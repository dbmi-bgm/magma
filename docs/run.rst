==============================
run module (meta-worklfow-run)
==============================

This is a module to work with :ref:`meta-workflow-runs <meta-workflow-run-label>` in json format.

Import the library
++++++++++++++++++

.. code-block:: python

    from magma import run

Usage
+++++

MetaWorkflowRun object
^^^^^^^^^^^^^^^^^^^^^^

*MetaWorkflowRun* object stores meta-workflow-run general information, together with workflow-runs information as *WorkflowRun* objects.

Initialize MetaWorkflowRun object
*********************************

.. code-block:: python

    import json

    # Read input json
    with open('.run.json') as json_file:
        data = json.load(json_file)

    # Create MetaWorkflowRun object
    wflrun_obj = run.MetaWorkflowRun(data)

This will read meta-workflow-run ``.run.json`` content into a *MetaWorkflowRun* object and create a *WorkflowRun* object for each of the workflow-runs in ``workflow_runs``.

Attributes
**********

  - ``wflrun_obj.meta_workflow_uuid``, stores ``meta_workflow_uuid`` content as string.

  - ``wflrun_obj.input``, stores ``input`` content as list.

  - ``wflrun_obj.workflow_runs``, stores ``workflow_runs`` content as list.

  - ``wflrun_obj.runs``, stores *WorkflowRun* objects as dictionary.

  .. code-block:: python

      # wflrun_obj.runs structure
      {
        run_obj_1.shard_name = run_obj_1,
        run_obj_2.shard_name = run_obj_2,
        ...
      }

Methods
*******

The method ``wflrun_obj.to_run()`` returns a list of *WorkflowRun* objects that are ready to run (objects status is set to pending and dependencies run completed).

The method ``wflrun_obj.running()`` returns a list of *WorkflowRun* objects with status set to running.

The method ``wflrun_obj.update_attribute(shard_name<str>, attribute<str>, value<any>)`` update *attribute* *value* for *WorkflowRun* object corresponding to *shard_name* in ``wflrun_obj.runs``.

The method ``wflrun_obj.runs_to_json()`` return ``workflow_runs`` as json. Build ``workflow_runs`` directly from *WorkflowRun* objects in ``wflrun_obj.runs``.

The method ``wflrun_obj.to_json()`` return meta-workflow-run as json. Build ``workflow_runs`` directly from *WorkflowRun* objects in ``wflrun_obj.runs``.

The method ``wflrun_obj.reset_step(step_name<str>)`` reset attributes value for *WorkflowRun* objects in runs corresponding to step-workflow specified as *step_name*.
Reset all workflow-runs associated to specified step-workflow.

The method ``wflrun_obj.reset_shard(shard_name<str>)`` reset attributes value for *WorkflowRun* object in runs corresponding to workflow-run specified as *shard_name*.
Reset only workflow-run specified by shard.

The method ``wflrun_obj.update_status()`` check the status for all *WorkflowRun* objects, if all are set as completed set *MetaWorkflowRun* final status as ``completed``.

WorkflowRun object
^^^^^^^^^^^^^^^^^^

*WorkflowRun* is an object to represent a workflow-run.

Attributes
**********

  - ``run_obj.name``, stores ``name`` content as string.

  - ``run_obj.status``, stores ``status`` content as string (*pending | running | completed | failed*).

  - ``run_obj.shard``, stores ``shard`` content as string.

  - ``run_obj.shard_name``, stores ``shard_name`` (``name`` + ``shard``) content as string.

  - ``run_obj.output``, stores ``output`` content as list, default [].

  - ``run_obj.dependencies``, stores ``dependencies`` content as list, default [].
