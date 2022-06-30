===========================
*metawflrun* module (magma)
===========================

This is a module to work with :ref:`MetaWorkflowRun[json] <meta-workflow-run-label>` format.

Import the library
++++++++++++++++++

.. code-block:: python

    from magma import metawflrun as run

Usage
+++++

MetaWorkflowRun object
^^^^^^^^^^^^^^^^^^^^^^

*MetaWorkflowRun* object stores MetaWorkflowRun[json] general information, together with shards information as *WorkflowRun* objects.

Initialize MetaWorkflowRun object
*********************************

.. code-block:: python

    import json

    # Read input json
    with open('.run.json') as json_file:
        data = json.load(json_file)

    # Create MetaWorkflowRun object
    wflrun_obj = run.MetaWorkflowRun(data)

This will read MetaWorkflowRun[json] ``.run.json`` content into a *MetaWorkflowRun* object and create a *WorkflowRun* object for each of the shards in ``workflow_runs``.

Attributes
**********

  - ``wflrun_obj.meta_workflow``, stores ``meta_workflow`` content as string.

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

  - ``wflrun_obj.final_status``, stores ``final_status`` as string.

Methods
*******

The method ``wflrun_obj.to_run()`` returns a list of *WorkflowRun* objects that are ready to run (objects status is set to pending and dependencies run completed).

The method ``wflrun_obj.running()`` returns a list of *WorkflowRun* objects with status set to running.

The method ``wflrun_obj.update_attribute(shard_name<str>, attribute<str>, value<any>)`` updates *attribute* *value* for *WorkflowRun* object corresponding to *shard_name* in ``wflrun_obj.runs``.

The method ``wflrun_obj.runs_to_json()`` returns ``workflow_runs`` as json. Builds ``workflow_runs`` directly from *WorkflowRun* objects in ``wflrun_obj.runs``.

The method ``wflrun_obj.to_json()`` returns MetaWorkflowRun[json]. Builds ``workflow_runs`` directly from *WorkflowRun* objects in ``wflrun_obj.runs``.

The method ``wflrun_obj.reset_step(step_name<str>)`` resets attributes value for *WorkflowRun* objects corresponding to step specified as *step_name*.
Resets all shards associated to specified step.

The method ``wflrun_obj.reset_shard(shard_name<str>)`` resets attributes value for *WorkflowRun* object in runs corresponding to shard specified as *shard_name*.
Resets only specified shard.

The method ``wflrun_obj.update_status()`` checks the status for all *WorkflowRun* objects, sets *MetaWorkflowRun* final status accordingly. Returns updated ``wflrun_obj.final_status``.

WorkflowRun object
^^^^^^^^^^^^^^^^^^

*WorkflowRun* is an object to represent a shard.

Attributes
**********

  - ``run_obj.name``, stores ``name`` content as string.

  - ``run_obj.status``, stores ``status`` content as string (*pending | running | completed | failed*).

  - ``run_obj.shard``, stores ``shard`` content as string.

  - ``run_obj.shard_name``, stores ``shard_name`` (``name`` + ``shard``) content as string.

  - ``run_obj.output``, stores ``output`` content as list, default [].

  - ``run_obj.dependencies``, stores ``dependencies`` content as list, default [].
