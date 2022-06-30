========================
*metawfl* module (magma)
========================

This is a module to work with :ref:`MetaWorkflow[json] <meta-workflow-label>` format.

Import the library
++++++++++++++++++

.. code-block:: python

    from magma import metawfl as wfl

Usage
+++++

MetaWorkflow object
^^^^^^^^^^^^^^^^^^^

*MetaWorkflow* object stores MetaWorkflow[json] general information, together with specific information for each of the steps as *StepWorkflow* objects.

Initialize MetaWorkflow object
******************************

.. code-block:: python

    import json

    # Read input json
    with open('.json') as json_file:
        data = json.load(json_file)

    # Create MetaWorkflow object
    wfl_obj = wfl.MetaWorkflow(data)

This will read MetaWorkflow[json] ``.json`` content into a *MetaWorkflow* object and create a *StepWorkflow* object for each of the steps in ``workflows``.

Attributes
**********

  - ``wfl_obj.steps``, stores *StepWorkflow* objects as dictionary.

      .. code-block:: python

          # wfl_obj.steps structure
          {
            step_obj_1.name = step_obj_1,
            step_obj_2.name = step_obj_2,
            ...
          }

  - ``wfl_obj.name``, stores ``name`` content as string, if any.

  - ``wfl_obj.uuid``, stores ``uuid`` content as string.

  - ``wfl_obj.input``, stores ``input`` content as list.

  - ``wfl_obj.workflows``, stores ``workflows`` content as list.

Write MetaWorkflowRun[json]
***************************

The method ``wfl_obj.write_run(input_structure<str | str list>, [end_steps<str list>])`` returns a :ref:`MetaWorkflowRun[json] <meta-workflow-run-label>` given specific input_structure and end steps.
It is not necessary to specify names for end steps. If missing, shards are automatically calculated to run all the steps.

.. code-block:: python

    # input is a string or list of strings, up to 3-dimensions
    input = [[['file_1', 'file_2'], ['file_3', 'file_4']]]

    # end_steps, is a list of the final steps to run
    # if missing, the end steps are automatically calculated to run everything
    end_steps = ['step_5', 'step_6']

    # run wfl_obj.write_run
    run_json = wfl_obj.write_run(input, end_steps)

StepWorkflow object
^^^^^^^^^^^^^^^^^^^

Attributes
**********

  - ``step_obj.name``, stores ``name`` content as string.

  - ``step_obj.workflow``, stores ``workflow`` content as string.

  - ``step_obj.config``, stores ``config`` content as dict.

  - ``step_obj.input``, stores ``input`` content as list.

  - ``step_obj.is_scatter``, stores ``scatter`` dimension for step as int.

  - ``step_obj.gather_from``, stores increment for input dimension for steps to gather from as dict.

      .. code-block:: python

          # step_obj.gather_from structure
          {
            step_obj_1.name = dimension_1,
            step_obj_2.name = dimension_2,
            ...
          }

  - ``step_obj.dependencies``, stores names of steps that are dependency as set, if any.
