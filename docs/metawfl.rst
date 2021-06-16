========================
*metawfl* module (magma)
========================

This is a module to work with :ref:`meta-workflows <meta-workflow-label>` in json format.

Import the library
++++++++++++++++++

.. code-block:: python

    from magma import metawfl as wfl

Usage
+++++

MetaWorkflow object
^^^^^^^^^^^^^^^^^^^

*MetaWorkflow* object stores meta-workflow general information, together with step-workflows information as *StepWorkflow* objects.

Initialize MetaWorkflow object
******************************

.. code-block:: python

    import json

    # Read input json
    with open('.json') as json_file:
        data = json.load(json_file)

    # Create MetaWorkflow object
    wfl_obj = wfl.MetaWorkflow(data)

This will read meta-workflow ``.json`` content into a *MetaWorkflow* object and create a *StepWorkflow* object for each of the step-workflows in ``workflows``.

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

Write meta-workflow-run
***********************

The method ``wfl_obj.write_run(input_structure<str | str list>, [end_steps<str list>])`` returns a json structure for a :ref:`meta-workflow-run-label` given specific input_structure and end step-workflows.
It is not necessary to specify end step-workflows names, if missing are automatically calculated to run everything in the meta-workflow.

.. code-block:: python

    # input is a string or list of strings, up to 3-dimensions
    input = [[['file_1', 'file_2'], ['file_3', 'file_4']]]

    # end_steps, is a list of the final step-workflows for the meta-workflow-run
    # optional, if missing the end step are automatically calculated to run everything
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

  - ``step_obj.gather_from``, stores increment for input dimension for step-workflows to gather from as dict.

      .. code-block:: python

          # step_obj.gather_from structure
          {
            step_obj_1.name = dimension_1,
            step_obj_2.name = dimension_2,
            ...
          }

  - ``step_obj.dependencies``, stores names of step-workflows that are dependency as set, if any.
