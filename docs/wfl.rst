==========================
wfl module (meta-worklfow)
==========================

This is a module to work with :ref:`meta-workflows <meta-workflow-label>` in json format.

Import the library
++++++++++++++++++

.. code-block:: python

    from magma import wfl

Usage
+++++

Wfl object
^^^^^^^^^^

``Wfl`` object stores meta-workflow general information, together with step-workflows information as ``Step`` objects.

Initialize Wfl object
*********************

.. code-block:: python

    import json

    # Read input
    with open('.json') as json_file:
        data = json.load(json_file)

    # Create Wfl object
    wfl_obj = wfl.Wfl(data)

This will read meta-workflow ``.json`` content into a ``Wfl`` object and create a ``Step`` object for each of the step-workflows in ``workflows``.

Attributes
**********

  - ``wfl_obj.steps``, stores ``Step`` objects as dictionary.

      .. code-block:: python

          # wfl_obj.steps structure
          {
            step_obj_1.name = step_obj_1,
            step_obj_2.name = step_obj_2,
            ...
          }

  - ``wfl_obj.accession``, stores ``accession`` content as string.

  - ``wfl_obj.app_name``, stores ``app_name`` content as string.

  - ``wfl_obj.app_version``, stores ``app_version`` content as string.

  - ``wfl_obj.uuid``, stores ``uuid`` content as string.

  - ``wfl_obj.arguments``, stores ``arguments`` content as dict.

  - ``wfl_obj.workflows``, stores ``workflows`` content as dict.

Write workflow run
******************

The method ``wfl_obj.write_wfl_run(end_steps, inputs)`` creates a json structure for a :ref:`workflow-run-label` given specific end steps and inputs.

.. code-block:: python

    # input is a list of inputs, up to 3-dimensions
    inputs = [[['file_1', 'file_2'], ['file_3', 'file_4']]]

    # end steps, is a list of the final step-workflows for the workflow run
    end_steps = ['step_5', 'step_6']

    # run wfl_obj.write_wfl_run
    run_json = wfl_obj.write_wfl_run(end_steps, inputs)

Step object
^^^^^^^^^^^

Attributes
**********

  - ``step_obj.name``, stores ``name`` content as string.

  - ``step_obj.uuid``, stores ``uuid`` content as string.

  - ``step_obj.config``, stores ``config`` content as dict.

  - ``step_obj.arguments``, stores ``arguments`` content as dict.

  - ``step_obj.outputs``, stores ``outputs`` content as list.

  - ``step_obj.is_scatter``, stores ``scatter`` dimension for step as int.

  - ``step_obj.gather_from``, stores increment for input dimension for step-workflows to gather from as dict.

      .. code-block:: python

          # step_obj.gather_from structure
          {
            step_obj_1.name = dimension_1,
            step_obj_2.name = dimension_2,
            ...
          }

  - ``step_obj.dependencies``, stores names of step-workflows that are dependency as set.
