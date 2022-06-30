==========================
*parser* module (magma_ff)
==========================

ParserFF object
^^^^^^^^^^^^^^^

While magma uses MetaWorkflow[json] or MetaWorkflowRun[json] formats, the portal uses slightly different formats where arguments are encoded as string.
*ParserFF* provides methods to allow compatibility and convert between the portal and magma arguments representations.

Initialize ParserFF object
**************************

.. code-block:: python

    from magma_ff import parser

    #input_json
    #   -> portal representation of MetaWorkflow[json] or MetaWorkflowRun[json]

    pff_obj = parser.ParserFF(input_json)

Methods
*******

The method ``pff_obj.arguments_to_json()`` parses the portal representation of MetaWorkflow[json] or MetaWorkflowRun[json] stored in ``self.in_json`` attribute.
If ``input`` key is found, converts and replaces arguments in ``input`` from *portal* string format to *magma* format.
If ``workflows``, for each steps converts and replaces arguments in ``input`` from *portal* string format to *magma* format.
Updates and returns ``self.in_json``.
