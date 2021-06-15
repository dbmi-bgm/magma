========================
parser module (magma_ff)
========================

ParserFF object
^^^^^^^^^^^^^^^

*ParserFF* object stores meta-workflow or meta-workflow-run as json.
Provides methods to allow compatibility and convert between magma and portal (ff) json formats.

Initialize ParserFF object
**************************

.. code-block:: python

    from magma_ff import parser

    #input_json -> meta-workflow or meta-workflow-run json

    pff_obj = parser.ParserFF(input_json)

Methods
*******

The method ``pff_obj.arguments_to_json()`` allows to parse meta-workflow or meta-workflow-run json stored in ``self.in_json`` attribute.
If ``input`` key is found, convert and replace arguments in ``input`` from *portal* string format to *magma* json format.
If ``workflows``, for each step-workflow convert and replace arguments in ``input`` from *portal* string format to *magma* json format.
Updates and returns the stored json.
