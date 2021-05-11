.. _meta-workflow-label:

=============
meta-workflow
=============

A meta-workflow is a json object that describes the general structure of a multi-step workflow.
Stores general information for the multi-step workflow, as well as specific information for the workflows that are steps of the meta-workflow (step-workflows).

Structure
+++++++++

.. code-block:: python

    {
      ## General meta-worflow information
      #   These are general fields that are required by the parser,
      #   however, there is no check on the content that can be customized
      "accession": "", # custom unique identifier
      "app_name": "", # name for the meta-worflow
      "app_version": "", # version for the meta-worflow
      "uuid": "", # universally unique identifier

      ## General meta-worflow arguments
      #   These are general arguments that are used by multiple steps
      #   and can be grouped here to avoid repetition
      "arguments": [

        # Structure for a file argument
        {
          # These are necessary fields
          "argument_name": "",
          "argument_type": "file",
          "uuid": "",
          # These are optional fields
          #   It's possible to skip these fields or add custom ones
          "mount": False,
          "rename": "",
          "unzip": ""
        },

        # Structure for a parameter argument
        {
          # These are necessary fields
          "argument_name": "",
          "argument_type": "parameter",
          "value": ""
        }

      ],

      ## Steps for the meta-workflow
      "workflows": [

        # Structure for step-workflow
        {
          # General step-workflow information
          #   These are general fields that are required by the parser,
          #   however, there is no check on the content that can be customized
          "name": "", # name for the step-workflow
          "uuid": "", # universally unique identifier
          "config": { # configuration for the step-workflow
            "instance_type": "",
            "ebs_size": "",
            "EBS_optimized": True,
            "spot_instance": True,
            "log_bucket": "tibanna-output",
            "run_name": "run_",
            "behavior_on_capacity_limit": "wait_and_retry"
          },

          # Step-workflow arguments
          #   These are the arguments that are used by the step-workflow
          "arguments": [

            # Structure for a file argument
            {
              # These are necessary fields
              "argument_name": "",
              "argument_type": "file",
              # Linking fields
              #   If no source step is specified,
              #   the argument will be matched to general arguments/initial input
              #   by source_argument_name or argument name if source_argument_name is missing
              #   If parameter or file value is case specific and need to come from
              #     workflow run set a placeholder like <whatever>.value
              #     where <whatever>.value means pick 'value' in 'whatever' section in
              #     current workflow run object
              "source_step": "",
              "source_argument_name": ""
              # These are optional fields
              #   It's possible to skip these fields or add custom ones
              "scatter": 0, # dimension to scatter list arguments if any
              "gather": 0, # increment for input dimension if previous steps were scattered
              "mount": False,
              "rename": "",
              "unzip": ""
            },

            # Structure for a parameter argument
            {
              # These are necessary fields
              "argument_name": "",
              "argument_type": "parameter",
              # These are optional fields
              #   If no value is specified,
              #   the argument will be matched to general arguments by source_argument_name
              #   or argument name if source_argument_name is missing
              "value": "",
              "source_argument_name": ""
            }

          ],

          # Step-workflow outputs
          "outputs": []
        }

      ]
    }
