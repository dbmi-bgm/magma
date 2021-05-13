.. _meta-workflow-label:

=============
meta-workflow
=============

A meta-workflow is a json object that describes the general structure of a multi-step workflow.
Stores general information for the multi-step workflow, as well as specific information for each of the workflows that is step of the meta-workflow (step-workflow).

Structure
+++++++++

.. code-block:: python

    {
      ## General meta-workflow information
      #   These are general fields that are required by the parser,
      #   however, there is no check on the content that can be customized
      "accession": "", # custom unique identifier
      "app_name": "", # name for the meta-workflow
      "app_version": "", # version for the meta-workflow
      "uuid": "", # universally unique identifier

      ## General meta-workflow arguments
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
          #   It is possible to skip these fields or add custom ones
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
          "custom_pf_fields": { #
            "<argument_name>": {
              "file_type": "",
              "description": ""
            }
          },
          "custom_qc_fields": { #
            "<argument_name>": {
              "file_type": "",
              "description": ""
            }
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
              #     the argument will be matched to general arguments by source_argument_name
              #     or argument_name if source_argument_name is missing
              #   First will try to match to argument in meta-worfklow-run specific input
              #     if no match is found will try to match to meta-workflow default argument
              "source_step": "",
              "source_argument_name": "",
              # These are optional fields
              #   It is possible to skip these fields or add custom ones
              "scatter": 2, # dimension to scatter list arguments if any
              "gather": 1, # increment for input dimension if previous steps were scattered
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
              #     the argument will be matched to general arguments by source_argument_name
              #     or argument_name if source_argument_name is missing
              #   First will try to match to argument in meta-worfklow-run specific input
              #     if no match is found will try to match to meta-workflow default argument
              "value": "",
              "source_argument_name": ""
            }

          ],

          # Step-workflow outputs
          "outputs": []
        }

      ]
    }
