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
      "name": "", # name for the meta-workflow
      "uuid": "", # universally unique identifier

      ## General meta-workflow arguments
      #   These are general arguments that are used by multiple steps
      #   and can be grouped here to avoid repetition
      "input": [

        # Structure for a file argument
        {
          # These are necessary fields
          "argument_name": "",
          "argument_type": "file",
          "files": ""
        },

        # Structure for a parameter argument
        {
          # These are necessary fields
          "argument_name": "",
          "argument_type": "parameter",
          "value": ""
        }

        # Arguments with no value or file uuid can be specified as well
        #   and will need to be provided as input in meta-worfklow-run

      ],

      ## Steps for the meta-workflow
      "workflows": [

        # Structure for step-workflow
        {
          # General step-workflow information
          #   These are general fields that are required by the parser,
          #   however, there is no check on the content that can be customized
          "name": "", # name for the step-workflow
          "workflow": "", # universally unique identifier
          "config": { # configuration for the step-workflow
            "instance_type": "",
            "ebs_size": "",
                  # !!! it is possible to specify formulas "formula:<formula>"
                  #   values to be replaced must be defined as
                  #   parameter arguments in meta-worfklow-run specific input !!!
            "EBS_optimized": True,
            "spot_instance": True,
            "log_bucket": "tibanna-output",
            "run_name": "run_",
            "behavior_on_capacity_limit": "wait_and_retry"
          },

          # Additional step-workflow information
          #   Optional fields can be added and customized
          "dependencies": [], # allows to force general dependencies to steps

          # Example of additional Tibanna specific fields
          "custom_pf_fields": {},
          "custom_qc_fields": {},

          # Step-workflow arguments
          #   These are the arguments that are used by the step-workflow
          "input": [

            # Structure for a file argument
            {
              # These are necessary fields
              "argument_name": "",
              "argument_type": "file",

              # Linking fields
              #   These are optional fields
              #   If no source step is specified,
              #     the argument will be matched to general arguments by source_argument_name
              #     or argument_name if source_argument_name is missing
              #   First will try to match to argument in meta-worfklow-run specific input
              #     if no match is found will try to match to meta-workflow default argument
              "source": "", # step that is source
              "source_argument_name": "",

              # Input dimension
              #   These are optional fields that can be used to change input argument dimension
              "scatter": 2, # dimension to scatter list argument if any
              "gather": 1, # increment for input argument dimension if previous steps were scattered

              # These are optional fields
              #   It is possible to skip these fields or add custom ones
              "mount": True,
              "rename": "formula:<parameter_name>",
                    #  !!! formula:<parameter_name> can be used to
                    #    specify a parameter name that need to be matched
                    #    to parameter argument in meta-worfklow-run specific input
                    #    and the value replaced !!!
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

          ]
        }
      ]
    }
