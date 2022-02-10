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
      "name": <str>, # name for the meta-workflow
      "uuid": <str>, # universally unique identifier

      ## General meta-workflow arguments
      #   These are general arguments that are used by multiple steps
      #   and can be grouped here to avoid repetition
      "input": [

        # Structure for a file argument
        {
          # These are necessary fields
          "argument_name": <str>,
          "argument_type": "file",
          "files": <...>
        },

        # Structure for a parameter argument
        {
          # These are necessary fields
          "argument_name": <str>,
          "argument_type": "parameter",
          "value": <...>
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
          "name": <str>, # name for the step-workflow
          "workflow": <str>, # universally unique identifier
          "config": { # configuration for the step-workflow
            "instance_type": <str>,
            "ebs_size": <str> | "formula:<formula>",
                  # !!! it is possible to specify formulas "formula:<formula>"
                  #   values to be replaced must be defined as
                  #   parameter arguments in meta-worfklow-run specific input !!!
            "EBS_optimized": <bool>,
            "spot_instance": <bool>,
            "log_bucket": <str>,
            "run_name": <str>,
            "behavior_on_capacity_limit": <str>
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
              "argument_name": <str>,
              "argument_type": "file",

              # Linking fields
              #   These are optional fields
              #   If no source step is specified,
              #     the argument will be matched to general arguments by source_argument_name
              #     or argument_name if source_argument_name is missing
              #   First will try to match to argument in meta-worfklow-run specific input
              #     if no match is found will try to match to meta-workflow default argument
              "source": <str>, # step that is source
              "source_argument_name": <str>,

              # Input dimension
              #   These are optional fields to specify input argument dimensions to use
              #     when creating the meta-worfklow-run or step specific inputs
              "scatter": <int>, # input argument dimension to use to scatter the step
                                #   !!! this will create multiple shards in the meta-worfklow-run structure !!!
                                #   the same dimension will be used to subset the input argument when creating the step specific input
              "gather": <int>, # increment for input argument dimension when gathering from previous steps
                               #   !!! this will collate multiple shards in the meta-worfklow-run structure !!!
                               #   the same increment in dimension will be used when creating the step specific input
              "input_dimension": <int>, # additional dimension used to subset the input argument when creating the step specific input
                                        #   this will be applied on top of scatter, if any, and will only affect the step specific input
                                        #   !!! this will not affect scatter dimension in building the meta-worfklow-run structure !!!
              "extra_dimension": <int>, # additional increment to dimension used when creating the step specific input
                                        #   this will be applied on top of gather, if any, and will only affect the step specific input
                                        #   !!! this will not affect gather dimension in building the meta-worfklow-run structure !!!
              # These are optional fields
              #   It is possible to skip these fields or add custom ones
              "mount": <bool>,
              "rename": "formula:<parameter_name>",
                    #  !!! formula:<parameter_name> can be used to
                    #    specify a parameter name that need to be matched
                    #    to parameter argument in meta-worfklow-run specific input
                    #    and the value replaced !!!
              "unzip": <str>
            },

            # Structure for a parameter argument
            {
              # These are necessary fields
              "argument_name": <str>,
              "argument_type": "parameter",

              # These are optional fields
              #   If no value is specified,
              #     the argument will be matched to general arguments by source_argument_name
              #     or argument_name if source_argument_name is missing
              #   First will try to match to argument in meta-worfklow-run specific input
              #     if no match is found will try to match to meta-workflow default argument
              "value": <...>,
              "source_argument_name": <str>
            }

          ]
        }
      ]
    }
