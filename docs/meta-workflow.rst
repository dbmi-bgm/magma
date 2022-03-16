.. _meta-workflow-label:

==================
MetaWorkflow[json]
==================

*MetaWorkflow[json]* is a data format that describes the general structure of a multi-step workflow in json format.
The format can store general information for the workflow (e.g. reference files, shared arguments, ...), as well as specific information for each of the steps (*StepWorkflow[json]*).
The format is very flexible and allows to extend the minimal set of keys that are required with any number of custom keys.

Structure
+++++++++

.. code-block:: python

    {
      ## General MetaWorkflow[json] information
      #   These are general fields that are required by the parser,
      #   however, there is no check on the content that can be customized
      "name": <str>, # name for the MetaWorkflow[json]
      "uuid": <str>, # universally unique identifier

      ## General MetaWorkflow[json] arguments
      #   These are general arguments that are used by multiple steps
      #   and can be grouped here to avoid repetition
      "input": [

        # Structure for a file argument
        {
          # These are necessary fields
          "argument_name": <str>,
          "argument_type": "file",
          "files": <...>
          # These are optional fields
          #   It is possible to skip these fields or add custom ones
          "dimensionality": <int>
        },

        # Structure for a parameter argument
        {
          # These are necessary fields
          "argument_name": <str>,
          "argument_type": "parameter",
          "value": <...>
          # These are optional fields
          #   It is possible to skip these fields or add custom ones
          "value_type": <str>
        }

        # Arguments with no value or file uuid can be specified as well
        #   and will need to be provided as input in MetaWorkflowRun[json]

      ],

      ## Steps for the MetaWorkflow[json]
      "workflows": [

        # Structure for StepWorkflow[json]
        {
          # General StepWorkflow[json] information
          #   These are general fields that are required by the parser,
          #   however, there is no check on the content that can be customized
          "name": <str>, # name for the StepWorkflow[json]
          "workflow": <str>, # universally unique identifier
          "config": { # configuration for the StepWorkflow[json]
            # example for AWS and Tibanna
            "instance_type": <str>,
            "ebs_size": <str> | "formula:<formula>",
                  # !!! it is possible to specify formulas "formula:<formula>"
                  #   values to be replaced must be defined as
                  #   parameter arguments in MetaWorkflowRun[json] specific input !!!
            "EBS_optimized": <bool>,
            "spot_instance": <bool>,
            "log_bucket": <str>,
            "run_name": <str>,
            "behavior_on_capacity_limit": <str>
          },

          # Additional StepWorkflow[json] information
          #   Optional fields can be added and customized
          "dependencies": [], # allows to force general dependencies to steps

          # Example of additional Tibanna specific fields
          "custom_pf_fields": {
            # example for CGAP data model
            "<filename>": {
                 "file_type": <str>,
                 "variant_type": <str>,
                 "description": <str>,
                 "linkto_location": [<str>, ...]
             }
          },
          "custom_qc_fields": {},

          # StepWorkflow[json] arguments
          #   These are the arguments that are used by the StepWorkflow[json]
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
              #   First will try to match to argument in MetaWorkflowRun[json] specific input
              #     if no match is found will try to match to MetaWorkflow[json] default argument
              "source": <str>, # step that is source
              "source_argument_name": <str>,

              # Input dimension
              #   These are optional fields to specify input argument dimensions to use
              #     when creating the MetaWorkflowRun[json] or step specific inputs
              "scatter": <int>, # input argument dimension to use to scatter the step
                                #   !!! this will create multiple shards in the MetaWorkflowRun[json] structure !!!
                                #   the same dimension will be used to subset the input argument when creating the step specific input
              "gather": <int>, # increment for input argument dimension when gathering from previous steps
                               #   !!! this will collate multiple shards in the MetaWorkflowRun[json] structure !!!
                               #   the same increment in dimension will be used when creating the step specific input
              "input_dimension": <int>, # additional dimension used to subset the input argument when creating the step specific input
                                        #   this will be applied on top of scatter, if any, and will only affect the step specific input
                                        #   !!! this will not affect scatter dimension in building the MetaWorkflowRun[json] structure !!!
              "extra_dimension": <int>, # additional increment to dimension used when creating the step specific input
                                        #   this will be applied on top of gather, if any, and will only affect the step specific input
                                        #   !!! this will not affect gather dimension in building the MetaWorkflowRun[json] structure !!!
              # These are optional fields
              #   It is possible to skip these fields or add custom ones
              "mount": <bool>,
              "rename": "formula:<parameter_name>",
                    #  !!! formula:<parameter_name> can be used to
                    #    specify a parameter name that need to be matched
                    #    to parameter argument in MetaWorkflowRun[json] specific input
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
              #   First will try to match to argument in MetaWorkflowRun[json] specific input
              #     if no match is found will try to match to MetaWorkflow[json] default argument
              "value": <...>,
              "source_argument_name": <str>
            }

          ]
        }
      ]
    }
