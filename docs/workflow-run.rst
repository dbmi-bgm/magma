.. _meta-workflow-run-label:

=================
meta-workflow-run
=================

A meta-workflow-run is a json object that describes the structure of a multi-step workflow given the corresponding meta-workflow, specific input and defined end points.
Scatter, gather and dependencies information are used to create and link all the shards for individual step-workflows (workflow-runs) that are necessary to reach end points based on the input.

Structure
+++++++++

.. code-block:: python

    {
      ## General meta-workflow-run information
      "meta_workflow_uuid": "", # universally unique identifier
                                #   for the corresponding meta-workflow

      ## Workflow-runs for meta-workflow-run
      "workflow_runs" : [

            # Workflow-run structure
            {
              # These are necessary fields
              "name": "",
              "status": "", # pending | running | completed | failed
              "shard": "", # x 1D | x:x 2D | x:x:x 3D | ...
              # These are optional fields
              #   or fields created during the processing
              "dependencies": [],
              "output": [
                # Structure for a file argument,
                #   only type of argument that can be output of a workflow-run
                {
                  # These are necessary fields
                  "argument_name": "",
                  "uuid": ""
                }
              ],
              # Fields that are created to link the actual run
              "workflow_run_uuid": "",
              "jobid": ""
            },

            # Example
            { "name": "step1",
              "workflow_run_uuid": "uuid-step1:0-run",
              "status": "complete",
              "output": [
                {
                  "argument_name": "out_step1",
                  "uuid": "uuid-out_step1:0"
                }
              ],
              "shard": "0"
            },
            { "name": "step1",
              "workflow_run_uuid": "uuid-step1:1-run",
              "status": "complete",
              "output": [
                {
                  "argument_name": "out_step1",
                  "uuid": "uuid-out_step1:1"
                }
              ],
              "shard": "1"
            },
            { "name": "step2",
              "status": "running",
              "dependencies": ["step1:0"],
              "shard": "0"
            },
            { "name": "step2",
              "status": "running",
              "dependencies": ["step1:1"],
              "shard": "1"
            },
            { "name": "step3",
              "status": "pending",
              "dependencies": ["step2:0", "step2:1"],
              "shard": "0"
            }
      ],

      ## Specific input for meta-workflow-run
      "input": [
        # Structure for a file argument
        {
          # These are necessary fields
          "argument_name": "",
          "argument_type": "file",
          "uuid": ""
        },

        # Structure for a parameter argument
        {
          # These are necessary fields
          "argument_name": "",
          "argument_type": "parameter",
          "value": ""
        }
      ],

      ## Specific output for meta-workflow-run
      "output": []
    }
