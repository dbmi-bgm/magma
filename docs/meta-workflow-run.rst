.. _meta-workflow-run-label:

=====================
MetaWorkflowRun[json]
=====================

*MetaWorkflowRun[json]* is a json data format that describes the structure of a multi-step workflow given the corresponding *MetaWorkflow[json]*, specific input and defined end points.
Scatter, gather and dependencies information are used to create and link all the *shards* for individual steps (*WorkflowRun[json]*) that are necessary to reach end points based on the input.

Structure
+++++++++

.. code-block:: python

    {
      ## General MetaWorkflowRun[json] information
      "meta_workflow": "", # universally unique identifier
                           #   for the corresponding MetaWorkflow[json]

      ## Shards for MetaWorkflowRun[json]
      "workflow_runs" : [

            # WorkflowRun[json] structure
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
                #   only type of argument that can be output of a WorkflowRun[json]
                {
                  # These are necessary fields
                  "argument_name": "",
                  "files": ""
                }
              ],
              # Additonal fields created to link the actual run
              "jobid": "", # run identifier
              "workflow_run": # universally unique identifier
            },

            # Example
            { "name": "step1",
              "workflow_run": "uuid-step1:0-run",
              "status": "complete",
              "output": [
                {
                  "argument_name": "out_step1",
                  "files": "uuid-out_step1:0"
                }
              ],
              "shard": "0"
            },
            { "name": "step1",
              "workflow_run": "uuid-step1:1-run",
              "status": "complete",
              "output": [
                {
                  "argument_name": "out_step1",
                  "files": "uuid-out_step1:1"
                }
              ],
              "shard": "1"
            },
            { "name": "step2",
              "workflow_run": "uuid-step2:0-run",
              "status": "running",
              "dependencies": ["step1:0"],
              "shard": "0"
            },
            { "name": "step2",
              "workflow_run": "uuid-step2:1-run",
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

      ## Specific input for MetaWorkflowRun[json]
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
      ],

      ## Final status
      "final_status": "", # pending | running | completed | failed

      ## Optional general fields for MetaWorkflowRun[json]
      "common_fields": {}
    }
