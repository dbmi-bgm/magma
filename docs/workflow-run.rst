.. _workflow-run-label:

============
workflow-run
============

A workflow-run is a json object that describes a workflow structure for a specific input and defined end points given a meta-workflow.
Scatter, gather and dependencies information are used to create and link all the shards for individual steps that are necessary to reach end points based on the input.

Structure
+++++++++

.. code-block:: python

    {
      ## General workflow-run information
      "meta_workflow_uuid": "", # universally unique identifier for the corresponding meta-worklow

      ## Steps for workflow-run
      "workflow_runs" : [

            # Step structure
            { "name": "step1",
              "workflow_run_uuid": "", # universally unique identifier for the actual run
              "status": "running",
              "output": [],
              "dependencies": [], # step shards that are dependencies
              "shard": "0" # step shard
            },

            { "name": "step1",
              "workflow_run_uuid": "",
              "status": "complete",
              "output": [],
              "dependencies": [],
              "shard": "1"
            },
            { "name": "step2",
              "workflow_run_uuid": "",
              "status": "pending",
              "output": [],
              "dependencies": ["step1:0"],
              "shard": "0"
            },
            { "name": "step2",
              "workflow_run_uuid": "",
              "status": "pending",
              "output": [],
              "dependencies": ["step1:1"],
              "shard": "1"
            },
            { "name": "step3",
              "workflow_run_uuid": "",
              "status": "pending",
              "output": [],
              "dependencies": ["step2:0", "step2:1"],
              "shard": "0"
            }
      ]

      ## Specific input for workflow-run
      "input": []

      ## Specific output for workflow-run
      "output": []
    }
