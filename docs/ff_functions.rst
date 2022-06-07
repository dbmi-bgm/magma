============
FF_functions
============

create_metawfr
**************

The class ``MetaWorkflowRunFromSampleProcessing(sp_uuid<str>, metawf_uuid<str>, ff_key<key>, expect_family_structure=True)`` can be used to create a MetaWorkflowRun[json] from a *SampleProcessing* object on the portal.
Initializing the class will automatically create the correct MetaWorkflowRun[json] that will be stored as attribute.
The method ``post_and_patch()`` can be used to post the MetaWorkflowRun[json] as *MetaWorkflowRun* object on the portal and patch it to the *SampleProcessing*.

The class ``MetaWorkflowRunFromSample`` works similarly for *Sample* portal items.

.. code-block:: python

    from magma_ff import create_metawfr

    # UUIDs
    metawf_uuid = '' # uuid for MetaWorkflow[portal]
    sp_uuid = '' # uuid for SampleProcessing[portal]

    # ff_key
    #   authorization key for the portal

    # expect_family_structure
    #   True | False
    #   if True a family structure is expected,
    #     samples are sorted for a trio analysis

    # Create MetaWorkflowRunFromSampleProcessing object
    #   this will automatically create the correct MetaWorkflowRun[json]
    #   and store it as .meta_workflow_run attribute
    create_metawfr_obj = create_metawfr.MetaWorkflowRunFromSampleProcessing(sp_uuid, metawf_uuid, ff_key, expect_family_structure=True)

    # Post and patch the MetaWorkflowRun[json] on the portal
    create_metawfr_obj.post_and_patch()



run_metawfr
***********

The function ``run_metawfr(metawfr_uuid<str>, ff_key<key>, verbose=False, sfn='tibanna_zebra', env='fourfront-cgap', maxcount=None, valid_status=None)`` can be used to run a *MetaWorkflowRun* object on the portal.
Calculates which shards are ready to run, starts the runs with Tibanna and patches the metadata.

.. code-block:: python

    from magma_ff import run_metawfr

    # UUIDs
    metawfr_uuid = '' # uuid for MetaWorkflowRun[portal]

    # ff_key
    #   authorization key for the portal

    # env
    #   environment to use to access metadata
    env = 'fourfront-cgap'

    # sfn
    #   step function to use for Tibanna
    sfn = 'tibanna_zebra'

    run_metawfr.run_metawfr(metawfr_uuid, ff_key, verbose=False, sfn=sfn, env=env, maxcount=None, valid_status=None)


status_metawfr
**************

The function ``status_metawfr(metawfr_uuid<str>, ff_key<key>, verbose=False, env='fourfront-cgap', valid_status=None)`` can be used to check and patch status for a *MetaWorkflowRun* object on the portal.
Updates the status to ``completed`` or ``failed`` for finished runs. Updates *MetaWorkflowRun* final status accordingly. Patches the metadata.

.. code-block:: python

    from magma_ff import status_metawfr

    # UUIDs
    metawfr_uuid = '' # uuid for MetaWorkflowRun[portal]

    # ff_key
    #   authorization key for the portal

    # env
    #   environment to use to access metadata
    env = 'fourfront-cgap'

    status_metawfr.status_metawfr(metawfr_uuid, ff_key, verbose=False, env=env, valid_status=None)


update_cost_metawfr
*******************

The function ``update_cost_metawfr(metawfr_uuid<str>, ff_key<key>, verbose=False)`` can be used to compute and patch the cost for a *MetaWorkflowRun* object on the portal (includes failed runs).

.. code-block:: python

    from magma_ff import update_cost_metawfr

    # UUIDs
    metawfr_uuid = '' # uuid for MetaWorkflowRun[portal]

    # ff_key
    #   authorization key for the portal

    update_cost_metawfr.update_cost_metawfr(metawfr_uuid, ff_key, verbose=False)


import_metawfr
**************

The function ``import_metawfr(metawf_uuid<str>, metawfr_uuid<str>, sp_uuid<str>, steps_name<str list>, ff_key<key>, post=False, verbose=False, expect_family_structure=True)`` can be used to create a new MetaWorkflowRun[json] from a older *MetaWorkflowRun* object on the portal.
The specified *SampleProcessing* is used to create the basic structure for the new MetaWorkflowRun[json]. The steps listed in ``steps_name`` are then imported from the older *MetaWorkflowRun* object specified as ``metawfr_uuid``.
Returns the new MetaWorkflowRun[json].
Can automatically post the new MetaWorkflowRun[json] as *MetaWorkflowRun* object on the portal and patch it to the *SampleProcessing*.

.. code-block:: python

    from magma_ff import import_metawfr

    # UUIDs
    metawf_uuid = '' # uuid for MetaWorkflow[portal]
    metawfr_uuid = '' # uuid for old MetaWorkflowRun[portal] to import
    sp_uuid = '' # uuid for SampleProcessing[portal]

    # Post
    #   post=True to automatically post new MetaWorkflowRun[json] object on the portal

    # ff_key
    #   authorization key for the portal

    # steps_name
    steps_name = ['workflow_granite-mpileupCounts', 'workflow_gatk-ApplyBQSR-check']

    metawfr_json = import_metawfr.import_metawfr(metawf_uuid, metawfr_uuid, sp_uuid, steps_name, ff_key, expect_family_structure=True)


reset_metawfr
*************

The function ``reset_status(metawfr_uuid<str>, status<str | str list>, step_name<str | str list>, ff_key<key>, verbose=False, valid_status=None)`` can be used to re-set runs for a *MetaWorkflowRun* object on the portal that correspond to step/steps specified as ``step_name`` and with status in ``status``.

.. code-block:: python

    from magma_ff import reset_metawfr

    # UUIDs
    metawfr_uuid = '' # uuid for MetaWorkflowRun[portal]

    # ff_key
    #   authorization key for the portal

    # step_name
    #   name or list of names for steps that need to be reset
    step_name = ['workflow_granite-mpileupCounts', 'workflow_gatk-ApplyBQSR-check']

    # status
    #   status or list of status to reset
    status = 'failed' # running | completed | failed

    reset_metawfr.reset_status(metawfr_uuid, status, step_name, ff_key, verbose=False, valid_status=None)


The function ``reset_all(metawfr_uuid<str>, ff_key<key>, verbose=False, valid_status=None)`` can be used to re-set all runs for a *MetaWorkflowRun* object on the portal.

.. code-block:: python

    from magma_ff import reset_metawfr

    # UUIDs
    metawfr_uuid = '' # uuid for MetaWorkflowRun[portal]

    # ff_key
    #   authorization key for the portal

    reset_metawfr.reset_all(metawfr_uuid, ff_key, verbose=False, valid_status=None)


The function ``reset_failed(metawfr_uuid<str>, ff_key<key>, verbose=False, valid_status=None)`` can be used to re-set all runs for a *MetaWorkflowRun* object on the portal with status ``failed``.

.. code-block:: python

    from magma_ff import reset_metawfr

    # UUIDs
    metawfr_uuid = '' # uuid for MetaWorkflowRun[portal]

    # ff_key
    #   authorization key for the portal

    reset_metawfr.reset_failed(metawfr_uuid, ff_key, verbose=False, valid_status=None)
