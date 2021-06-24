============
FF_functions
============

run_metawfr
***********

Function to run workflow-runs in meta-workflow-run.
Calculates which workflow-runs are ready to run, starts the run with tibanna and patch the metadata.

``run_metawfr(metawfr_uuid<str>, ff_key<key>, verbose=False, sfn='tibanna_zebra', env='fourfront-cgap')``

.. code-block:: python



status_metawfr
**************

Function to check and patch status for workflow-runs in meta-workflow-run that are running.

``status_metawfr(metawfr_uuid<str>, ff_key<key>, verbose=False, env='fourfront-cgap')``

.. code-block:: python



import_metawfr
**************

Function to import metadata for meta-workflow-run from a different meta-workflow-run.

``import_metawfr(metawf_uuid<str>, metawfr_uuid<str>, case_uuid<str>, steps_name<str list>, create_metawfr<function>, ff_key<key>, post=False, verbose=False)``

.. code-block:: python
