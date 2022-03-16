===============================
*checkstatus* module (magma_ff)
===============================

Check Status and Output
***********************

.. code-block:: python

    from magma_ff import checkstatus

    wflrun_obj = None  # or any actual wflrun_obj
    job_id = 'RBwlMTyOWvpZ' # JOBID for the run

    # Create a CheckStatus object with an environment name
    cs_obj = checkstatus.CheckStatusFF(wflrun_obj, env='fourfront-cgap')

    # get_status funtion can be used as stand-alone
    cs_obj.get_status(job_id)
    #'complete'

    # get_output funtion can be used as stand-alone
    cs_obj.get_output(job_id)
    #[{'workflow_argument_name': 'sorted_bam',
    #  'uuid': '07ae8a9c-260e-4b1b-80ae-ae59a624d746'}]

    # Create a generator for check_running
    cr = cs_obj.check_running()

    # Iterate (needs a non-empty wflrun_obj for this to work)
    next(cr)
