========================================
check_status and check_status_ff modules
========================================


Check Status and Output
************************

.. code-block:: python

    from magma import check_status_ff

    wflrun_obj = None  # or any actual wflrun_obj

    # create a CheckStatus object with an environment name
    cs = check_status_ff.CheckStatusFF(wflrun_obj, env='fourfront-cgap')

    # get_status funtion can be used as stand-alone
    cs.get_status('RBwlMTyOWvpZ')
    #'complete'

    # get_output funtion can be used as stand-alone
    cs.get_output('RBwlMTyOWvpZ')
    #[{'workflow_argument_name': 'sorted_bam',
    #  'uuid': '07ae8a9c-260e-4b1b-80ae-ae59a624d746'}]

    # create a generator for check_running
    cr = cs.check_running()

    # iterate (needs a non-empty wflrun_obj for this to work)
    next(cr)
