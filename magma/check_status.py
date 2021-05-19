class AbstractCheckStatus(object):
    '''
    Skeleton for CheckStatus Classes
    '''

    def __init__(self, wflrun_obj):
        '''

                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
        '''
        # Basic attributes
        self.wflrun_obj = wflrun_obj

    @property
    def status_map(self):
        """Mapping from get_status output to Magma status.
        Set to property so that inherited classes can overwrite it."""
        return {
            'pending': 'pending',
            'running': 'running',
            'completed': 'completed',
            'failed' : 'failed'
        }

    def check_running(self): # We can maybe have a flag that switch between tibanna or dcic utils functions
        '''
        '''
        for run_obj in self.wflrun_obj.running():

            # Check current status from jobid
            status_ = self.get_status(run_obj.jobid)
            status = self.status_map[status_]

            # Update run status no matter what
            self.wflrun_obj.update_attribute(run_obj.shard_name, 'status', status)

            if status == 'completed':

                # Get formatted output
                output = self.get_output(run_obj.jobid)

                # Update output only if it the run succeeded
                self.wflrun_obj.update_attribute(run_obj.shard_name, 'output', output)

            elif status == 'running':
                yield None  # yield None so that it doesn't terminate iteration
                continue
            else:  # failed
                # handle error status - anything to do before yielding the updated json.
                self.handle_error(run_obj)
            #end if

            # Return the json to patch workflow_runs for both completed and failed
            # and keep going so that it can continue updating status for other runs.
            yield self.wflrun_obj.runs_to_json()
        #end for
    #end def

    # inherited classes could define stuff to do with an error case.
    def handle_error(self, run_obj):
        pass

    # replace them with real functions for getting status or (formatted) output
    def get_status(self, jobid):
        return 'running'

    def get_output(self, jobid):
        return [{'workflow_argument_name': 'arg', 'uuid': 'uuid'}]
#end class
