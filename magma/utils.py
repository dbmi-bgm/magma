#!/usr/bin/env python3

################################################
#
#   Library of utilities to work with
#       meta-workflow and meta-workflow-run objects
#
#   Michele Berselli
#   berselli.michele@gmail.com
#
################################################

################################################
#   Libraries
################################################
import sys, os

# tibanna
from tibanna.utils import create_jobid

# dcicutils


################################################
#   Functions
################################################

################################################
#   Objects
################################################
class Argument(object):
    '''
    '''

    def __init__(self, input_json):
        '''
        '''
        # Basic attributes
        for key in input_json:
            setattr(self, key, input_json[key])
        #end for
        # Validate
        self._validate()
        # Calculated attributes
        if not getattr(self, 'source_argument_name', None):
            self.source_argument_name = self.argument_name
        #end if
    #end def

    def _validate(self):
        '''
        '''
        try:
            getattr(self, 'argument_name')
            getattr(self, 'argument_type')
        except AttributeError as e:
            raise ValueError('JSON validation error, {0}\n'
                                .format(e.args[0]))
        #end try
    #end def

#end class

class InputGenerator(object):
    '''
    '''

    def __init__(self, wfl_obj, wflrun_obj):
        '''

                wfl_obj, MetaWorkflow object representing a meta-workflow
                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
        '''
        # Basic attributes
        self.wfl_obj = wfl_obj
        self.wflrun_obj = wflrun_obj
    #end def

    def input_generator(self):
        '''
        '''
        for run_obj, run_args in self._input():
            jobid = create_jobid()
            # Update run status and jobid
            self.wflrun_obj.update_attribute(run_obj.shard_name, 'status', 'running')
            self.wflrun_obj.update_attribute(run_obj.shard_name, 'jobid', jobid)
            ### This is where formatting happens,
            #       to change formatting just change this part
            step_obj = self.wfl_obj.steps[run_obj.name]
            input_json = {
                'app_name': run_obj.name,
                'workflow_uuid': step_obj.uuid,
                'config': step_obj.config,
                'parameters': {},
                'input_files': [],
                'jobid': jobid
            }
            if getattr(step_obj, 'custom_pf_fields', None):
                input_json.setdefault('custom_pf_fields', step_obj.custom_pf_fields)
            #end if
            if getattr(step_obj, 'custom_qc_fields', None):
                input_json.setdefault('custom_qc_fields', step_obj.custom_qc_fields)
            #end if
            for arg_obj in run_args:
                if arg_obj.argument_type == 'parameter':
                    input_json['parameters'].setdefault(arg_obj.argument_name, arg_obj.value)
                else:
                    # Basic argument information
                    arg_ = {
                        'workflow_argument_name': arg_obj.argument_name,
                        'uuid': arg_obj.uuid
                    }
                    # Additional information
                    if getattr(arg_obj, 'mount', None):
                        arg_.setdefault('mount', arg_obj.mount)
                    #end if
                    if getattr(arg_obj, 'rename', None):
                        arg_.setdefault('rename', arg_obj.rename)
                    #end if
                    if getattr(arg_obj, 'unzip', None):
                        arg_.setdefault('unzip', arg_obj.unzip)
                    #end if
                    input_json['input_files'].append(arg_)
                #end if
            #end for
            yield input_json, self.wflrun_obj.runs_to_json()
        #end for
    #end def

    def _input(self):
        '''
        '''
        out_ = []
        # Get workflow-runs that need to be run
        for run_obj in self.wflrun_obj.to_run():
            # Get workflow-run arguments
            run_args = self._run_arguments(run_obj)
            # Match and update workflow-run arguments
            #   file arguments -> uuid
            #   parameter arguments -> value
            self._match_arguments(run_args, run_obj)
            out_.append((run_obj, run_args))
        #end for
        return out_
    #end def

    def _run_arguments(self, run_obj):
        '''
                run_obj, is a WorkflowRun object for a workflow-run
        '''
        run_args = []
        for arg in self.wfl_obj.steps[run_obj.name].arguments:
            arg_obj = Argument(arg)
            run_args.append(arg_obj)
        #end for
        return run_args
    #end def

    def _match_arguments(self, run_args, run_obj):
        '''
                run_args, is a list of Argument objects for a workflow-run
                run_obj, is a WorkflowRun object for a workflow-run
        '''
        for arg_obj in run_args:
            is_file = False
            # Check argument type
            if arg_obj.argument_type == 'file':
                is_file = True
                is_match = self._match_argument_file(arg_obj, run_obj)
            else: # is parameter
                is_match = self._match_argument_parameter(arg_obj)
            #end if
            if not is_match:
                raise ValueError('Value error, cannot find a match for argument {0}\n'
                                    .format(arg_obj.argument_name))
            #end if
            # Check Scatter
            if getattr(arg_obj, 'scatter', None):
                shard = map(int, run_obj.shard.split(':'))
                if is_file: in_ = arg_obj.uuid
                else: in_ = arg_obj.value
                #end if
                for idx in shard:
                    in_ = in_[idx]
                #end for
                if is_file: arg_obj.uuid = in_
                else: arg_obj.value = in_
                #end if
            #end if
        #end for
    #end def

    def _match_argument_file(self, arg_obj, run_obj):
        '''
        '''
        if getattr(arg_obj, 'source_step', None):
        # Is workflow-run dependency, match to workflow-run output
            uuid_ = []
            for dependency in run_obj.dependencies:
                if arg_obj.source_step in dependency:
                    for arg in self.wflrun_obj.runs[dependency].output:
                        if arg_obj.source_argument_name == arg['argument_name']:
                            uuid_.append(arg['uuid'])
                            break
                        #end if
                    #end for
                #end if
            #end for
            if len(uuid_) > 1:
                arg_obj.uuid = uuid_
            else:
                arg_obj.uuid = uuid_[0]
            #end if
            return True
        else:
        # No dependency, match to general argument
            return self._match_general_argument(arg_obj)
        #end if
    #end def

    def _match_argument_parameter(self, arg_obj):
        '''
        '''
        if getattr(arg_obj, 'value', None) != None:
            # Is value
            return True
        else:
        # No value, match to general argument
            return self._match_general_argument(arg_obj)
        #end if
    #end def

    def _match_general_argument(self, arg_obj):
        '''
        '''
        # Try and match with meta-worfklow-run input
        if self._value(arg_obj, self.wflrun_obj.input):
            return True
        #end if
        # No match, try match to default argument in meta-worfklow
        if self._value(arg_obj, self.wfl_obj.arguments):
            return True
        #end if
        return False
    #end def

    def _value(self, arg_obj, arg_list):
        '''
                arg_obj, is Argument object for a workflow-run
                arg_list, is a list of arguments as dictionaries
        '''
        for arg in arg_list:
            if arg_obj.source_argument_name == arg['argument_name'] and \
               arg_obj.argument_type == arg['argument_type']:
                if arg_obj.argument_type == 'file':
                    arg_obj.uuid = arg['uuid']
                else:
                    arg_obj.value = arg['value']
                #end if
                return True
            #end if
        #end for
        return False
    #end def

#end class

class CheckStatus(object):
    '''
    '''

    def __init__(self, wflrun_obj):
        '''

                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
        '''
        # Basic attributes
        self.wflrun_obj = wflrun_obj
        self.status_ = {
            'pending': 'pending',
            'running': 'running',
            'completed': 'completed',
            'failed': 'failed'
        }
    #end def

    def check_running(self): # We can maybe have a flag that switch between tibanna or dcic utils functions
        '''
        '''
        for run_obj in self.wflrun_obj.running():
            # Check current status from jobid
            status = function.check_status(run_obj.jobid)
            ###########

            if self.status_[status] == 'completed':

                # Get output
                output_ = function.get_output(run_obj.jobid)
                ###########
                # Pre-process and format output
                output = function.format_output(output_)
                ###########

                # Update run status and output
                self.wflrun_obj.update_attribute(run_obj.shard_name, 'status', 'completed')
                self.wflrun_obj.update_attribute(run_obj.shard_name, 'output', output)
                # Return the json to patch workflow_runs
                yield self.wflrun_obj.runs_to_json()
            elif self.status_[status] == 'running':
                continue
            else: # handle error status
                #TODO
                continue
            #end if
        #end for
    #end def
#end class
