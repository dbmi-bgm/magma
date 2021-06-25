#!/usr/bin/env python3

################################################
#
#   Object to combine and integrate
#       meta-workflow and meta-workflow-run objects
#       to generate input for workflow-runs
#
#   Michele Berselli
#   berselli.michele@gmail.com
#
################################################

################################################
#   Libraries
################################################
import sys, os
import re

# tibanna
from tibanna.utils import create_jobid

################################################
#   Argument
################################################
class Argument(object):
    """
        object to model an argument
    """

    def __init__(self, input_json):
        """
            initialize Argument object from input_json

                input_json is an argument in json format
        """
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
        """
        """
        try:
            getattr(self, 'argument_name')
            getattr(self, 'argument_type')
        except AttributeError as e:
            raise ValueError('JSON validation error, {0}\n'
                                .format(e.args[0]))
        #end try
    #end def

#end class

################################################
#   InputGenerator
################################################
class InputGenerator(object):
    """
        object to combine MetaWorkflow and MetaWorkflowRun objects
    """

    def __init__(self, wfl_obj, wflrun_obj):
        """
            initialize InputGenerator object

                wfl_obj, MetaWorkflow object representing a meta-workflow
                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
        """
        # Basic attributes
        self.wfl_obj = wfl_obj
        self.wflrun_obj = wflrun_obj
        # Key to use to access file value information
        #   in workflow-runs output
        self.file_key = 'files'
    #end def

    def input_generator(self):
        """
            template function, can be customized to any specific format

            return a generator to input for workflow-run in json format
            and updated workflow-runs and final_status information in json format for patching

            for each workflow-run ready to run:
                update workflow-run status to running
                create and add a jobid
                format input json
                create an updated workflow_runs json for patching
                yield input json and workflow_runs
        """
        for run_obj, run_args in self._input():
            jobid = create_jobid()
            # Update run status and jobid
            self.wflrun_obj.update_attribute(run_obj.shard_name, 'status', 'running')
            self.wflrun_obj.update_attribute(run_obj.shard_name, 'jobid', jobid)
            ### This is where formatting happens,
            #       to change formatting just change this part
            step_obj = self.wfl_obj.steps[run_obj.name]
            input_json = {
                'name': run_obj.name,
                'workflow': step_obj.workflow,
                'config': self._eval_formula(step_obj.config),
                'parameters': {},
                'input_files': [],
                'jobid': jobid
            }

            for arg_obj in run_args:
                if arg_obj.argument_type == 'parameter':
                    input_json['parameters'].setdefault(arg_obj.argument_name, arg_obj.value)
                else:
                    # Basic argument information
                    arg_ = {
                        'workflow_argument_name': arg_obj.argument_name,
                        'files': arg_obj.files
                    }
                    # Additional information
                    if getattr(arg_obj, 'mount', None):
                        arg_.setdefault('mount', arg_obj.mount)
                    #end if
                    if getattr(arg_obj, 'rename', None):
                        rname = arg_obj.rename
                        if isinstance(rname, str) and rname.startswith('formula:'):
                            frmla = rname.split('formula:')[-1]
                            rname = self._value_parameter(frmla, self.wflrun_obj.input)
                        #end if
                        arg_.setdefault('rename', rname)
                    #end if
                    if getattr(arg_obj, 'unzip', None):
                        arg_.setdefault('unzip', arg_obj.unzip)
                    #end if
                    input_json['input_files'].append(arg_)
                #end if
            #end for
            yield input_json, {'final_status':  self.wflrun_obj.update_status(),
                               'workflow_runs': self.wflrun_obj.runs_to_json()}
        #end for
    #end def

    def _eval_formula(self, dit):
        """
            replace formulas in dit with corresponding calculated values
            detect formula as "formula:<formula>"

                dit, dictionary to evaluate for formulas
        """
        d_ = {}
        for k, v in dit.items():
            if isinstance(v, str) and v.startswith('formula:'):
                frmla = v.split('formula:')[-1]
                # match parameters
                re_match = re.findall('([a-zA-Z_]+)', frmla)
                # replace parameters
                for s in re_match:
                    try:
                        val = self._value_parameter(s, self.wflrun_obj.input)
                        frmla = frmla.replace(s, str(val))
                    except Exception:
                        pass
                    #end try
                #end for
                v = eval(frmla)
            #end if
            d_.setdefault(k, v)
        #end for
        return d_
    #end def

    def _input(self):
        """
        """
        out_ = []
        # Get workflow-runs that need to be run
        for run_obj in self.wflrun_obj.to_run():
            # Get workflow-run arguments
            run_args = self._run_arguments(run_obj)
            # Match and update workflow-run arguments
            #   file arguments -> files
            #   parameter arguments -> value
            self._match_arguments(run_args, run_obj)
            out_.append((run_obj, run_args))
        #end for
        return out_
    #end def

    def _run_arguments(self, run_obj):
        """
                run_obj, is a WorkflowRun object for a workflow-run
        """
        run_args = []
        for arg in self.wfl_obj.steps[run_obj.name].input:
            arg_obj = Argument(arg)
            run_args.append(arg_obj)
        #end for
        return run_args
    #end def

    def _match_arguments(self, run_args, run_obj):
        """
                run_args, is a list of Argument objects for a workflow-run
                run_obj, is a WorkflowRun object for a workflow-run
        """
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
                raise ValueError('Value error, cannot find a match for argument "{0}"\n'
                                    .format(arg_obj.argument_name))
            #end if
            # Check Scatter
            if getattr(arg_obj, 'scatter', None):
                shard = map(int, run_obj.shard.split(':'))
                if is_file: in_ = arg_obj.files
                else: in_ = arg_obj.value
                #end if
                for idx in list(shard)[:arg_obj.scatter]:
                    # [:arg_obj.scatter] handle multiple scatter in same shard,
                    #   use scatter dimension to subset shard index list
                    in_ = in_[idx]
                #end for
                if is_file: arg_obj.files = in_
                else: arg_obj.value = in_
                #end if
            #end if
        #end for
    #end def

    def _match_argument_file(self, arg_obj, run_obj):
        """
                arg_obj, is Argument object for a workflow-run
                run_obj, is a WorkflowRun object for a workflow-run
        """
        if getattr(arg_obj, 'source', None):
        # Is workflow-run dependency, match to workflow-run output
            file_ = []
            for dependency in run_obj.dependencies:
                if arg_obj.source == dependency.split(':')[0]:
                    for arg in self.wflrun_obj.runs[dependency].output:
                        if arg_obj.source_argument_name == arg['argument_name']:
                            file_.append(arg[self.file_key])
                            break
                        #end if
                    #end for
                #end if
            #end for
            gather = getattr(arg_obj, 'gather', 0)
            if gather == 1:  # gather 1 dimension
                arg_obj.files = file_
            elif gather > 1:  # gather 2 dimensions
                arg_obj.files = file_
                for i in range(1, gather):
                    arg_obj.files = [arg_obj.files]
            else:  # no gather
                arg_obj.files = file_[0]
            #end if
            return True
        else:
        # No dependency, match to general argument
            return self._match_general_argument(arg_obj)
        #end if
    #end def

    def _match_argument_parameter(self, arg_obj):
        """
                arg_obj, is Argument object for a workflow-run
        """
        if getattr(arg_obj, 'value', None) != None:
            # Is value
            return True
        else:
        # No value, match to general argument
            return self._match_general_argument(arg_obj)
        #end if
    #end def

    def _match_general_argument(self, arg_obj):
        """
                arg_obj, is Argument object for a workflow-run
        """
        # Try and match with meta-worfklow-run input
        if self._value(arg_obj, self.wflrun_obj.input):
            return True
        #end if
        # No match, try match to default argument in meta-worfklow
        if self._value(arg_obj, self.wfl_obj.input):
            return True
        #end if
        return False
    #end def

    def _value(self, arg_obj, arg_list):
        """
                arg_obj, is Argument object for a workflow-run
                arg_list, is a list of arguments as dictionaries
        """
        for arg in arg_list:
            if arg_obj.source_argument_name == arg['argument_name'] and \
               arg_obj.argument_type == arg['argument_type']:
                if arg_obj.argument_type == 'file':
                    arg_obj.files = arg['files']
                else:
                    arg_obj.value = arg['value']
                #end if
                return True
            #end if
        #end for
        return False
    #end def

    def _value_parameter(self, arg_name, arg_list):
        """
                arg_name, is the name of the argument
                arg_list, is a list of arguments as dictionaries to match
        """
        for arg in arg_list:
            if arg_name == arg['argument_name'] and \
               arg['argument_type'] == 'parameter':
                return arg['value']
            #end if
        #end for
        raise ValueError('Value error, cannot find a match for parameter "{0}"\n'
                            .format(arg_name))
    #end def

#end class
