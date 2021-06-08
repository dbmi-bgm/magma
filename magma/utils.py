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
import re

# tibanna
from tibanna.utils import create_jobid


################################################
#   Functions
################################################

################################################
#   Objects
################################################
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
    #end def

    def input_generator(self):
        """
            return a generator to input for workflow-run in json format
            and updated workflow-runs and final_status information in json format for patching

            for each workflow-run ready to run:
                update workflow-run status to running
                create and add a jobid
                format input json for tibanna zebra
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
                'app_name': run_obj.name,
                'workflow_uuid': step_obj.workflow,
                'config': self._eval_formula(step_obj.config),
                'parameters': {},
                'input_files': [],
                'jobid': jobid
            }

            ####################################################################
            # This should either come from run_obj or step_obj
            #     at some point add that option and possibly merge the two
            if getattr(step_obj, 'custom_pf_fields', None):
                input_json.setdefault('custom_pf_fields', step_obj.custom_pf_fields)
            #end if
            if getattr(step_obj, 'custom_qc_fields', None):
                input_json.setdefault('custom_qc_fields', step_obj.custom_qc_fields)
            #end if
            ####################################################################

            ####################################################################
            # This should either come from wflrun_obj or wfl_obj
            #     at some point add that option and possibly merge the two
            if getattr(self.wflrun_obj, 'common_fields', None):
                input_json.setdefault('common_fields', self.wflrun_obj.common_fields)
            #end if
            ####################################################################

            for arg_obj in run_args:
                if arg_obj.argument_type == 'parameter':
                    input_json['parameters'].setdefault(arg_obj.argument_name, arg_obj.value)
                else:
                    # Basic argument information
                    arg_ = {
                        'workflow_argument_name': arg_obj.argument_name,
                        'uuid': arg_obj.files
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
                    val = self._value_parameter(s, self.wflrun_obj.input)
                    frmla = frmla.replace(s, str(val))
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
                            file_.append(arg['files'])
                            break
                        #end if
                    #end for
                #end if
            #end for
            if len(file_) > 1:
                arg_obj.files = file_
            else:
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

################################################
#   RunUpdate
################################################
class RunUpdate(object):
    """
        object to handle MetaWorkflowRun and WorkflowRun update
    """

    def __init__(self, wflrun_obj):
        """
            initialize RunUpdate object

                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
        """
        # Basic attributes
        self.wflrun_obj = wflrun_obj
    #end def

    def reset_steps(self, steps_name):
        """
            reset WorkflowRun objects with name in steps_name
            return updated workflow-runs and final_status information as json

                steps_name, list of names for step-workflows that need to be reset
        """
        for name in steps_name:
            self.wflrun_obj.reset_step(name)
        #end for
        return {'final_status':  self.wflrun_obj.update_status(),
                'workflow_runs': self.wflrun_obj.runs_to_json()}
    #end def

    def reset_shards(self, shards_name):
        """
            reset WorkflowRun objects with shard_name in shards_name
            return updated workflow-runs and final_status information as json

                shards_name, list of names for workflow-runs that need to be reset
        """
        for name in shards_name:
            self.wflrun_obj.reset_shard(name)
        #end for
        return {'final_status':  self.wflrun_obj.update_status(),
                'workflow_runs': self.wflrun_obj.runs_to_json()}
    #end def

    def import_steps(self, wflrun_obj, steps_name):
        """
            update current MetaWorkflowRun object information
            import and use information from specified wflrun_obj
            update WorkflowRun objects up to steps specified by steps_name
            return updated meta-workflow-run as json

                wflrun_obj, MetaWorkflowRun object to get information from
                steps_name, list of names for step-workflows to fill in information from wflrun_obj
        """
        ## Import input
        self.wflrun_obj.input = wflrun_obj.input
        ## Import WorkflowRun objects
        for name in steps_name:
            queue = [] # queue of steps to import
                       #    name step and its dependencies
            # Get workflow-runs corresponding to name step
            for shard_name, run_obj in self.wflrun_obj.runs.items():
                if name == shard_name.split(':')[0]:
                    queue.append(run_obj)
                #end if
            #end for
            # Iterate queue, get dependencies and import workflow-runs
            while queue:
                run_obj = queue.pop(0)
                shard_name = run_obj.shard_name
                dependencies = run_obj.dependencies
                try:
                    self.wflrun_obj.runs[shard_name] = wflrun_obj.runs[shard_name]
                except KeyError as e:
                    raise ValueError('JSON content error, missing information for workflow-run "{0}"\n'
                                        .format(e.args[0]))
                #end try
                for dependency in dependencies:
                    queue.append(self.wflrun_obj.runs[dependency])
                #end for
            #end while
        #end for
        # Update final_status
        self.wflrun_obj.update_status()

        return self.wflrun_obj.to_json()
    #end def

#end class
