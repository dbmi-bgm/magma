#!/usr/bin/env python3

################################################
#
#   Library to work with meta-workflow-run objects
#
#   Michele Berselli
#   berselli.michele@gmail.com
#
################################################

################################################
#   Libraries
################################################
import sys, os

################################################
#   Objects
################################################
class MetaWorkflowRun(object):
    '''
        object to represent a meta-workflow-run
    '''

    def __init__(self, input_json):
        '''
            initialize MetaWorkflowRun object from input_json

                input_json is a meta-workflow-run in json format
        '''
        # Basic attributes
        for key in input_json:
            setattr(self, key, input_json[key])
        #end for
        # Calculated attributes
        self.runs = {} #{run_obj.shard_name: run_obj, ...}

        # Calculate attributes
        self._validate()
        self._read_runs()
    #end def

    class WorkflowRun(object):
        '''
            object to represent a workflow-run
        '''

        def __init__(self, input_json):
            '''
                initialize WorkflowRun object from input_json

                    input_json is a workflow-run in json format
            '''
            # Basic attributes
            for key in input_json:
                setattr(self, key, input_json[key])
            #end for
            if not getattr(self, 'output', None):
                self.output = []
            #end if
            if not getattr(self, 'dependencies', None):
                self.dependencies = []
            #end if
            # Validate
            self._validate()
            # Calculated attributes
            self.shard_name = self.name + ':' + self.shard
        #end def

        def _validate(self):
            '''
            '''
            try:
                getattr(self, 'name') #str
                getattr(self, 'status') #str, pending | running | completed | failed
                getattr(self, 'shard') #str
            except AttributeError as e:
                raise ValueError('JSON validation error, {0}\n'
                                    .format(e.args[0]))
            #end try
        #end def

    #end class

    def _validate(self):
        '''
        '''
        try:
            getattr(self, 'meta_workflow_uuid') #str
            getattr(self, 'input') #list
            getattr(self, 'workflow_runs') #list
        except AttributeError as e:
            raise ValueError('JSON validation error, {0}\n'
                                .format(e.args[0]))
        #end try
    #end def

    def _read_runs(self):
        '''
            read workflow-runs
            initialize WorkflowRun objects
        '''
        for run in self.workflow_runs:
            run_obj = self.WorkflowRun(run)
            if run_obj.shard_name not in self.runs:
                self.runs.setdefault(run_obj.shard_name, run_obj)
            else:
                raise ValueError('Validation error, step {0} duplicate in step workflows\n'
                                    .format(run_obj.shard_name))
            #end if
        #end for
    #end def

    def to_run(self):
        '''
            check pending workflow-runs
            check if dependencies are completed
            return a list of workflow-runs ready to run
        '''
        to_run = []
        for _, run_obj in self.runs.items():
            if run_obj.status == 'pending':
                is_dependencies = True
                # Check dependencies are completed
                for shard_name_ in run_obj.dependencies:
                    if self.runs[shard_name_].status != 'completed':
                        is_dependencies = False
                        break
                    #end if
                #end for
                if is_dependencies: to_run.append(run_obj)
                #end if
            #end if
        #end for
        return to_run
    #end def

    def update_attribute(self, shard_name, attribute, value):
        '''
            update attribute for WorkflowRun object in runs

                shard_name, WorkflowRun object shard_name
                attribute, attribute to update
                value, new value for attribute
        '''
        setattr(self.runs[shard_name], attribute, value)
    #end def

    def runs_to_json(self):
        '''
            return workflow_runs as json object
            build workflow_runs from WorkflowRun objects
        '''
        runs_ = []
        for run in self.workflow_runs: #used to get the right order
            run_ = {}
            shard_name = run['name'] + ':' + run['shard']
            for key, val in vars(self.runs[shard_name]).items():
                if val: run_.setdefault(key, val)
                #end if
            #end for
            runs_.append(run_)
        #end for
        return runs_
    #end def

#end class
