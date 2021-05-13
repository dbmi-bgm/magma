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
        try:
            # self.accession = input_json['accession'] #str, need to be unique
            # self.uuid = input_json['uuid'] #str, need to be unique
            # self.app_name = input_json['app_name'] #str, need to be unique
            # self.app_version = input_json['app_version'] #str
            self.meta_workflow_uuid = input_json['meta_workflow_uuid'] #str
            self.input = input_json['input'] #list
            self.workflow_runs = input_json['workflow_runs'] #list
        except KeyError as e:
            raise ValueError('Validation error, missing key {0} in meta-workflow-run json\n{1}\n'
                                .format(e.args[0], input_json))
        #end try
        # Calculated attributes
        self.runs = {} #{run_obj.shard_name: run_obj, ...}

        # Calculate attributes
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
            try:
                self.name = input_json['name'] #str
                self.workflow_run_uuid = input_json['workflow_run_uuid'] #str
                self.output = input_json['output'] #
                self.status = input_json['status'] #str, pending | running | completed | failed
                self.dependencies = input_json['dependencies'] #list
                self.shard = input_json['shard'] #str
            except KeyError as e:
                raise ValueError('Validation error, missing key {0} in workflow-run json\n{1}\n'
                                    .format(e.args[0], input_json))
            #end try
            # Calculated attributes
            self.shard_name = self.name + ':' + self.shard
        #end def

    #end class

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

#end class
