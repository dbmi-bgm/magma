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
import copy

################################################
#   MetaWorkflowRun
################################################
class MetaWorkflowRun(object):
    """
        object to represent a meta-workflow-run
    """

    def __init__(self, input_json):
        """
            initialize MetaWorkflowRun object from input_json

                input_json is a meta-workflow-run in json format
        """
        # Copy it so that the original does not get changed unexpectedly
        input_json_ = copy.deepcopy(input_json)

        # Basic attributes
        for key in input_json_:
            setattr(self, key, input_json_[key])
        #end for
        # Calculated attributes
        self.runs = {} #{run_obj.shard_name: run_obj, ...}

        # Calculate attributes
        self._validate()
        self._read_runs()
    #end def

    class WorkflowRun(object):
        """
            object to represent a workflow-run
        """

        def __init__(self, input_json):
            """
                initialize WorkflowRun object from input_json

                    input_json is a workflow-run in json format
            """
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
            """
            """
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
        """
        """
        try:
            getattr(self, 'meta_workflow') #str
            getattr(self, 'input') #list
            getattr(self, 'workflow_runs') #list
            getattr(self, 'final_status') #str, pending | running | completed | failed
        except AttributeError as e:
            raise ValueError('JSON validation error, {0}\n'
                                .format(e.args[0]))
        #end try
    #end def

    def _read_runs(self):
        """
            read workflow-runs
            initialize WorkflowRun objects
        """
        for run in self.workflow_runs:
            run_obj = self.WorkflowRun(run)
            if run_obj.shard_name not in self.runs:
                self.runs.setdefault(run_obj.shard_name, run_obj)
            else:
                raise ValueError('Validation error, step "{0}" duplicate in step workflows\n'
                                    .format(run_obj.shard_name))
            #end if
        #end for
    #end def

    def to_run(self):
        """
            check pending workflow-runs
            check if dependencies are completed
            return a list of WorkflowRun objects
            for workflow-runs that are ready to run
        """
        runs_ = []
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
                if is_dependencies: runs_.append(run_obj)
                #end if
            #end if
        #end for
        return runs_
    #end def

    def running(self):
        """
            check running workflow-runs
            return a list of WorkflowRun objects
            for workflow-runs that have status set to running
        """
        runs_ = []
        for _, run_obj in self.runs.items():
            if run_obj.status == 'running':
                runs_.append(run_obj)
            #end if
        #end for
        return runs_
    #end def

    def update_attribute(self, shard_name, attribute, value):
        """
            update attribute value for WorkflowRun object in runs

                shard_name, WorkflowRun object shard_name ('name:shard')
                attribute, attribute to update
                value, new value for attribute
        """
        setattr(self.runs[shard_name], attribute, value)
    #end def

    def runs_to_json(self):
        """
            return workflow_runs as json (list of dictionaries)
            build workflow_runs from WorkflowRun objects
        """
        runs_ = []
        for run in self.workflow_runs: #used to get the right order
            run_ = {}
            shard_name = run['name'] + ':' + run['shard']
            for key, val in vars(self.runs[shard_name]).items():
                if val and key != 'shard_name':
                    run_.setdefault(key, val)
                #end if
            #end for
            runs_.append(run_)
        #end for
        return runs_
    #end def

    def to_json(self):
        """
            return meta-workflow-run json
            build workflow_runs from WorkflowRun objects
        """
        run_json = {}
        # Get attributes
        for key, val in vars(self).items():
            if key != 'runs':
                run_json.setdefault(key, val)
            #end if
        #end for
        # Get updated workflow_runs from current WorkflowRun objects
        run_json['workflow_runs'] = self.runs_to_json()
        return run_json
    #end def

    def _reset_run(self, shard_name):
        """
            reset attributes value for WorkflowRun object in runs

                shard_name, is the name of the workflow-run to reset
        """
        run_obj = self.runs[shard_name]
        # Reset run_obj
        run_obj.output = []
        run_obj.status = 'pending'
        if getattr(run_obj, 'jobid', None):
            delattr(run_obj, 'jobid')
        #end if
    #end def

    def reset_step(self, step_name):
        """
            reset attributes value for WorkflowRun objects in runs
            reset all workflow-runs corresponding to step-workflow specified by step_name

                step_name, is the name of the step-workflow to reset
        """
        for shard_name_ in self.runs:
            if step_name == shard_name_.split(':')[0]:
                # Reset run_obj
                self._reset_run(shard_name_)
            #end if
        #end for
    #end def

    def reset_shard(self, shard_name):
        """
            reset attributes value for WorkflowRun object in runs
            reset only workflow-run specified by shard_name

                shard_name, is the name of the workflow-run to reset
        """
        for shard_name_ in self.runs:
            if shard_name == shard_name_:
                # Reset run_obj
                self._reset_run(shard_name_)
            #end if
        #end for
    #end def

    def update_status(self): # failed -> running -> completed
        """
            check status for all WorkflowRun objects
            if at least one is failed, set final_status as failed
            if no failed and at least one is running, set final_status as running
            if all are completed set final_status as completed
            return final_status
        """
        self.final_status = 'pending' # initial set to pending
        all_completed = True
        is_completed = False
        for _, run_obj in self.runs.items():
            if run_obj.status != 'completed':
                all_completed = False
                if run_obj.status == 'failed':
                    self.final_status = 'failed'
                    break
                elif run_obj.status == 'running':
                    self.final_status = 'running'
                #end if
            else: is_completed = True
            #end if
        #end for
        if all_completed:
            self.final_status = 'completed'
        elif self.final_status == 'pending' and is_completed:
            self.final_status = 'inactive'
        #end if
        return self.final_status
    #end def

#end class
