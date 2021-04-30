#!/usr/bin/env python3

################################################
#
#   Library to work with meta-workflow objects
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
#   Objects
################################################
class Wfl(object):
    '''
        object to represent a meta-workflow
    '''

    def __init__(self, wfl_json):
        '''
            initialize Wfl object from wfl_json

                wfl_json is a meta-workflow in json format
        '''
        # Basic attributes
        try:
            self.accession = wfl_json['accession'] #str, need to be unique
            self.app_name = wfl_json['app_name'] #str, need to be unique
            self.app_version = wfl_json['app_version'] #str
            self.uuid = wfl_json['uuid'] #str, need to be unique
            self.arguments = wfl_json['arguments'] #dict
            self.workflows = wfl_json['workflows'] #dict
        except KeyError as e:
            raise ValueError('Validation error, missing key {0} in meta-workflow json\n{1}\n'
                                .format(e.args[0], wfl_json))
        #end try
        # Calculated attributes
        self.steps = {} #{step_obj.name: step_obj, ...}

        # Calculate attributes
        self._read_steps()
    #end def

    class Step(object):
        '''
            object to represent a meta-workflow step
        '''

        def __init__(self, step_json):
            '''
                initialize Step object

                    step_json is a step workflow in json format
            '''
            # Basic attributes
            try:
                self.name = step_json['name'] #str, need to be unique
                self.uuid = step_json['uuid'] #str, need to be unique
                self.config = step_json['config'] #dict
                self.arguments = step_json['arguments'] #dict
                self.outputs = step_json['outputs'] #dict
            except KeyError as e:
                raise ValueError('Validation error, missing key {0} in step workflow json\n{1}\n'
                                    .format(e.args[0], step_json))
            #end try
            # Calculated attributes
            self.is_scatter = False
            self.gather_from = set() #names of steps to gather from
            self.dependencies = set() #names of steps that are dependency
            # For building graph structure
            self._nodes = set() #step_objects for steps that depend on current step

            # Calculate attributes
            self._attributes()
        #end def

        def _attributes(self):
            '''
                read arguments
                set step calculated attributes
            '''
            for arg in self.arguments:
                if not self.is_scatter and arg.get('scatter'):
                    self.is_scatter = True
                #end if
                source_step = arg.get('source_step') #source step name
                if source_step:
                    self.dependencies.add(source_step)
                    if arg.get('gather'):
                        self.gather_from.add(source_step)
                    #end if
                #end if
            #end for
        #end def

    #end class

    def _read_steps(self):
        '''
            read step workflows
            initialize Step objects
        '''
        for wfl in self.workflows:
            step_obj = self.Step(wfl)
            if step_obj.name not in self.steps:
                self.steps.setdefault(step_obj.name, step_obj)
            else:
                raise ValueError('Validation error, step {0} duplicate in step workflows\n'
                                    .format(step_obj.name))
            #end if
        #end for
    #end def

    def _build_wfl_run(self, end_steps):
        '''
            build graph structure for workflow given end_steps
            backtrack from end_steps to link steps that are dependencies
            return a set containg steps that are entry point

                end_steps, names list of end steps to build workflow for
        '''
        steps_ = set() #steps that are entry point to wfl_run
        for end_step in end_steps:
            # Initialize queue with end_step
            queue = [self.steps[end_step]]
            # Reconstructing dependencies
            while queue:
                step_obj = queue.pop(0)
                if step_obj.dependencies:
                    for dependency in step_obj.dependencies:
                        try:
                            queue.append(self.steps[dependency])
                            self.steps[dependency]._nodes.add(step_obj)
                        except Exception:
                            raise ValueError('Validation error, missing dependency step {0} in step workflows\n'
                                                .format(dependency))
                        #end try
                    #end for
                else: steps_.add(step_obj)
                #end if
            #end while
        #end for
        return steps_
    #end def

    def _order_wfl_run(self, end_steps):
        '''
            sort and list all workflow steps given end_steps
            _build_wfl_run to build graph structure for workflow
            start from steps that are entry points
            navigate the graph structure
            return a list with workflow steps in order

                end_steps, names list of end steps to build workflow for
        '''
        steps_ = []
        queue = list(self._build_wfl_run(end_steps))
        while queue:
            step_obj = queue.pop(0)
            # Adding next steps to queue
            for node in step_obj._nodes:
                if node not in steps_ and node not in queue:
                    queue.append(node)
                #end if
            #end for
            # Checking if dependencies are satisfied already and step can be added to steps_
            is_dependencies = True
            if step_obj.dependencies:
                for dependency in step_obj.dependencies:
                    if self.steps[dependency] not in steps_:
                        is_dependencies = False
                        queue.append(step_obj)
                        break
                    #end if
                #end for
            #end if
            if is_dependencies:
                steps_.append(step_obj)
            #end if
        #end while
        return steps_
    #end def

    def write_wfl_run(self, end_steps, input=3):
        '''
            create json structure for workflow given end_steps and input
            _order_wfl_run to sort and list all workflow steps
            use scatter, gather_from and dependencies information
            to create and collect shards for individual steps
            complete attributes and other metadata for workflow
            return a json that represents a workflow run object

                end_steps, names list of end steps to build workflow for
                input, dictionary with input arguments
        '''
        n = input
        scatter = {}
        steps_ = self._order_wfl_run(end_steps)
        run_json = {
            'meta_workflow_uuid': self.uuid,
            'workflow_runs': [],
            'input': []
        }
        for step_obj in steps_:
            run_step = {}
            run_step.setdefault('name', step_obj.name)
            # run_step.setdefault('workflow_run_uuid', '')
            # run_step.setdefault('output', '')
            # run_step.setdefault('status', 'pending')
            run_step.setdefault('dependencies', [])
            # Check scatter
            #   If dependency in scatter but not in gather_from,
            #       current step must be scattered.
            scatter_ = 1 #default is no scatter, 1 step
            if step_obj.is_scatter:
                scatter_ = n
                scatter.setdefault(step_obj.name, scatter_)
            else:
                for dependency in step_obj.dependencies:
                    if dependency in scatter:
                        if dependency not in step_obj.gather_from:
                            scatter_ = scatter[dependency]
                            scatter.setdefault(step_obj.name, scatter_)
                            break
                        #end if
                    #end if
                #end for
            #end if
            # Created shards
            for i in range(scatter_):
                run_step_ = copy.deepcopy(run_step)
                run_step_.setdefault('shard', i)
                # Check gather_from
                #   If dependency in gather_from,
                #       dependencies must be aggregated from scatter.
                for dependency in step_obj.dependencies:
                    if dependency in step_obj.gather_from:
                        for i_ in range(scatter[dependency]):
                            run_step_['dependencies'].append('{0}:{1}'.format(dependency, i_))
                        #end for
                    else:
                        run_step_['dependencies'].append('{0}:{1}'.format(dependency, i))
                    #end if
                #end for
                run_json['workflow_runs'].append(run_step_)
            #end for
        #end for
        return run_json
    #end def

#end class
