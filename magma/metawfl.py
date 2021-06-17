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
#   MetaWorkflow
################################################
class MetaWorkflow(object):
    """
        object to represent a meta-workflow
    """

    def __init__(self, input_json):
        """
            initialize MetaWorkflow object from input_json

                input_json is a meta-workflow in json format
        """

        # Copy it so that the original does not get changed unexpectedly
        input_json_ = copy.deepcopy(input_json)

        # Basic attributes
        for key in input_json_:
            setattr(self, key, input_json_[key])
        #end for
        # Calculated attributes
        self.steps = {} #{step_obj.name: step_obj, ...}
        self._end_workflows = None

        # Calculate attributes
        self._validate()
        self._read_steps()

    #end def

    class StepWorkflow(object):
        """
            object to represent a step-workflow
            that is a step of the meta-workflow
        """

        def __init__(self, input_json):
            """
                initialize StepWorkflow object

                    input_json is a step-workflow in json format
            """
            # Basic attributes
            for key in input_json:
                setattr(self, key, input_json[key])
            #end for
            # Calculated attributes
            self.is_scatter = 0 #dimension to scatter, int
            self.gather_from = {} #{name: dimension, ...} of steps to gather from
                                  # dimension is input argument dimension increment
                                  # and shard dimension decrement, int
            # For building graph structure
            self._nodes = set() #step_objects for steps that depend on current step
            # Dependencies
            #   names of steps that are dependency
            if getattr(self, 'dependencies', None):
                self.dependencies = set(self.dependencies)
            else:
                self.dependencies = set()
            #end def

            # Calculate attributes
            self._validate()
            self._attributes()
        #end def

        def _validate(self):
            """
            """
            try:
                getattr(self, 'name') #str, need to be unique
                getattr(self, 'workflow') #str, need to be unique
                getattr(self, 'config') #dict
                getattr(self, 'input') #list
            except AttributeError as e:
                raise ValueError('JSON validation error, {0}\n'
                                    .format(e.args[0]))
            #end try
        #end def

        def _attributes(self):
            """
                read arguments
                set calculated attributes for step-workflow
            """
            for arg in self.input:
                scatter = arg.get('scatter') #scatter dimension
                if scatter and scatter > self.is_scatter: # get max scatter
                    self.is_scatter = scatter
                #end if
                source = arg.get('source') #source step name
                if source:
                    self.dependencies.add(source)
                    gather = arg.get('gather')
                    if gather:
                        self.gather_from.setdefault(source, gather)
                    #end if
                #end if
            #end for
        #end def

    #end class

    def _validate(self):
        """
        """
        try:
            getattr(self, 'uuid') #str, need to be unique
            getattr(self, 'input') #list
            getattr(self, 'workflows') #list
        except AttributeError as e:
            raise ValueError('JSON validation error, {0}\n'
                                .format(e.args[0]))
        #end try
    #end def

    def _read_steps(self):
        """
            read step-workflows
            initialize StepWorkflow objects
        """
        for wfl in self.workflows:
            step_obj = self.StepWorkflow(wfl)
            if step_obj.name not in self.steps:
                self.steps.setdefault(step_obj.name, step_obj)
            else:
                raise ValueError('Validation error, step "{0}" duplicate in step workflows\n'
                                    .format(step_obj.name))
            #end if
        #end for
    #end def

    def _build_run(self, end_steps):
        """
            build graph structure for meta-workflow given end_steps
            backtrack from end_steps to link step-workflows that are dependencies
            return a set containg step-workflows that are entry point

                end_steps, names list of end step-workflows to build graph structure for
        """
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
                            raise ValueError('Validation error, missing dependency step "{0}" in step workflows\n'
                                                .format(dependency))
                        #end try
                    #end for
                else: steps_.add(step_obj)
                #end if
            #end while
        #end for
        return steps_
    #end def

    def _order_run(self, end_steps):
        """
            sort and list all step-workflows for meta-workflow given end_steps
            _build_run to build graph structure for meta-workflow
            start from step-workflows that are entry points
            navigate the graph structure
            return a list with step-workflows in order

                end_steps, names list of end step-workflows to build graph structure for
        """
        steps_ = []
        queue = list(self._build_run(end_steps))
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

    def _input_dimensions(self, input_structure):
        """
            given input_structure as list
            calculate dimensions

                input_structure, structure for the input with maximum scatter as list

            # TODO
            rewrite the function and generalize using recursion
        """
        input_dimensions = {}
        input_dimensions.setdefault(1, [len(input_structure)])
        if isinstance(input_structure[0], list):
            input_dimensions.setdefault(2, [])
            for i in input_structure:
                input_dimensions[2].append(len(i))
                if isinstance(i[0], list):
                    input_dimensions.setdefault(3, [])
                    d_ = []
                    for ii in i:
                        d_.append(len(ii))
                    #end for
                    input_dimensions[3].append(d_)
                #end if
            #end for
        #end if
        return input_dimensions
    #end def

    def _shards(self, input_dimensions, dimension):
        """
            given input_dimensions
            calculate shards for specified dimension

                input_dimensions, dimensions of input argument from _input_dimension
                dimension, dimension to calculate shards for

            # TODO
            rewrite the function and generalize using recursion
        """
        shards = []
        input_dimension = input_dimensions[dimension]
        if dimension == 1: #1st dimension
            for i in range(input_dimension[0]):
                shards.append([str(i)])
            #end for
        elif dimension == 2: #2nd dimension
            for i, d in enumerate(input_dimension):
                for ii in range(d):
                    shards.append([str(i), str(ii)])
                #end for
            #end for
        else: #3rd dimension
            for i, d in enumerate(input_dimension):
                for ii, dd in enumerate(d):
                    for iii in range(dd):
                        shards.append([str(i), str(ii), str(iii)])
                    #end for
                #end for
            #end for
        #end if
        return shards
    #end def

    def write_run(self, input_structure, end_steps=[]):
        """
            create json meta-workflow-run structure for meta-workflow given end_steps and input_structure
            _order_run to sort and list all step-workflows
            use scatter, gather_from and dependencies information
            to create and collect shards for individual step-workflows (workflow-runs)
            complete attributes and other metadata for meta-workflow
            return a json that represents a meta-workflow-run

                end_steps, names list of end step-workflows to build meta-workflow-run for
                           if no end_steps calculate using end_workflows
                input_structure, structure for the input with maximum scatter as list (e.g. [[A, B], [C, D], [E]])
        """
        # Get end_steps
        if not end_steps:
            end_steps = self.end_workflows
        #end if
        # Make input_structure a list if is string
        if isinstance(input_structure, str):
            input_structure = [input_structure]
        #end if
        scatter = {} #{step_obj.name: dimension, ...}
        dimensions = self._input_dimensions(input_structure)
        steps_ = self._order_run(end_steps)
        run_json = {
            'meta_workflow': self.uuid,
            'workflow_runs': [],
            'input': [],
            'final_status': 'pending'
        }
        for step_obj in steps_:
            run_step = {}
            run_step.setdefault('name', step_obj.name)
            run_step.setdefault('status', 'pending')
            # Check scatter
            #   If is_scatter or dependency in scatter
            #       but not in gather_from
            #       current step must be scattered
            scatter_dimension = 0 #dimension to scatter if any
            if step_obj.is_scatter:
                scatter_dimension = step_obj.is_scatter
                # Check if higher dimension in scatter
                #   get max scatter
                for dependency in step_obj.dependencies:
                    if dependency in scatter and scatter[dependency] > scatter_dimension:
                        scatter_dimension = scatter[dependency]
                    #end if
                #end for
                scatter.setdefault(step_obj.name, scatter_dimension)
            else:
                in_gather, gather_dimensions = True, []
                for dependency in step_obj.dependencies:
                    if dependency in scatter:
                        scatter_dimension = scatter[dependency]
                        if dependency not in step_obj.gather_from:
                            in_gather = False
                            break
                        else:
                            gather_dimension = scatter_dimension - step_obj.gather_from[dependency]
                            gather_dimensions.append(gather_dimension)
                        #end if
                    #end if
                #end for
                if in_gather and gather_dimensions:
                    scatter_dimension = max(gather_dimensions)
                #end if
                if scatter_dimension > 0:
                    scatter.setdefault(step_obj.name, scatter_dimension)
                #end if
            #end if
            # Created shards
            if scatter_dimension: #create shards
                shards = self._shards(dimensions, scatter_dimension)
            else: shards = [['0']] #no scatter, only one shard
            #end if
            for s in shards:
                run_step_ = copy.deepcopy(run_step)
                run_step_.setdefault('shard', ':'.join(s))
                # Check gather_from
                #   If dependency in gather_from,
                #       dependencies must be aggregated from scatter
                for dependency in sorted(step_obj.dependencies):
                    run_step_.setdefault('dependencies', [])
                    if dependency in step_obj.gather_from:
                        # reducing dimension with gather
                        #   but need to get shards for original scatter dimension
                        shards_gather = self._shards(dimensions, scatter[dependency])
                        gather_dimension = scatter[dependency] - step_obj.gather_from[dependency]
                        for s_g in shards_gather:
                            if scatter_dimension == 0 or \
                                scatter_dimension > gather_dimension: #gather all from that dependency
                                run_step_['dependencies'].append('{0}:{1}'.format(dependency, ':'.join(s_g)))
                            elif s_g[:scatter_dimension] == s: #gather only corresponding subset
                                run_step_['dependencies'].append('{0}:{1}'.format(dependency, ':'.join(s_g)))
                            #end if
                        #end for
                    else:
                        run_step_['dependencies'].append('{0}:{1}'.format(dependency, ':'.join(s)))
                    #end if
                #end for
                run_json['workflow_runs'].append(run_step_)
            #end for
        #end for
        return run_json
    #end def

    @property
    def end_workflows(self):
        if not self._end_workflows:
            all_wfls = [wfl.get('name') for wfl in self.workflows]
            sources = []
            for wfl in self.workflows:
                sources.extend([arg.get('source') for arg in wfl.get('input')])
                sources.extend(wfl.get('dependencies', []))
            #end for
            self._end_workflows = list(set(all_wfls).difference(set(sources)))
        #end if
        return sorted(self._end_workflows)
    #end def

#end class
