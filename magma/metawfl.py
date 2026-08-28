#!/usr/bin/env python3

################################################
#
#   Library to work with MetaWorkflow[json]
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
    """Class to represent a MetaWorkflow[json].
    """

    def __init__(self, input_json):
        """Constructor method.
        Initialize object and attributes.

        :param input_json: MetaWorkflow[json]
        :type input_json: dict
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
        """Class to represent a StepWorkflow[json]
        that is step of a MetaWorkflow[json].
        """

        def __init__(self, input_json):
            """Constructor method.
            Initialize object and attributes.

            :param input_json: StepWorkflow[json]
            :type input_json: dict
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
            self.gather_input = {} #equivalent to gather_from but this will only
                                   # affect dependencies input and not the
                                   # scatter structure of the step
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
            # Validate that gather and gather_input have not been set together
            #   for the step
            if self.gather_from and self.gather_input:
                raise ValueError('JSON validation error, {0}\n'
                    .format('gather and gather_input can\'t be used together in the same step'))
            #end if
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
            """Read arguments and set calculated attributes.
            """
            for arg in self.input:
                scatter = arg.get('scatter') #scatter dimension
                if scatter and scatter > self.is_scatter: #get max scatter
                    self.is_scatter = scatter
                #end if
                source = arg.get('source') #source step name
                if source:
                    self.dependencies.add(source)
                    gather = arg.get('gather')
                    if gather:
                        self.gather_from.setdefault(source, gather)
                    else:
                        gather_input = arg.get('gather_input')
                        if gather_input:
                            self.gather_input.setdefault(source, gather_input)
                        #end if
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
        """Build graph structure for MetaWorkflow[json] given end_steps.
        Backtrack from end_steps to link StepWorkflow[obj] that are dependencies.
        Return a set containg StepWorkflow[obj] that are entry point.

        :param end_steps: List of names for finals StepWorkflow[obj] to
            use while building the graph structure
        :type end_steps: list(str)
        :return: Set of StepWorkflow[obj] that are starting points
        :rtype: set(obj)
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
        """Sort and list all StepWorkflow[obj]
        necessary to run MetaWorkflow[json] given end_steps.

        The function will:
            - _build_run to build a graph structure for MetaWorkflow[json]
            - navigate the graph structure starting from StepWorkflow[obj]
                that are entry points

        :param end_steps: List of names for finals StepWorkflow[obj] to
            use while building the graph structure
        :type end_steps: list(str)
        :return: List with sorted StepWorkflow[obj]
        :rtype: list(object)
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
        """Given input_structure as list calculate dimensions.

        :param input_structure: Structure for the input with maximum scatter as list
        :type input_structure: list
        :return: Input dimensions
        :rtype: dict
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
        """Given input_dimensions calculate shards for specified dimension.

        :param input_dimensions: Input dimensions
        :type input_dimensions: dict
        :param dimension: Dimension to calculate shards for
        :type dimension: int [1|2|3]
        :return: List of shards
        :rtype: list(str)
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

    def _shards_dimension(self, shards):
        """
        """
        return len(shards[0])

    def _scatter_argument_names(self, step_obj):
        """Given StepWorkflow[obj] calculate the names of the input arguments
        that the step scatters over at its maximum scatter dimension.

        Only arguments that are matched to the input of the MetaWorkflowRun are
        returned, arguments that are matched to the output of a previous step
        (source) scatter over that output and not over the input.

        :param step_obj: StepWorkflow[obj] representing a StepWorkflow[json]
        :type step_obj: object
        :return: Names of the input arguments to scatter over
        :rtype: list(str)
        """
        argument_names = []
        for arg in step_obj.input:
            if arg.get('scatter') != step_obj.is_scatter:
                continue
            if arg.get('argument_type') != 'file' or arg.get('source'):
                continue
            # source_argument_name is the name of the argument
            #   in the input of the MetaWorkflowRun, if specified
            argument_names.append(
                arg.get('source_argument_name') or arg.get('argument_name')
            )
        return argument_names

    def _step_dimensions(self, step_obj, scatter_dimension, dimensions, dimensions_by_argument):
        """Given StepWorkflow[obj] get the input dimensions that define
        the shards of the step.

        These are the dimensions of the input argument the step scatters over,
        if that argument has its own input structure, else the dimensions of the
        input structure with maximum scatter.

        :param step_obj: StepWorkflow[obj] representing a StepWorkflow[json]
        :type step_obj: object
        :param scatter_dimension: Dimension the step is scattered on
        :type scatter_dimension: int
        :param dimensions: Input dimensions for the input structure
            with maximum scatter
        :type dimensions: dict
        :param dimensions_by_argument: Input dimensions by input argument name
        :type dimensions_by_argument: dict
        :return: Input dimensions to calculate the shards of the step
        :rtype: dict
        """
        if scatter_dimension != step_obj.is_scatter:
            # The scatter dimension has been increased by a scattered dependency,
            #   the input argument of the step does not define the structure
            return dimensions
        step_dimensions, argument_names = [], []
        for argument_name in self._scatter_argument_names(step_obj):
            argument_dimensions = dimensions_by_argument.get(argument_name)
            if argument_dimensions and argument_dimensions not in step_dimensions:
                step_dimensions.append(argument_dimensions)
                argument_names.append(argument_name)
        if not step_dimensions:
            return dimensions
        if len(step_dimensions) > 1:
            raise ValueError(
                'Value error, step "{0}" scatters over arguments with different input structures ({1})\n'
                    .format(step_obj.name, ', '.join(argument_names))
            )
        return step_dimensions[0]

    def _inherited_shards(self, step_obj, inherit_from, shards_by_step):
        """Given StepWorkflow[obj] that inherits its scatter structure from
        dependencies get the shards to align to them.

        :param step_obj: StepWorkflow[obj] representing a StepWorkflow[json]
        :type step_obj: object
        :param inherit_from: Names of the dependencies the scatter structure
            is inherited from
        :type inherit_from: list(str)
        :param shards_by_step: Shards by step name
        :type shards_by_step: dict
        :return: List of shards
        :rtype: list(str)
        """
        shards_ = []
        for dependency in inherit_from:
            if shards_by_step[dependency] not in shards_:
                shards_.append(shards_by_step[dependency])
        if len(shards_) > 1:
            raise ValueError(
                'Value error, step "{0}" depends on steps with different shards ({1}), gather is required to combine them\n'
                    .format(step_obj.name, ', '.join(inherit_from))
            )
        return shards_[0]

    def _validate_gather_shards(self, step_obj, shards, scatter_dimension, shards_by_step):
        """Given StepWorkflow[obj] that is scattered and gathers from
        dependencies check that its shards match the shards of the steps
        it gathers from.

        The shards of a partial gather are calculated from the input structure,
        which is the wrong structure if a step to gather from is scattered on
        a structure of its own.

        :param step_obj: StepWorkflow[obj] representing a StepWorkflow[json]
        :type step_obj: object
        :param shards: Shards calculated for the step
        :type shards: list(str)
        :param scatter_dimension: Dimension the step is scattered on
        :type scatter_dimension: int
        :param shards_by_step: Shards by step name
        :type shards_by_step: dict
        :raises ValueError: If the shards of the step don't match the shards
            of a step it gathers from
        """
        for dependency in sorted(step_obj.gather_from):
            shards_gather = shards_by_step.get(dependency)
            if not shards_gather:
                continue
            if self._shards_dimension(shards_gather) < scatter_dimension:
                # All shards of the dependency are gathered in every shard
                #   of the step, the dependency does not define its shards
                continue
            # The shards of the step must be the shards of the dependency
            #   reduced to the dimension the step is scattered on
            shards_ = []
            for s_g in shards_gather:
                if s_g[:scatter_dimension] not in shards_:
                    shards_.append(s_g[:scatter_dimension])
            if sorted(shards_) != sorted(shards):
                raise ValueError(
                    'Value error, shards {0} calculated for step "{1}" don\'t match the shards {2} it gathers from step "{3}"\n'
                        .format(
                            [':'.join(s) for s in shards],
                            step_obj.name,
                            [':'.join(s) for s in shards_],
                            dependency
                        )
                )

    def write_run(self, input_structure, end_steps=[], input_structures=None):
        """Create MetaWorkflowRun[json] for MetaWorkflow[json]
        given end_steps and input_structure.

        The function will:
            - _order_run to sort and list all necessary StepWorkflow[json]
            - use scatter, gather_from and dependencies information
                to create and collect shards for individual StepWorkflow[json]
            - complete attributes and other metadata for MetaWorkflow[json]

        :param end_steps: List of names for finals StepWorkflow[obj] to
            use while creating MetaWorkflowRun[json], if no end_steps
            is specified calculate using end_workflows
        :type end_steps: list(str)
        :param input_structure: Structure for the input with
            maximum scatter as list (e.g. [[A, B], [C, D], [E]])
        :type input_structure: list [1|2|3 dimensions]
        :param input_structures: Structures for the individual input arguments
            as dict (e.g. {'ARG_NAME': [A, B, C], ...}), used to calculate the
            shards of the steps that scatter over them. Arguments that are
            missing, and steps that scatter over the output of a previous step,
            fall back to input_structure
        :type input_structures: dict
        :return: MetaWorkflowRun[json]
        :rtype: dict
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
        fixed_shards = {} #{step_obj.name: shards, ...}
        shards_by_step = {} #{step_obj.name: shards, ...} for all steps
        dimensions = self._input_dimensions(input_structure)
        # Get dimensions for the input arguments with their own structure
        dimensions_by_argument = {} #{argument_name: dimensions, ...}
        for argument_name, argument_structure in (input_structures or {}).items():
            if isinstance(argument_structure, str):
                argument_structure = [argument_structure]
            #end if
            dimensions_by_argument.setdefault(
                argument_name, self._input_dimensions(argument_structure)
            )
        #end for
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
            inherit_from = [] #dependencies the scatter structure is inherited from
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
                in_gather, gather_dimensions, inherit_dimensions = True, [], []
                #   sorted to make the inherited scatter structure deterministic
                for dependency in sorted(step_obj.dependencies):
                    if dependency in scatter:
                        if dependency not in step_obj.gather_from:
                            in_gather = False
                            inherit_dimensions.append(scatter[dependency])
                            if dependency not in step_obj.gather_input:
                                # gather_input only affects the dependencies and
                                #   not the scatter structure of the step
                                inherit_from.append(dependency)
                            #end if
                        else:
                            gather_dimension = scatter[dependency] - step_obj.gather_from[dependency]
                            gather_dimensions.append(gather_dimension)
                        #end if
                    #end if
                #end for
                if inherit_dimensions:
                    # Scatter structure is inherited from the dependencies
                    #   that are not gathered from, get max scatter
                    scatter_dimension = max(inherit_dimensions)
                elif in_gather and gather_dimensions:
                    scatter_dimension = max(gather_dimensions)
                #end if
                # Only the dependencies scattered on the max dimension
                #   define the shards of the step
                inherit_from = [i for i in inherit_from if scatter[i] == scatter_dimension]
                if scatter_dimension > 0:
                    scatter.setdefault(step_obj.name, scatter_dimension)
                #end if
            #end if
            # Created shards
            #   Check if there are fixed shards,
            #       else calculate based in input, scatter, gather dimensions
            if hasattr(step_obj, 'shards'):
                shards = step_obj.shards
                fixed_shards.setdefault(step_obj.name, shards)
            elif scatter_dimension:
                if inherit_from:
                    # Align to the shards of the dependencies
                    #   the scatter structure is inherited from
                    shards = self._inherited_shards(step_obj, inherit_from, shards_by_step)
                else:
                    shards = self._shards(
                        self._step_dimensions(
                            step_obj, scatter_dimension, dimensions, dimensions_by_argument
                        ),
                        scatter_dimension
                    )
                    self._validate_gather_shards(
                        step_obj, shards, scatter_dimension, shards_by_step
                    )
                #end if
            else: shards = [['0']] #no scatter, only one shard
            #end if
            shards_by_step.setdefault(step_obj.name, shards)
            for s in shards:
                run_step_ = copy.deepcopy(run_step)
                run_step_.setdefault('shard', ':'.join(s))
                for dependency in sorted(step_obj.dependencies):
                    run_step_.setdefault('dependencies', [])
                    # Check gather
                    #   If dependency in gather_from or gather_input,
                    #       dependencies must be aggregated from scatter
                    gather_from_ = None
                    if dependency in step_obj.gather_from:
                        gather_from_ = step_obj.gather_from
                    elif dependency in step_obj.gather_input:
                        gather_from_ = step_obj.gather_input
                    #end if
                    if gather_from_:
                        # Get the shards of the previous step,
                        #   these can be fixed shards, shards calculated for the
                        #   original scatter dimension, or shards inherited from
                        #   its own dependencies
                        shards_gather = shards_by_step[dependency]
                        # Reducing dimension organically to gather
                        gather_dimension = self._shards_dimension(shards_gather) - gather_from_[dependency]
                        for s_g in shards_gather:
                            if scatter_dimension == 0 or \
                                scatter_dimension > gather_dimension: #gather all from that dependency
                                run_step_['dependencies'].append('{0}:{1}'.format(dependency, ':'.join(s_g)))
                            elif s_g[:scatter_dimension] == s: #gather only corresponding subset
                                run_step_['dependencies'].append('{0}:{1}'.format(dependency, ':'.join(s_g)))
                            #end if
                        #end for
                    else: # No gather, normal dependency
                        # Choose dependency shard:
                        # - if producer has fixed_shards and it's exactly one, always use that one
                        # - else if producer is scattered, align to current shard `s`
                        # - else (producer not scattered), use ['0']
                        if dependency in fixed_shards:
                            dep_fixed = fixed_shards[dependency]   # e.g. [['0']] or [['0'], ['1'], ...]
                            dep_shard = dep_fixed[0] if len(dep_fixed) == 1 else s
                        elif dependency in scatter:
                            dep_shard = s
                        else:
                            dep_shard = ['0']
                        # Check that the shard of the dependency exists,
                        #   steps that are scattered differently can only be
                        #   combined by gathering from them
                        if dep_shard not in shards_by_step[dependency]:
                            raise ValueError(
                                'Value error, shard "{0}" of step "{1}" depends on step "{2}" that has no matching shard, gather is required to combine them\n'
                                    .format(':'.join(s), step_obj.name, dependency)
                            )
                        #end if
                        # Add dependency with shard
                        run_step_['dependencies'].append('{0}:{1}'.format(dependency, ':'.join(dep_shard)))
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
