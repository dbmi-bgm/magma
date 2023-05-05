#!/usr/bin/env python3

################################################
#   Libraries
################################################
from magma.validated_dictionary import ValidatedDictionary

################################################
#   MetaWorkflowRunStep
################################################
class MetaWorkflowRunStep(ValidatedDictionary):
    """
    Class to represent a MetaWorkflow Run object,
    as a step within a MetaWorkflow Run Handler object.
    Assumption that this is based on ordered_meta_workflows list
    from a MetaWorfklow Handler.
    """

    NAME_ATTR = "name" # name of metaworkflow corresponding to the metaworkflow run
    STATUS_ATTR = "status"
    DEP_ATTR = "dependencies"
    MWF_RUN_ATTR = "meta_workflow_run" #TODO: used within the handler itself
    # ITEMS_CREATION_ATTR = "items_for_creation" #TODO: do this embedding in ff. BUT. make req in schema?
    # this above TODO: is very important (unless checked elsewhere)

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: a MetaWorkflow step (object) and accompanying info within handler, defined by json file
        :type input_dict: dict
        """
        super().__init__(input_dict)

        # for automatically setting initial status to "pending", unless explicitly defined not to
        if not hasattr(self, self.STATUS_ATTR):
            setattr(self, self.STATUS_ATTR, "pending")

        # Validate presence of basic attributes of this MetaWorkflow step
        self._validate_basic_attributes(self.NAME_ATTR, self.DEP_ATTR)

################################################
#   MetaWorkflowRunHandler
################################################
class MetaWorkflowRunHandler(ValidatedDictionary):
    """
    Class representing a MetaWorkflowRun Handler object,
    a list of MetaWorkflowsRuns with specified dependencies,
    and their status
    """

    UUID_ATTR = "uuid"
    ASSOCIATED_METAWORKFLOW_HANDLER_ATTR = "meta_workflow_handler"
    META_WORKFLOW_RUNS_ATTR = "meta_workflow_runs"
    FINAL_STATUS_ATTR = "final_status"

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: MetaWorkflow Handler Run object
        :type input_dict: dict
        """

        ### Basic attributes ###

        super().__init__(input_dict)
        
        self._validate_basic_attributes(self.UUID_ATTR, self.ASSOCIATED_METAWORKFLOW_HANDLER_ATTR, self.META_WORKFLOW_RUNS_ATTR)

        # initial final status attribute upon creation
        setattr(self, self.FINAL_STATUS_ATTR, "pending")

        ### Calculated attributes ###

        # by nature of how a MetaWorkflowRun Handler is created from the MetaWorkflow Handler,
        # the array "meta_workflow_runs" will already be in some valid topologically sorted order
        #(based on topologically sorted list "meta_workflows" in the regular handler)
        # here, though, we create a dictionary of the form {mwf_name: MetaWorkflowRunStep_object,...}
        # for faster lookup and updating of steps
        self.meta_workflow_run_step_dict = self._create_meta_workflow_run_step_objects()


    def _create_meta_workflow_run_step_objects(self):
        # creates dict: {name_1: step_obj_1, name_2: step_obj_2,...}
        meta_workflow_run_step_dict = {}
        for meta_workflow_run in self.meta_workflow_runs:
            meta_workflow_run_step_object = MetaWorkflowRunStep(meta_workflow_run)
            step_name = meta_workflow_run["name"]
            meta_workflow_run_step_dict[step_name] = meta_workflow_run_step_object
        return meta_workflow_run_step_dict

    # to update final_status attribute of the handler
    def update_final_status(self):
        """
        Check status for all MetaWorkflowRunStep objects.
        Initial final status = pending
        If a step is running and none others have failed or stopped, final_status = running
        If all steps are completed, final_status = completed
        If a step has failed, final_status = failed
        If a step has been stopped, final_status = stopped

        :return: final_status
        :rtype: str
        """
        # options for mwf runs: pending, running, completed, failed, stopped
        # TODO: additional final_status possibilities from mwf run schema --> inactive, quality metric failed (how to handle these??)
        # TODO: use setattr method consistently

        all_steps_completed = True

        for meta_workflow_run_step in self.meta_workflow_run_step_dict.values():
            if meta_workflow_run_step.status != "completed":
                all_steps_completed = False
                if meta_workflow_run_step.status == "running":
                    setattr(self, self.FINAL_STATUS_ATTR, "running")
                elif meta_workflow_run_step.status == "failed":
                    setattr(self, self.FINAL_STATUS_ATTR, "failed")
                    break
                elif meta_workflow_run_step.status == "stopped":
                    setattr(self, self.FINAL_STATUS_ATTR, "stopped")
                    break
                    
        # if all the steps were successfully completed
        if all_steps_completed:
            setattr(self, self.FINAL_STATUS_ATTR, "completed")

        #TODO: update pytests here
        return self.FINAL_STATUS_ATTR

    #TODO: add this to pytests
    def retrieve_meta_workflow_run_step_by_name(self, meta_workflow_run_name):
        step_obj = self.meta_workflow_run_step_dict[meta_workflow_run_name]
        return step_obj

    # the following allows for resetting a MetaWorkflow Run Step
    # this can happen only when the duplication flag is set to True
    def reset_meta_workflow_run_step(self, meta_workflow_run_name):
        """
        Resets status and meta_workflow_run attributes of a MetaWorkflowRunStep, given its name

        :param meta_workflow_run_name: name attribute of a MetaWorkflowRunStep
        :type meta_workflow_run_name: str
        """
        try:
            step_obj = self.retrieve_meta_workflow_run_step_by_name(meta_workflow_run_name)
            # Reset the status of the MetaWorkflow Run 
            setattr(step_obj, step_obj.STATUS_ATTR, "pending")
            # Remove and reset the attribute for the LinkTo to the corresponding MetaWorkflow Run
            setattr(step_obj, step_obj.MWF_RUN_ATTR, None)
        except KeyError as key_err:
            raise KeyError("{0} is not a valid MetaWorkflowRun Step name.\n"
                                .format(key_err.args[0]))

    # this is a more generalized version of the above
    # this is for redefining any attribute of a MetaWorkflow Run Step
    def update_meta_workflow_run_step(self, meta_workflow_run_name, attribute, value):
        try:
            step_obj = self.retrieve_meta_workflow_run_step_by_name(meta_workflow_run_name)
            # Reset the given attribute
            setattr(step_obj, attribute, value)
        except KeyError as key_err:
            raise KeyError("{0} is not a valid MetaWorkflowRun Step name.\n"
                                .format(key_err.args[0]))

    # TODO: also have to add this to pytests -- nonexistent attr? check w other fxn too
    def get_step_attr(self, meta_workflow_run_name, attribute_to_fetch):
        try:
            step_obj = self.retrieve_meta_workflow_run_step_by_name(meta_workflow_run_name)
            # Return the status
            return getattr(step_obj, attribute_to_fetch, None)
        except KeyError as key_err:
            raise KeyError("{0} is not a valid MetaWorkflowRun Step name.\n"
                                .format(key_err.args[0]))

    def pending_steps(self):
        """
        returns a list of pending steps (by name)
        if no more pending, return empty list
        """
        pending_steps_list = []

        for meta_workflow_run_step in self.meta_workflow_runs:
            step_name = meta_workflow_run_step["name"]
            #TODO: make pending a global var
            if self.get_step_attr(step_name, "status") == "pending":
                pending_steps_list.append(step_name)
        
        return pending_steps_list

    def running_steps(self):
        """
        returns a list of running steps (by name)
        if no more running, return empty list
        """
        running_steps_list = []
        for meta_workflow_run in self.meta_workflow_runs:
            associated_meta_workflow_name = meta_workflow_run["name"]
            if self.get_step_attr(associated_meta_workflow_name, "status") == "running":
                running_steps_list.append(associated_meta_workflow_name)
        
        return running_steps_list

    # TODO: move to ff because portal specific
    # and test out
    def update_meta_workflows_array(self):
        """
        updates run_uuid, status, error attrs
        for mwfr dicts for patching mwfr steps array
        """
        for meta_workflow_run_dict in self.meta_workflow_runs:
            associated_meta_workflow_name = meta_workflow_run_dict["name"]
            meta_workflow_run_uuid = self.get_step_attr(associated_meta_workflow_name, "run_uuid")
            status = self.get_step_attr(associated_meta_workflow_name, "status")
            error = self.get_step_attr(associated_meta_workflow_name, "error")

            if meta_workflow_run_uuid:
                meta_workflow_run_dict["run_uuid"] = meta_workflow_run_uuid
            if status:
                meta_workflow_run_dict["status"] = status
            if error:
                meta_workflow_run_dict["error"] = error

        return self.meta_workflow_runs