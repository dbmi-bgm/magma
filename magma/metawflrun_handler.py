#!/usr/bin/env python3

################################################
#   Libraries
################################################
from magma.validated_dictionary import ValidatedDictionary
from magma.magma_constants import *

################################################
#   MetaWorkflowRunStep
################################################
class MetaWorkflowRunStep(ValidatedDictionary):
    """
    Class to represent a MetaWorkflow Run object,
    as a step within a MetaWorkflow Run Handler object.
    Assumption that this is based on ordered_meta_workflows (name) list
    from a MetaWorkflow Handler.
    """

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: dictionary representing a MetaWorkflow step (object) and accompanying info within handler
        :type input_dict: dict
        """
        super().__init__(input_dict)

        # for automatically setting initial status to "pending", unless explicitly defined not to
        if not hasattr(self, STATUS):
            setattr(self, STATUS, PENDING)

        # Validate presence of basic attributes of this MetaWorkflow step
        # TODO: make items_for_creation a required attr?
        # !!!AND!!! meta_workflow_run --> not necessarily, not defined until creation of mwfr
        self._validate_basic_attributes(NAME, DEPENDENCIES)

################################################
#   MetaWorkflowRunHandler
################################################
class MetaWorkflowRunHandler(ValidatedDictionary):
    """
    Class representing a MetaWorkflowRun Handler object,
    a list of MetaWorkflowsRuns with specified dependencies,
    and their status.
    """

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: MetaWorkflow Handler Run object
        :type input_dict: dict
        """

        ### Basic attributes ###

        super().__init__(input_dict)
        
        self._validate_basic_attributes(UUID, ASSOCIATED_META_WORKFLOW_HANDLER, META_WORKFLOW_RUNS)

        ### Calculated attributes ###

        # by nature of how a MetaWorkflowRun Handler is created from the MetaWorkflow Handler,
        # the array "meta_workflow_runs" will already be in some valid topologically sorted order
        # here, though, we create a dictionary of the form {mwfr_name: MetaWorkflowRunStep_object,...}
        # for faster lookup and updating of steps
        self.meta_workflow_run_steps_dict = self._set_meta_workflow_runs_dict()

        # initial final_status attribute upon creation should be "pending"
        # setattr(self, FINAL_STATUS, PENDING)
        self.update_final_status()


    def _set_meta_workflow_runs_dict(self):
        """
        Using meta_workflow_runs attribute (an array of MetaWorkflow Runs and their metadata),
        create a dictionary of the form {meta_workflow_run_name_a: meta_workflow_run_step_obj_a, ...},
        allowing for quicker lookup and updating of MetaWorkflowRunStep objects.

        :return: dictionary containing {MetaWorkflowRun name: MetaWorkflowRunStep object} key-value pairs
        """
        meta_workflow_run_step_dict = {}
        for meta_workflow_run in self.meta_workflow_runs:
            meta_workflow_run_step_object = MetaWorkflowRunStep(meta_workflow_run)
            step_name = meta_workflow_run[NAME]
            meta_workflow_run_step_dict[step_name] = meta_workflow_run_step_object
        return meta_workflow_run_step_dict


    def update_final_status(self):
        """
        Update final_status of handler based on combined statuses of
        all MetaWorkflowRunStep objects.

        If all steps are pending, final_status = pending.
        If a step is running and none others have failed or stopped, final_status = running.
        If all steps are completed, final_status = completed.
        If a step has failed, final_status = failed.
        If a step has been stopped, final_status = stopped.

        :return: final_status of the MetaWorkflow Run Handler
        :rtype: str
        """
        setattr(self, FINAL_STATUS, PENDING)

        all_steps_completed = True
        all_steps_pending = True

        for meta_workflow_run_step in self.meta_workflow_run_steps_dict.values():
            current_step_status = getattr(meta_workflow_run_step, STATUS)

            # checking if all steps are "completed" or "pending" and toggling corresponding flags
            if current_step_status != COMPLETED:
                all_steps_completed = False
            if current_step_status != PENDING:
                all_steps_pending = False

            # if step neither "completed" or "pending", update final_status accordingly
            if current_step_status == RUNNING:
                setattr(self, FINAL_STATUS, RUNNING)
            elif current_step_status == FAILED:
                setattr(self, FINAL_STATUS, FAILED)
                break
            elif current_step_status == STOPPED:
                setattr(self, FINAL_STATUS, STOPPED)
                break
                    
        # if all the steps were successfully completed
        if all_steps_completed:
            setattr(self, FINAL_STATUS, COMPLETED)

        # if all the steps were pending
        if all_steps_pending:
            setattr(self, FINAL_STATUS, PENDING)

        return getattr(self, FINAL_STATUS)


    def _retrieve_meta_workflow_run_step_obj_by_name(self, meta_workflow_run_name):
        """
        Given a MetaWorkflow Run name,
        retrieve its corresponding MetaWorkflowRunStep object.

        :param meta_workflow_run_name: name of MetaWorkflow Run to be retrieved
        :type meta_workflow_run_name: str
        :return: MetaWorkflowRunStep object corresponding to the given name
        :raises: KeyError if the MetaWorkflow Run name is invalid
        """
        try:
            step_obj = self.meta_workflow_run_steps_dict[meta_workflow_run_name]
            return step_obj
        except KeyError as key_err:
            raise KeyError("{0} is not a valid MetaWorkflowRun Step name.\n"
                                .format(key_err.args[0]))
        #TODO: sharding of mwfrs....
        

    def get_meta_workflow_run_step_attr(self, meta_workflow_run_name, attribute_to_fetch):
        """
        Given a MetaWorkflow Run name and an attribute to fetch,
        retrieve this attribute from the corresponding MetaWorkflowRunStep object,
        or None if the attribute to fetch doesn't exist on the MetaWorkflowRunStep object.

        :param meta_workflow_run_name: name of MetaWorkflow Run to be accessed
        :type meta_workflow_run_name: str
        :return: attribute_to_fetch's value from the MetaWorkflowRunStep object specified
        :rtype: varied, or None if not an existing attribute on the given Run Step
        :raises: KeyError if the MetaWorkflow Run name is invalid
        """
        step_obj = self._retrieve_meta_workflow_run_step_obj_by_name(meta_workflow_run_name)
        # Return the attribute_to_fetch
        return getattr(step_obj, attribute_to_fetch, None)


    def update_meta_workflow_run_step_obj(self, meta_workflow_run_name, attribute, value):
        """
        Given a MetaWorkflow Run name, an attribute to update, and value to update it to,
        retrieve its corresponding MetaWorkflowRunStep object by name
        and redefine the given attribute with the provided new value.

        :param meta_workflow_run_name: name of MetaWorkflow Run to be retrieved and updated
        :type meta_workflow_run_name: str
        :param attribute: attribute to update
        :type attribute: str
        :param value: new value of the updated attribute
        :type value: varies
        :raises: KeyError if the MetaWorkflow Run name is invalid
        """
        # Retrieve the specified step object
        step_obj = self._retrieve_meta_workflow_run_step_obj_by_name(meta_workflow_run_name)
        # Reset the given attribute
        setattr(step_obj, attribute, value)


    def pending_steps(self):
        """
        Returns a list of names of MetaWorkflowRunStep objects whose status is "pending".
        Returns empty list if none are pending.
        
        :returns: list of pending steps, by name
        :rtype: list[str]
        """
        pending_steps_list = []

        for meta_workflow_run_step in self.meta_workflow_runs:
            step_name = meta_workflow_run_step[NAME]
            if self.get_meta_workflow_run_step_attr(step_name, STATUS) == PENDING:
                pending_steps_list.append(step_name)
        
        return pending_steps_list

    def running_steps(self):
        """
        Returns a list of names of MetaWorkflowRunStep objects whose status is "running".
        Returns empty list if none are running.

        :returns: list of running steps, by name
        :rtype: list[str]
        """
        running_steps_list = []
        for meta_workflow_run in self.meta_workflow_runs:
            associated_meta_workflow_name = meta_workflow_run[NAME]
            if self.get_meta_workflow_run_step_attr(associated_meta_workflow_name, STATUS) == RUNNING:
                running_steps_list.append(associated_meta_workflow_name)
        
        return running_steps_list

    # TODO: move to ff because portal specific
    def update_meta_workflow_runs_array(self):
        """
        Following any updates to MetaWorkflowRunStep objects in meta_workflow_run_steps_dict,
        this method is called in order to update the original meta_workflow_runs array of dicts.
        Possible attributes that are updated are meta_workflow_run (a linkTo),
        status, and error.

        This allows for future PATCHing of a meta_workflow_runs array on the CGAP portal,
        by providing the updated meta_workflow_runs.

        :returns: updated meta_workflow_runs array
        """
        #TODO: make sure this works with sharding
        for meta_workflow_run_dict in self.meta_workflow_runs:
            meta_workflow_run_name = meta_workflow_run_dict[NAME]
            meta_workflow_run_linkto = self.get_meta_workflow_run_step_attr(meta_workflow_run_name, META_WORKFLOW_RUN)
            status = self.get_meta_workflow_run_step_attr(meta_workflow_run_name, STATUS)
            error = self.get_meta_workflow_run_step_attr(meta_workflow_run_name, ERROR)

            if meta_workflow_run_linkto:
                meta_workflow_run_dict[META_WORKFLOW_RUN] = meta_workflow_run_linkto
            if status:
                meta_workflow_run_dict[STATUS] = status
            if error:
                meta_workflow_run_dict[ERROR] = error

        return self.meta_workflow_runs