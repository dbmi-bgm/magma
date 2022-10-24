#!/usr/bin/env python3

################################################
#   Libraries
################################################

################################################
#   Functions
################################################

def check_variable_type(variable, intended_type):
    """
    Checks that given variable is of the intended type.
    Raises TypeError if not of the intended type.
    If it matches, returns None.

    :param variable: variable to be checked
    :type variable: depends?? (TODO: lol check how to define this)
    :param intended_type: the variable type that is intended
    :type intended_type: also....depends..(TODO:)
    :raises TypeError: if variable is of incorrect/unintended type
    """
    if not isinstance(variable, intended_type):
        raise TypeError("Input must be of type {0}".format(str(intended_type)))

def check_list_elements_type(list_to_check, intended_type):
    """
    Checks that all elements in  list are of a given type.
    Raises Exception if not all elements are strings.

    :param list_to_check: list to be checked
    :type list_to_check: list
    :param intended_type: the variable type that is intended
    :type intended_type: also....depends..(TODO:)
    :raises TypeError: if list_to_check is of incorrect type (not a list)
    :raises Exception: if not all list elements are of the intended type
    TODO: should this exception also be a TypeError
    """
    # Check that input is of type list
    check_variable_type(list_to_check, list)

    # check that all elements in list are strings
    if not all(isinstance(element, intended_type) for element in list_to_check):
        raise Exception("All elements in list must be of type {0}".format(str(intended_type)))

def check_presence_of_attributes(input_object, attributes_to_check=None):
    """
    Takes in an object and a list of attributes, checks that those attributes are present
    in this object

    :param input_object: object to check
    :type input_object: object (dict)
    :param attributes_to_check: list of attributes to check
    :type attributes_to_check: list[str]
    :raises ValueError: if object doesn't have a specified attribute

    TODO: should this have a return? right now it just raises errors or not
    """
    if attributes_to_check is None:
        return

    # Check that attributes_to_check is of type list
    check_variable_type(attributes_to_check, list)

    # check that all attributes to be checked are strings
    check_list_elements_type(attributes_to_check, str)

    for attribute in attributes_to_check:
        try:
            getattr(input_object, attribute)
        except AttributeError as e:
            raise ValueError("Object validation error, {0}\n"
                                .format(e.args[0]))

def set_list_attributes(input_object, attributes_to_set=None):
    """
    Checks for given attribute(s) of type list, sets as empty list if not present,
    else sets that list attribute, without duplicates.

    :param input_object: object with attributes to be set
    :type input_object: object (dict)
    :param attributes_to_set: list of attributes to set
    :type attributes_to_set: list[str]
    """
    if attributes_to_set is None:
        return
    # check that all attributes to be checked are strings
    check_list_elements_type(attributes_to_set, str)

    # especially if we are handling duplicates in reordering list function
    for attribute in attributes_to_set:
        if not hasattr(input_object, attribute):
            # if not present, set attribute as empty list
            setattr(input_object, attribute, [])

def generate_ordered_steps_list(steps_with_dependencies_array, name_of_step_attribute, name_of_dependency_attribute):
    """
    Takes in list of steps and reorders based on dependencies, returning reordered list.
    If impossible to create ordered list (circular dependencies, missing steps, etc.),
    returns None. TODO: check this -- it'll throw an exception/error, not return None

    :param steps_with_dependencies_array: list of objects, where each object has (at least) step and dependency attribute
    #TODO: check the above line -- dependency may not be necessary
    :type steps_with_dependencies_array: list of dicts/objects
    :param name_of_step_attribute: name of the key corresponding to the step's name –
                                    i.e. attribute referred to by dependency values
    :type name_of_step_attribute: str
    :param name_of_dependency_attribute: name of the key corresponding to the dependencies list
    :type name_of_dependency_attribute: str
    :return: the reordered list (if possible)
    :rtype: list of dicts/objects
    TODO: add errors and exceptions possibly thrown
    """
    # check that name_of_step_attribute is a string
    check_variable_type(name_of_step_attribute, str)

    # check that steps_with_dependencies_array is a list of objects/dicts
    check_list_elements_type(steps_with_dependencies_array, object)
    #TODO: does json "sub" object of type object or dict??
    # ^ also what are we actually working with here
    # ^ because json.load turns it into a dict....
    # and the MWFH class has those subparts as type dict (from manual testing)
    # solved: isinstance takes care of this anyway??...do we want that differentiation tho
    
    # check that all objects in steps_with_dependencies_array have name_of_step_attribute
    if not all(hasattr(element, name_of_step_attribute) for element in steps_with_dependencies_array):
        raise Exception("All elements in list must have attribute \"{0}\"".format(name_of_step_attribute))
    # TODO: make this a utility function -- also, should it raise exception or error

    # TODO: random, but we never make the check that the same key is used twice in dict
    # by default, python takes the value as the lastmost definition of that key
    # i don't think we need to worry about this

    # TODO: feel like this is overkill, but checking for duplicates in steps_with_dependencies_array?
    # is there any case where the exact same step will be defined?? i think gets tricky with sharding maybe? idk

    ### List reordering based on dependencies ###

    ## Preprocessing of dependencies lists -- TODO: could make this its own function
    # add dependencies attribute if not present, remove duplicates from dependencies,
    # and check for self dependencies  
    for step in steps_with_dependencies_array:
        # add empty dependency list if not present
        # TODO: setting of dependency to []? so that it at least has a value
        # VERSUS not having the dependency attribute at all -- I'm sticking w latter for now
        if not getattr(step, name_of_dependency_attribute, None): #TODO: use helper function above instead
            setattr(step, name_of_dependency_attribute, [])

        # get rid of duplicates -- TODO: make a helper function?
        dependencies = getattr(step, name_of_dependency_attribute)
        setattr(step, name_of_dependency_attribute, list(set(dependencies)))

        # check for self dependencies -- if present, throw exception (TODO: check this)
        dependencies = getattr(step, name_of_dependency_attribute) # list of dependencies
        name = getattr(step, name_of_step_attribute)
        if name in dependencies:
            raise Exception("Self dependency for step \"{0}\" not allowed".format(name))

    ## Build directed graph by "reversing" dependencies (TODO: redo this comment and make own function)

    # make list of "name" values, whose indices correspond to indices of the objects in steps_with_dependencies_array
    names = []
    for obj in steps_with_dependencies_array:
        names.append(getattr(obj, name_of_step_attribute)) #TODO:alternatively, do in the above for loop

    for step in steps_with_dependencies_array:
        dependencies = getattr(step, name_of_dependency_attribute)
        
        # go through each step this current step is dependent on
        # and add "step_after" attribute
        # (dependencies are "origin", "source", or "progenitor" steps)
        for dependency in dependencies:


            # if this dependency step doesn't have the step_after attribute, create it
            if not getattr(dependency, "step_after", None): #TODO: rename this attribute, use helper fxn here
                setattr(dependency, "step_after", [])


    # TODO:edge cases: all steps have dependencies, no steps depending on each other, dependency on self

    # TODO: should I do resetting of list attribute (reordered) from original source object here or outside?
    # (like, the pass by reference problem. look this up for python)
    # check this in use of other helper functions too
    # here, I have chosen to return the reordered array and redefine the metaworkflows list in main MWFH class
