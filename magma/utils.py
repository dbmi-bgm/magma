#!/usr/bin/env python3

################################################
#   Libraries
################################################

################################################
#   Functions
################################################
#TODO: description -- small utility fxns and
# object attribute checking

#TODO: following 2 fxn not used elsewhere but could be in future
def check_variable_type(variable, intended_type):
    """
    Checks that given variable is of the intended type.

    :param variable: variable to be checked
    :type variable: data type
    :param intended_type: the variable type that is intended
    :type intended_type: data type
    :return: True if the variable if of the intended_type, else False
    :rtype: bool
    """
    if not isinstance(variable, intended_type):
        return False
    else:
        return True

def check_list_elements_type(list_to_check, intended_type):
    """
    Checks that all elements in  list are of a given type.
    Raises Exception if not all elements are strings.

    :param list_to_check: list to be checked
    :type list_to_check: list
    :param intended_type: the variable type that is intended
    :type intended_type: data type
    :return: True if all elements of list_to_check are of the intended_type, else False
    :rtype: bool
    """
    # check that all elements in list are strings
    if not all(isinstance(element, intended_type) for element in list_to_check):
        return False
    else:
        return True

def check_presence_of_attributes(input_object, attributes_to_check=None):
    """
    Takes in an object and a list of attributes, checks that those attributes are present
    in this object

    :param input_object: object to check
    :type input_object: object (instance of some class)
    :param attributes_to_check: list of attributes to check
    :type attributes_to_check: list[str]
    :return: None, if there are no attributes to check
    :return: None, if all specified attributes are present
    :raises ValueError: if input_object doesn't have a specified attribute
    """
    #TODO: make the next three commands its own helper function? I repeat variations
    # several times
    if attributes_to_check is None:
        return

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
    :return: None, if there are no attributes to set
    :return: None, once entire function is completed with no errors
    """
    if attributes_to_set is None:
        return

    # especially if we are handling duplicates in reordering list function
    for attribute in attributes_to_set:
        if not hasattr(input_object, attribute):
            # if not present, set attribute as empty list
            setattr(input_object, attribute, [])
        else:
            attrib = getattr(input_object, attribute)

            # check if this attribute is a list
            if check_variable_type(attrib, list):
                # then get rid of duplicates, if present
                non_dup_attrib = []
                for item in attrib:
                    if item not in non_dup_attrib:
                        non_dup_attrib.append(item)
                setattr(input_object, attribute, non_dup_attrib)
            else: 
                continue