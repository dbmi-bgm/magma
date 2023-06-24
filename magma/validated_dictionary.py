#!/usr/bin/env python3

################################################
#   ValidatedDictionary TODO: eventually make part of dcicutils?
################################################
class ValidatedDictionary(object):
    """
    Parent class for MetaWorkflow(Run)Step and MetaWorkflow(Run) Handler classes.
    Takes in an input dictionary, and validates basic attributes (makes sure given attributes are present).
    """

    def __init__(self, input_dict):
        """
        Constructor method, initialize object and attributes.

        :param input_dict: input dictionary, defined by json file, which defines basic attributes of this object
        :type input_dict: dict
        """
        # Set basic (non-calculated) attributes #
        for key in input_dict:
            setattr(self, key, input_dict[key])

    def _validate_basic_attributes(self, *attributes_to_check):
        """
        Validation of the JSON input for this object.
        Checks that given attributes are present in the created object.

        :param attributes_to_check: attributes that are checked (variable number of non-keyword arguments)
        :type attributes_to_check: str(s)
        :return: None, if all specified attributes are present
        :raises ValueError: if this Validated Dictionary object doesn't have a specified attribute
        """
        for attribute in attributes_to_check:
            try:
                # retrieved_attr = getattr(self, attribute)
                getattr(self, attribute)
                # if retrieved_attr is None: # if not retrieved_attr --> for falsy values                    raise AttributeError("attribute %s cannot have value 'None'." % attribute)
                    # TODO: add this to the pytests
            except AttributeError as e:
                raise AttributeError("Object validation error, {0}\n"
                                    .format(e.args[0]))