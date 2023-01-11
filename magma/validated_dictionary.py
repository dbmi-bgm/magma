################################################
#   TODO: functions for dcic utils -- move later
################################################
from magma.utils import check_presence_of_attributes


################################################
#   ValidatedDictionary
################################################
class ValidatedDictionary(object):
    """
    Parent class for MetaWorkflowStep and MetaWorkflowHandler classes.
    Takes in an input dictionary, and validates basic attributes.
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

    def _validate_basic_attributes(self, list_of_attributes=None):
        """ TODO: make list of attributes a class attribute
        Validation of the JSON input for this object.
        Checks that given attributes are present in the created object.
        """
        check_presence_of_attributes(self, list_of_attributes)