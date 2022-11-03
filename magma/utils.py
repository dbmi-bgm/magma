#!/usr/bin/env python3

################################################
#   Libraries
################################################
from copy import deepcopy

################################################
#   Functions
################################################

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

def check_presence_of_key(list_of_dicts, key_to_check=None):
    """
    Takes in a list of dictionaries and a list of keys, checks that those keys 
    are present within every dict in this list/array.

    :param list_of_dicts: dictionaries to check
    :type input_dict: list[dict]
    :param key_to_check: key to check
    :type key_to_check: str
    :return: True, if the specified key is present in each dict, or there is no 
                                key to check, else False
    :rtype: bool
    """
    #TODO: make the next three commands its own helper function? I repeat variations
    # several times -- on this note, look up how to have flexible argument
    if key_to_check is None:
        return True

    if not all((key_to_check in dictionary) for dictionary in list_of_dicts):
        return False

    return True

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

#TODO: could make this more general...
def set_dependency_list_values(list_of_dicts, name_of_step_key, name_of_dependencies_key):
    """
    Checks for dependency key within each dictionary in list_of_dicts.
    If not present, add that key and set value as empty list.
    Else, remove duplicates and self-dependencies.

    :param list_of_dicts: list of dictionaries that should hold a dependency list. Each
                                dictionary corresponds to a step, and the list represents
                                a "list of steps" with dependencies between them.
    :type list_of_dicts: list[dict]
    :param name_of_step_key: name of the key corresponding to the step's name –
                                    i.e. attribute referred to by dependency values
    :type name_of_step_key: str
    :param name_of_dependencies_key: name of the key corresponding to the dependencies list
    :type name_of_dependencies_key: str
    :return: a copy of list_of_dicts with appropriate dependency lists set
    :rtype: list[dict]
    :raises TypeError: if name_of_dependencies_key is not a string
    """

    list_of_dicts_copy = deepcopy(list_of_dicts)
    # iterate through list of dicts and set dependencies key-value pair
    for dictionary in list_of_dicts_copy:
        # add empty dependency list if not present
        if not name_of_dependencies_key in dictionary:
            dictionary[name_of_dependencies_key] = []

        # get rid of duplicates
        # I choose this method for generalization, in the case that dependencies is
        # a list of dictionaries, which are an unhashable type
        dependencies = dictionary[name_of_dependencies_key]
        non_duplicated_dependencies = []
        for dependency in dependencies:
            if dependency not in non_duplicated_dependencies:
                non_duplicated_dependencies.append(dependency)
        dictionary[name_of_dependencies_key] = non_duplicated_dependencies
        #TODO: note -- im working under the assumption that because of the limitations
        # of the schema, the dependencies will be of the correct type. Must I include
        # a check that each dependency is in fact a name of another metaworkflow?
        #(...probably. :/ )

        # check for self-dependencies
        new_dependencies = dictionary[name_of_dependencies_key] #repetitive, but just for readability
        dictionary_name = dictionary[name_of_step_key]
        # remove from this list
        #TODO: should I throw exception instead? I think it's fine to just remove bc it's easy
        new_dependencies = list(filter(lambda element: element != dictionary_name, new_dependencies))

    return list_of_dicts_copy

def generate_ordered_step_name_list(list_of_dicts, name_of_step_key):
    """
    Based on a list of dictionaries (representing a list of steps) with a "name" key
    for each dictionary, return a list of the names of each dictionary with
    indices corresponding to the indices of the dictionaries themselves (same order).
    """
    names = []
    for dictionary in list_of_dicts:
        names.append(dictionary[name_of_step_key])
    return names
    #TODO: in test, check that it is always in the same order

def define_forward_dependencies(list_of_dicts, name_of_step_key, name_of_dependencies_key):
    """
    Build directed graph by "reversing" dependencies TODO: redo comment
    """
    names = generate_ordered_step_name_list(list_of_dicts, name_of_step_key)

    for dictionary in list_of_dicts:
        current_dependencies = dictionary[name_of_dependencies_key]
        current_dict_name = dictionary[name_of_step_key]

        # go through each step this current step is dependent on
        # and add "step_after" attribute
        # (dependencies are "origin", "source", or "progenitor" steps)
        for dependency in current_dependencies:
            # isolate the index of the dependency using names list
            #TODO: this matches to the first occurence of dependency within the array 
            idx = names.index(dependency)
            dependency_step_dict = list_of_dicts[idx]

            #TODO: consider helper fxn? but not necessary
            #TODO: rename this attribute
            if not ("steps_after" in dependency_step_dict):
                dependency_step_dict["steps_after"] = []

            dependency_step_dict["steps_after"].append(current_dict_name)

def find_index_with_given_step_name(steps_with_dependencies_list, name_of_step_key, name):
    for index, step in enumerate(steps_with_dependencies_list):
        if step[name_of_step_key] == name:
            return index, step

def topological_sort_DFS_helper(graph, curr_node, curr_idx, name_of_node_key, name_of_dependencies_key, visited_temporary, visited_permanent, queue):
    if visited_permanent[curr_idx]:
        return queue
    if visited_temporary[curr_idx]:
        raise Exception("cycle in graph!: node " + curr_node[name_of_node_key])
    
    visited_temporary[curr_idx] = True

    for following_step in curr_node[name_of_dependencies_key]:
        #TODO: can't have duplicates in names with this method!
        idx_following_node, following_node = find_index_with_given_step_name(graph, name_of_node_key, following_step)
        topological_sort_DFS_helper(graph, following_node, idx_following_node, name_of_node_key, name_of_dependencies_key, visited_temporary, visited_permanent, queue)

    visited_temporary[curr_idx] = False
    visited_permanent[curr_idx] = True
    queue.append(curr_node) 
    return queue
    # TODO: for test, can check that there are no duplicates in returned queue   


def topological_sort(graph, name_of_node_key, name_of_dependencies_key):
    """
    DFS algorithm from wikipedia https://en.wikipedia.org/wiki/Topological_sorting 
    Logic based on topological sort of directed graph from https://www.geeksforgeeks.org/topological-sorting/
    TODO: finish this docstring l8r
    Time complexity = O(V+E), where V = # vertices/nodes (steps), E = # edges (in directed graph, dependencies)
    https://www.geeksforgeeks.org/detect-cycle-in-directed-graph-using-topological-sort/?id=discuss = cycle detection  :
    So, in detail, just do a topological sort and get the queue of the results. Then as you pop from the final queue and
    push to your result vector/array, check all the adjacent nodes of the last popped item and if the adjacent node 
    exists in the vector then it's a cycle (if A goes to B then B should not precede A in the topological ordering). 
    ASSUMPTOPN: no self loops (i deletd them)

    pseudocode from wikipedia:
    L ← Empty list that will contain the sorted nodes
    while exists nodes without a permanent mark do
        select an unmarked node n
        visit(n)

    function visit(node n)
        if n has a permanent mark then
            return
        if n has a temporary mark then
            stop   (graph has at least one cycle)

        mark n with a temporary mark

        for each node m with an edge from n to m do
            visit(m)

        remove temporary mark from n
        mark n with a permanent mark
        add n to head of L
    """
    num_steps = len(graph)
    visited_temporary = [False]*num_steps
    visited_permanent = [False]*num_steps
    queue = [] #First In First Out

    while not all((element == True) for element in visited_permanent):
        curr_idx = visited_permanent.index(False)
        curr_node = graph[curr_idx]
        #calling recursive helper function
        queue = topological_sort_DFS_helper(graph, curr_node, curr_idx, name_of_node_key, name_of_dependencies_key, visited_temporary, visited_permanent, queue)

    # for element in queue:
    #     print(element["name"])

    return queue

def generate_ordered_steps_list(steps_with_dependencies_list, name_of_step_key, name_of_dependencies_key):
    """
    Takes in list of steps and reorders based on dependencies, returning a separate copy of
    a reordered list.
    If impossible to create ordered list (circular dependencies, missing steps, etc.),
    throws error or exception.

    :param steps_with_dependencies_list: list of dictionaries, where each dictionary has 
                                    at least a step name
    :type steps_with_dependencies_list: list[dict]
    :param name_of_step_key: name of the key corresponding to the step's name –
                                    i.e. attribute referred to by dependency values
    :type name_of_step_key: str
    :param name_of_dependencies_key: name of the key corresponding to the dependencies list
    :type name_of_dependencies_key: str
    :return: a copy of the reordered list (if possible)
    :rtype: list[dict]
    TODO: add errors and exceptions possibly thrown
    """

    # check that all objects in steps_with_dependencies_list have name_of_step_key
    if not check_presence_of_key(steps_with_dependencies_list, name_of_step_key):
        raise Exception("All dictionary elements in steps_with_dependencies_list must have attribute \"{0}\"".format(name_of_step_key))

    # TODO: feel like this is overkill, but checking for duplicates in steps_with_dependencies_list?
    # is there any case where the exact same step will be defined?? i think gets tricky with sharding maybe? idk

    ### List reordering based on dependencies ###

    ## Preprocessing of dependencies lists
    # add dependencies attribute if not present, remove duplicates from dependencies,
    # and check for self dependencies
    preprocessed_steps_with_dependencies_list =  set_dependency_list_values(steps_with_dependencies_list, name_of_step_key, name_of_dependencies_key)
    
    ## Build directed graph by "reversing" dependencies (TODO: redo this comment and make own function)
    define_forward_dependencies(preprocessed_steps_with_dependencies_list, name_of_step_key, name_of_dependencies_key) 

    ordered_steps_list = topological_sort(preprocessed_steps_with_dependencies_list, name_of_step_key, name_of_dependencies_key)
            
    return ordered_steps_list


    # TODO:edge cases: all steps have dependencies, no steps depending on each other, dependency on self, identical steps
