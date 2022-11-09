#!/usr/bin/env python3

################################################
#   Libraries
################################################
from copy import deepcopy
from magma.utils import check_variable_type

################################################
#   Functions
################################################

def check_presence_of_key(list_of_dicts, key_to_check=None):
    """
    Takes in a list of dictionaries and a list of keys, checks that those keys 
    are present within every99 dict in this list/array.

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

#TODO: could make this more general...
def set_dependency_list_values(list_of_dicts, name_of_step_key, name_of_dependencies_key, existing_steps_list):
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
    """

    list_of_dicts_copy = deepcopy(list_of_dicts) #TODO: make sure original doesnt change in test
    # iterate through list of dicts and set dependencies key-value pair
    for dictionary in list_of_dicts_copy:
        # add empty dependency list if not present
        if not name_of_dependencies_key in dictionary:
            dictionary[name_of_dependencies_key] = []
            continue
            #TODO: do some renaming of this function to follow pattern of obj vs dict key setting?

        # get rid of duplicates
        # I choose this method for generalization, in the case that dependencies is
        # a list of dictionaries, which are an unhashable type
        dependencies = dictionary[name_of_dependencies_key]
        # check this is indeed a list
        if not check_variable_type(dependencies, list):
            dictionary[name_of_dependencies_key] = []
            continue
            #TODO: throw exception here instead of resetting value

        # get rid of duplicates and self-dependencies
        # and each dependency is in fact a name of another metaworkflow
        non_duplicated_dependencies = []
        dictionary_name = dictionary[name_of_step_key]
        for dependency in dependencies:
            if (dependency not in non_duplicated_dependencies) and (dependency != dictionary_name) and (dependency in existing_steps_list):
                non_duplicated_dependencies.append(dependency)
                #TODO: throw exception for self dependencies, duplicates, or nonexistent names?
        dictionary[name_of_dependencies_key] = non_duplicated_dependencies
        # dictionary["steps_after"] = []

    return list_of_dicts_copy

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

    for previous_step in curr_node[name_of_dependencies_key]:
        #TODO: can't have duplicates in names with this method!
        idx_previous_node, previous_node = find_index_with_given_step_name(graph, name_of_node_key, previous_step)
        topological_sort_DFS_helper(graph, previous_node, idx_previous_node, name_of_node_key, name_of_dependencies_key, visited_temporary, visited_permanent, queue)

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

    #TODO: remove the following, just for testing
    for element in queue:
        print(element["name"])

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

    names = generate_ordered_step_name_list(steps_with_dependencies_list, name_of_step_key)

    ## Preprocessing of dependencies lists
    # add dependencies attribute if not present, remove duplicates from dependencies,
    # and check for self dependencies
    preprocessed_steps_with_dependencies_list =  set_dependency_list_values(steps_with_dependencies_list, name_of_step_key, name_of_dependencies_key, names)

    # import pdb; pdb.set_trace()
    ordered_steps_list = topological_sort(preprocessed_steps_with_dependencies_list, name_of_step_key, name_of_dependencies_key)
            
    return ordered_steps_list

    # TODO:edge cases: all steps have dependencies (cycle or deleted self-dependency), no steps depending on each other, dependency on self, identical steps