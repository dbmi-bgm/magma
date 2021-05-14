#!/usr/bin/env python3

################################################
#
#   Library of utilities to work with
#       meta-workflow and meta-workflow-run objects
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
#   Templates
################################################
# {
#   “app_name”: “workflow_vep-annot-check”,
#   “workflow_uuid”: “cgap:workflow_vep-annot-check_v22",
#   “parameters”: {
#     “nthreads”: 64
#   },
#   “config”: {
#     “instance_type”: “c5n.18xlarge”,
#     “ebs_size”: “0.25x”,
#     “EBS_optimized”: true,
#     “spot_instance”: true,
#     “log_bucket”: “tibanna-output”,
#     “key_name”: “4dn-encode”,
#     “public_postrun_json”: true,
#     “behavior_on_capacity_limit”: “wait_and_retry”
#   },
#   “custom_pf_fields”: { #meta-workflow
#     “annotated_vcf”: {
#       “file_type”: “vep-annotated VCF”,
#       “description”: “vep-annotated VCF file”,
#       “genome_assembly”: “GRCh38" # from foursight
#     }
#   },
#    “custom_qc_fields”: { #meta-workflow
#      “annotated_vcf”: {
#        “file_type”: “vep-annotated VCF”,
#        “description”: “vep-annotated VCF file”,
#        “genome_assembly”: “GRCh38"
#      }
#    },
#   “common_fields”: { #foursight
#     “project”: “/projects/brigham-genomic-medicine/“,
#     “institution”: “/institutions/bwh/”
#   },
#   “input_files”: [
#     {
#       “workflow_argument_name”: “CADD_indel”,
#       “uuid”: “b9f123dd-be05-4a14-957a-5e1e5a5ce254”,
#       “mount”: true
#     },
#     {
#       “workflow_argument_name”: “spliceai_snv”,
#       “uuid”: “a35e580c-7579-4312-a3a1-66810e6d9366"
#       “mount”: true
#     },
#     {
#       “workflow_argument_name”: “vep”
#       “uuid”: “4ff30e10-aa8b-4ebf-87ac-20c7bfc15a46”
#     }
#   ],
#   “_tibanna”: { # foursight
#     “run_id”: “GAPFIQOGRXEU”, #foursight, is the sample
#     “env”: “fourfront-cgap” #foursight
#     “run_type”: “workflow_vep-annot-check” #meta-workflow
#   }
# }
################################################
#   Functions
################################################

################################################
#   Objects
################################################
class Argument(object):
    '''
    '''

    def __init__(self, input_json):
        '''
        '''
        # Basic attributes
        for key in input_json:
            setattr(self, key, input_json[key])
        #end for
        #TODO, add some validation for the attributes
        #   to streamline format errors identification
        # Calculated attributes
        if not getattr(self, 'source_argument_name', None):
            self.source_argument_name = self.argument_name
        #end if
    #end def

#end class

class InputGenerator(object):
    '''
    '''

    def __init__(self, wfl_obj, wflrun_obj):
        '''

                wfl_obj, MetaWorkflow object representing a meta-workflow
                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
        '''
        # Basic attributes
        self.wfl_obj = wfl_obj
        self.wflrun_obj = wflrun_obj
    #end def

    def input_generator(self):
        '''
        '''
        out_ = []
        # Get workflow-runs that need to be run
        for run_obj in self.wflrun_obj.to_run():
            # Get workflow-run arguments
            run_args = self._run_arguments(run_obj)
            # Match and update workflow-run arguments
            #   file arguments -> uuid
            #   parameter arguments -> value
            self._match_arguments(run_args, run_obj)
            out_.append((run_obj, run_args))
            ### This is where formatting happens,
            #       to change formatting just change this part
        #end for
        return out_
    #end def

    def _run_arguments(self, run_obj):
        '''
                run_obj, is a WorkflowRun object for a workflow-run
        '''
        run_args = []
        for arg in self.wfl_obj.steps[run_obj.name].arguments:
            arg_obj = Argument(arg)
            run_args.append(arg_obj)
        #end for
        return run_args
    #end def

    def _match_arguments(self, run_args, run_obj):
        '''
                run_args, is a list of Argument objects for a workflow-run
        '''
        for arg_obj in run_args:
            # Check argument type
            argument_type = getattr(arg_obj, 'argument_type')
            if argument_type == 'file':
                is_match = self._match_argument_file(arg_obj, run_obj)
            else: # is parameter
                is_match = self._match_argument_parameter(arg_obj)
            #end if
            if not is_match:
                raise ValueError('Value error, cannot find a match for argument {0}\n'
                                    .format(arg_obj.argument_name))
            #end if
        #end for
    #end def

    def _match_argument_file(self, arg_obj, run_obj):
        '''
        '''
        if getattr(arg_obj, 'source_step', None):
        # Is workflow-run dependency, match to workflow-run output
            uuid_ = []
            for dependency in run_obj.dependencies:
                if arg_obj.source_step in dependency:
                    for arg in self.wflrun_obj.runs[dependency].output:
                        if arg_obj.source_argument_name == arg['argument_name']:
                            uuid_.append(arg['uuid'])
                            break
                        #end if
                    #end for
                #end if
            #end for
            if len(uuid_) > 1: #TODO handle gather better
                arg_obj.uuid = uuid_
            else:
                arg_obj.uuid = uuid_[0]
            #end if
            return True
        else:
        # No dependency, match to general argument
            return self._match_general_argument(arg_obj)
        #end if
    #end def

    def _match_argument_parameter(self, arg_obj):
        '''
        '''
        if getattr(arg_obj, 'value', None) != None:
            # Is value
            return True
        else:
        # No value, match to general argument
            return self._match_general_argument(arg_obj)
        #end if
    #end def

    def _match_general_argument(self, arg_obj):
        '''
        '''
        # Try and match with meta-worfklow-run input
        if self._value(arg_obj, self.wflrun_obj.input):
            return True
        #end if
        # No match, try match to default argument in meta-worfklow
        if self._value(arg_obj, self.wfl_obj.arguments):
            return True
        #end if
        return False
    #end def

    def _value(self, arg_obj, arg_list):
        '''
                arg_obj, is Argument object for a workflow-run
                arg_list, is a list of arguments as dictionaries
        '''
        for arg in arg_list:
            if arg_obj.source_argument_name == arg['argument_name'] and \
               arg_obj.argument_type == arg['argument_type']:
                if arg_obj.argument_type == 'file':
                    arg_obj.uuid = arg['uuid']
                else:
                    arg_obj.value = arg['value']
                #end if
                return True
            #end if
        #end for
        return False
    #end def

#end class
