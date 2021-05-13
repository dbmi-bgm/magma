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
#   Objects
################################################
class InputGenerator(object):
    '''
    '''

    def __init__(self, wfl_obj, wflrun_obj):
        '''

                wfl_obj, MetaWorkflow object representing a meta-workflow
                wflrun_obj, MetaWorkflowRun object representing a meta-workflow-run
        '''
        self.wfl_obj = wfl_obj
        self.wflrun_obj = wflrun_obj
    #end def

    def input_generator():
        '''
        '''
        for run_obj in self.wflrun_obj.to_run():

    #end def

#end class
