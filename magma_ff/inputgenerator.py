#!/usr/bin/env python3

################################################
#
#   InputGenerator ff
#
################################################

################################################
#   Libraries
################################################
import sys, os

# tibanna
from tibanna.utils import create_jobid

# magma
from magma.inputgenerator import InputGenerator as InputGeneratorFromMagma
from magma.inputgenerator import Argument

################################################
#   InputGenerator
################################################
class InputGenerator(InputGeneratorFromMagma):

    def __init__(self, wfl_obj, wflrun_obj):
        super().__init__(wfl_obj, wflrun_obj)

        # Output from ff use file as key
        #      instead of files for file argument value
        self.file_key = 'file'
    #end def

#end class
