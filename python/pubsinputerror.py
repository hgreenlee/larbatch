#! /usr/bin/env python
######################################################################
#
# Name: pubsinputerror.py
#
# Purpose: Python class PubsInputError (exception class).
#          This exception is raised if input is temporarily unavailable
#          for a given (run, subrun) in pubs mode due to merging.
#
# Created: 22-May-2015  Herbert Greenlee
#
######################################################################

# PubsInputError class.

class PubsInputError(Exception):

    def __init__(self, run, subrun):
        self.run = run
        self.subrun = subrun
        return

    def __str__(self):
        return 'Input is not yet available for run %d, subrun %d.' % (
            self.run, self.subrun)
