#! /usr/bin/env python
######################################################################
#
# Name: pubsdeadenderror.py
#
# Purpose: Python class PubsDeadEndError (exception class).
#          This exception is raised if input is permanently unavailable
#          for a given (run, subrun) in pubs mode due to merging.
#
# Created: 22-May-2015  Herbert Greenlee
#
######################################################################

# PubsDeadEndError class.

class PubsDeadEndError(Exception):

    def __init__(self, run, subrun):
        self.run = run
        self.subrun = subrun
        return

    def __str__(self):
        return 'Input is permanently unavailable for run %d, subrun %d.' % (
            self.run, self.subrun)
