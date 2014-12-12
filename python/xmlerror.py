#! /usr/bin/env python
######################################################################
#
# Name: xmlerror.py
#
# Purpose: Python class XMLError (exception class).
#
# Created: 12-Dec-2014  Herbert Greenlee
#
######################################################################

# XML exception class.

class XMLError(Exception):

    def __init__(self, s):
        self.value = s
        return

    def __str__(self):
        return self.value

