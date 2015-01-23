#! /usr/bin/env python
######################################################################
#
# Name: stagestatus.py
#
# Purpose: Python class StageStatus (used by project.py script).
#          This class contains information about project stage status.
#
# Created: 12-Dec-2014  Herbert Greenlee
#
######################################################################

import os, string
from stagedef import StageDef
import project_utilities

# Project status class.

class StageStatus:

    # Constructor.

    def __init__(self, stage):

        # Default values.

        self.stage = stage               # Stage name.
        self.exists = False              # Does logdir exist?
        self.nfile = 0                   # Number of good files.
        self.nev = 0                     # Number of good events.
        self.nerror = 0                  # Number of workers with errors.
        self.nmiss = 0                   # Number of unprocessed input files.

        # Update data.
        
        self.update()

    # Update data for this stage.

    def update(self):

        logdir = self.stage.logdir

        # Test whether output directory exists.

        if project_utilities.safeexist(logdir):

            # Output directory exists.

            self.exists = True
            self.nfile = 0
            self.nev = 0
            self.nerror = 0
            self.nmiss = 0

            # Count good files and events.

            eventsfile = os.path.join(logdir, 'events.list')
            if project_utilities.safeexist(eventsfile):
                lines = project_utilities.saferead(eventsfile)
                for line in lines:
                    words = string.split(line)
                    if len(words) >= 2:
                        self.nfile = self.nfile + 1
                        self.nev = self.nev + int(words[1])

            # Count errors.

            badfile = os.path.join(logdir, 'bad.list')
            if project_utilities.safeexist(badfile):
                lines = project_utilities.saferead(badfile)
                self.nerror = self.nerror + len(lines)

            # Count missing files.

            missingfile = os.path.join(logdir, 'missing_files.list')
            if project_utilities.safeexist(missingfile):
                lines = project_utilities.saferead(missingfile)
                self.nmiss = self.nmiss + len(lines)

        else:

            # Output directory does not exist.

            self.exists = False

