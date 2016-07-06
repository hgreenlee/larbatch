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
from project_modules.stagedef import StageDef
import project_utilities
import larbatch_posix

# Project status class.

class StageStatus:

    # Constructor.

    def __init__(self, stage):

        # Default values.

        self.stage = stage               # Stage name.
        self.exists = False              # Does logdir exist?
        self.nfile = 0                   # Number of good art files.
        self.nev = 0                     # Number of good art events.
        self.nana = 0                    # Number of good non-art root files.
        self.nerror = 0                  # Number of workers with errors.
        self.nmiss = 0                   # Number of unprocessed input files.

        # Update data.
        
        self.update()

    # Update data for this stage.

    def update(self):

        logdir = self.stage.logdir

        # Test whether output directory exists.

        if larbatch_posix.exists(logdir):

            # Output directory exists.

            self.exists = True
            self.nfile = 0
            self.nev = 0
            self.nana = 0
            self.nerror = 0
            self.nmiss = 0

            # Count good files and events.

            eventsfile = os.path.join(logdir, 'events.list')
            if larbatch_posix.exists(eventsfile):
                lines = larbatch_posix.readlines(eventsfile)
                for line in lines:
                    words = string.split(line)
                    if len(words) >= 2:
                        self.nfile = self.nfile + 1
                        self.nev = self.nev + int(words[1])

            # Count good files analysis root files.

            filesana = os.path.join(logdir, 'filesana.list')
            if larbatch_posix.exists(filesana):
                lines = larbatch_posix.readlines(filesana)
                for line in lines:
                    self.nana = self.nana + 1

            # Count errors.

            badfile = os.path.join(logdir, 'bad.list')
            if larbatch_posix.exists(badfile):
                lines = larbatch_posix.readlines(badfile)
                for line in lines:
                    if line.strip():
                        self.nerror += 1

            # Count missing files.

            missingfile = os.path.join(logdir, 'missing_files.list')
            if larbatch_posix.exists(missingfile):
                lines = larbatch_posix.readlines(missingfile)
                for line in lines:
                    if line.strip():
                        self.nmiss += 1

        else:

            # Output directory does not exist.

            self.exists = False

