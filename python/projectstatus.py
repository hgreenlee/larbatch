#! /usr/bin/env python
######################################################################
#
# Name: projectstatus.py
#
# Purpose: Python class ProjectStatus (used by project.py script).
#          This class contains information about project status.
#
# Created: 12-Dec-2014  Herbert Greenlee
#
######################################################################

from stagestatus import StageStatus

# Project status class.

class ProjectStatus:

    # Constructor.

    def __init__(self, project):

        # Initialize attributes.

        self.project = project
        self.stats = {}
        for stage in self.project.stages:
            self.stats[stage.name] = StageStatus(stage)

    # Update data for the specified project.

    def update(self, project):

        # Update all stages.

        for stagename in self.stats.keys():
            self.stats[stagename].update()

    # Get stage status for specified stage.

    def get_stage_status(self, stagename):
        return self.stats[stagename]
