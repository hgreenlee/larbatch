#! /usr/bin/env python
######################################################################
#
# Name: batchstatus.py
#
# Purpose: Python class BatchStatus (used by project.py script).
#          This class contains information about batch system status
#          corresponding to the specified project.
#
# Created: 12-Dec-2014  Herbert Greenlee
#
######################################################################

import project_utilities
import subprocess

# Cache jobs list independent of project.

jobs = None

# Batch status class.

class BatchStatus:

    # Constructor.

    def __init__(self, project):

        # Initialize attributes.

        self.project = project

        # The status for each stage is a 4-tuple of integers consisting
        # of the following values.
        # 1. Number of idle batch jobs (state 'I')
        # 2. Number of running batch jobs (state 'R')
        # 3. Number of held batch jobs (state 'H')
        # 4. Number of batch jobs in any other stage.

        self.stage_stats = {}
        self.update(project)
        

    # Update data for the specified project.

    def update(self, project):

        global jobs

        for stage in project.stages:
            self.stage_stats[stage.name] = [0, 0, 0, 0]

        # Get information from the batch system.

        if jobs == None:
            BatchStatus.update_jobs()
        for job in jobs:
            words = job.split()
            if len(words) > 4:
                state = words[-4]
                script = words[-1]

                # Loop over stages.

                for stage in self.project.stages:
                    workscript = '%s-%s.sh' % (stage.name, project.name)
                    if script.find(workscript) == 0:
                        if state == 'I':
                            self.stage_stats[stage.name][0] = self.stage_stats[stage.name][0] + 1
                        elif state == 'R':
                            self.stage_stats[stage.name][1] = self.stage_stats[stage.name][1] + 1
                        elif state == 'H':
                            self.stage_stats[stage.name][2] = self.stage_stats[stage.name][2] + 1
                        else:
                            self.stage_stats[stage.name][3] = self.stage_stats[stage.name][3] + 1

    # Update jobs list.

    @staticmethod
    def update_jobs():

        global jobs

        command = ['jobsub_q']
        command.append('--group=%s' % project_utilities.get_experiment())
        command.append('--user=%s' % project_utilities.get_user())
        command.append('--role=%s' % project_utilities.get_role())
        jobs = subprocess.check_output(command).splitlines()

    # Return jobs list.

    @staticmethod
    def get_jobs():

        global jobs
        return jobs    

    # Get stage status for specified stage.
    # Returns 4-tuple (# idle, # running, # held, # other).

    def get_stage_status(self, stagename):
        return self.stage_stats[stagename]
