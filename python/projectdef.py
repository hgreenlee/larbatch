#! /usr/bin/env python
######################################################################
#
# Name: projectdef.py
#
# Purpose: Python class ProjectDef (used by project.py script).
#
# Created: 12-Dec-2014  Herbert Greenlee
#
######################################################################

import os, string, subprocess
from xmlerror import XMLError
from stagedef import StageDef

# Project definition class contains data parsed from project defition xml file.

class ProjectDef:

    # Constructor.
    # project_element argument can be an xml element or None.

    def __init__(self, project_element, override_merge):

        # Assign default values.
        
        self.name= ''                     # Project name.
        self.group = ''                   # Experiment or group.
        if os.environ.has_key('GROUP'):
            self.group = os.environ['GROUP']
        self.num_events = 0               # Total events (all jobs).
        self.num_jobs = 1                 # Number of jobs.
        self.os = ''                      # Batch OS.
        self.resource = 'DEDICATED,OPPORTUNISTIC' # Jobsub resources.
        self.lines = ''                   # Arbitrary condor commands.
        self.server = '-'                 # Jobsub server.
        self.site = ''                    # Site.
        self.merge = 'hadd -T'               # histogram merging program.
        self.release_tag = ''             # Larsoft release tag.
        self.release_qual = 'debug'       # Larsoft release qualifier.
        self.local_release_dir = ''       # Larsoft local release directory.
        self.local_release_tar = ''       # Larsoft local release tarball.
        self.file_type = ''               # Sam file type.
        self.run_type = ''                # Sam run type.
        self.script = 'condor_lar.sh'     # Batch script.
        self.start_script = 'condor_start_project.sh'  # Sam start project script.
        self.stop_script = 'condor_stop_project.sh'    # Sam stop project script.
        self.fclpath = []                 # Fcl search path.
        self.stages = []                  # List of stages (StageDef objects).
        self.parameters = {}              # Dictionary of metadata parameters.

        # Extract values from xml.

        # Project name (attribute)

        if project_element.attributes.has_key('name'):
            self.name = project_element.attributes['name'].firstChild.data
        if self.name == '':
            raise XMLError, 'Project name not specified.'

        # Group (subelement).

        group_elements = project_element.getElementsByTagName('group')
        if group_elements:
            self.group = group_elements[0].firstChild.data
        if self.group == '':
            raise XMLError, 'Group not specified.'

        # Total events (subelement).

        num_events_elements = project_element.getElementsByTagName('numevents')
        if num_events_elements:
            self.num_events = int(num_events_elements[0].firstChild.data)
        if self.num_events == 0:
            raise XMLError, 'Number of events not specified.'

        # Number of jobs (subelement).

        num_jobs_elements = project_element.getElementsByTagName('numjobs')
        if num_jobs_elements:
            self.num_jobs = int(num_jobs_elements[0].firstChild.data)

        # OS (subelement).

        os_elements = project_element.getElementsByTagName('os')
        if os_elements:
            self.os = os_elements[0].firstChild.data

        # Resource (subelement).

        resource_elements = project_element.getElementsByTagName('resource')
        if resource_elements:
            self.resource = resource_elements[0].firstChild.data

        # Lines (subelement).

        lines_elements = project_element.getElementsByTagName('lines')
        if lines_elements:
            self.lines = lines_elements[0].firstChild.data

        # Server (subelement).

        server_elements = project_element.getElementsByTagName('server')
        if server_elements:
            self.server = server_elements[0].firstChild.data

        # Site (subelement).

        site_elements = project_element.getElementsByTagName('site')
        if site_elements:
            self.site = site_elements[0].firstChild.data

        # merge (subelement).
 	
        merge_elements = project_element.getElementsByTagName('merge')
        if merge_elements:
            if merge_elements[0].firstChild:
                self.merge = merge_elements[0].firstChild.data
            else:
                self.merge = ''
        if override_merge != '':
            self.merge = override_merge
	    
        # Larsoft (subelement).

        larsoft_elements = project_element.getElementsByTagName('larsoft')
        if larsoft_elements:

            # Release tag (subelement).

            tag_elements = larsoft_elements[0].getElementsByTagName('tag')
            if tag_elements and tag_elements[0].firstChild != None:
                self.release_tag = tag_elements[0].firstChild.data

            # Release qualifier (subelement).

            qual_elements = larsoft_elements[0].getElementsByTagName('qual')
            if qual_elements:
                self.release_qual = qual_elements[0].firstChild.data

            # Local release directory or tarball (subelement).
            # 

            local_elements = larsoft_elements[0].getElementsByTagName('local')
            if local_elements:
                local = local_elements[0].firstChild.data
                if os.path.isdir(local):
                    self.local_release_dir = local
                else:
                    self.local_release_tar = local

        # Make sure local test release directory/tarball exists, if specified.
        # Existence of non-null local_release_dir has already been tested.

        if self.local_release_tar != '' and not os.path.exists(self.local_release_tar):
            raise IOError, "Local release directory/tarball %s does not exist." % self.local_release_tar
            
        # Sam file type (subelement).

        file_type_elements = project_element.getElementsByTagName('filetype')
        if file_type_elements:
            self.file_type = file_type_elements[0].firstChild.data

        # Sam run type (subelement).

        run_type_elements = project_element.getElementsByTagName('runtype')
        if run_type_elements:
            self.run_type = run_type_elements[0].firstChild.data

        # Batch script (subelement).

        script_elements = project_element.getElementsByTagName('script')
        if script_elements:
            self.script = script_elements[0].firstChild.data

        # Make sure batch script exists, and convert into a full path.

        script_path = ''
        try:
            proc = subprocess.Popen(['which', self.script], stdout=subprocess.PIPE)
            script_path = proc.stdout.readlines()[0].strip()
            proc.wait()
        except:
            pass
        if script_path == '' or not os.access(script_path, os.X_OK):
            raise IOError, 'Script %s not found.' % self.script
        self.script = script_path

        # Also convert start and stop project scripts into full path.
        # It is not treated as an error here if these aren't found.

        # Start project script.
        
        script_path = ''
        try:
            proc = subprocess.Popen(['which', self.start_script], stdout=subprocess.PIPE)
            script_path = proc.stdout.readlines()[0].strip()
            proc.wait()
        except:
            pass
        self.start_script = script_path

        # Stop project script.
        
        script_path = ''
        try:
            proc = subprocess.Popen(['which', self.stop_script], stdout=subprocess.PIPE)
            script_path = proc.stdout.readlines()[0].strip()
            proc.wait()
        except:
            pass
        self.stop_script = script_path

        # Fcl search path (repeatable subelement).

        fclpath_elements = project_element.getElementsByTagName('fcldir')
        for fclpath_element in fclpath_elements:
            self.fclpath.append(fclpath_element.firstChild.data)

        # Add $FHICL_FILE_PATH.

        if os.environ.has_key('FHICL_FILE_PATH'):
            for fcldir in string.split(os.environ['FHICL_FILE_PATH'], ':'):
                if os.path.exists(fcldir):
                    self.fclpath.append(fcldir)

        # Make sure all directories of fcl search path exist.

        for fcldir in self.fclpath:
            if not os.path.exists(fcldir):
                raise IOError, "Fcl search directory %s does not exist." % fcldir

        # Project stages (repeatable subelement).

        stage_elements = project_element.getElementsByTagName('stage')
        default_input_list = ''
        for stage_element in stage_elements:
            self.stages.append(StageDef(stage_element, 
                                        default_input_list, 
                                        self.num_jobs, 
                                        self.merge))
            default_input_list = os.path.join(self.stages[-1].outdir, 'files.list')

        # Dictionary of metadata parameters

        param_elements = project_element.getElementsByTagName('parameter')
        for param_element in param_elements:
            name = param_element.attributes['name'].firstChild.data
            value = param_element.firstChild.data
            self.parameters[name] = value

        # Done.
                
        return

    # String conversion.

    def __str__(self):
        result = 'Project name = %s\n' % self.name
        result += 'Group = %s\n' % self.group
        result += 'Total events = %d\n' % self.num_events
        result += 'Number of jobs = %d\n' % self.num_jobs
        result += 'OS = %s\n' % self.os
        result += 'Resource = %s\n' % self.resource
        result += 'Lines = %s\n' % self.lines
        result += 'Jobsub server = %s\n' % self.server
        result += 'Site = %s\n' % self.site
        result += 'Histogram merging program = %s\n' % self.merge
        result += 'Larsoft release tag = %s\n' % self.release_tag
        result += 'Larsoft release qualifier = %s\n' % self.release_qual
        result += 'Local test release directory = %s\n' % self.local_release_dir
        result += 'Local test release tarball = %s\n' % self.local_release_tar
        result += 'File type = %s\n' % self.file_type
        result += 'Run type = %s\n' % self.run_type
        result += 'Batch script = %s\n' % self.script
        result += 'Start sam project script = %s\n' % self.start_script
        result += 'Stop sam project script = %s\n' % self.stop_script
        result += 'Fcl search path:\n'
        for fcldir in self.fclpath:
            result += '    %s\n' % fcldir
        result += 'Metadata parameters:\n'
        for key in self.parameters:
            result += '%s: %s\n' % (key,self.parameters[key])
        result += '\nStages:'
        for stage in self.stages:
            result += '\n\n' + str(stage)
        return result

    # Get the specified stage.

    def get_stage(self, stagename):

        if len(self.stages) == 0:
            raise LookupError, "Project does not have any stages."

        elif stagename == '' and len(self.stages) == 1:
            return self.stages[0]

        else:
            for stage in self.stages:
                if stagename == stage.name:
                    return stage

        # If we fell through to here, we didn't find an appropriate stage.

        raise RuntimeError, 'No stage %s.' % stagename

    # Find fcl file on fcl search path.

    def get_fcl(self, fclname):
        fcl = ''
        for fcldir in self.fclpath:
            fcl = os.path.join(fcldir, fclname)
            if os.path.exists(fcl):
                break
        if fcl == '' or not os.path.exists(fcl):
            raise IOError, 'Could not find fcl file %s.' % fclname
        return fcl
