#! /usr/bin/env python
######################################################################
#
# Name: stagedef.py
#
# Purpose: Python class StageDef (used by project.py).
#
# Created: 12-Dec-2014  Herbert Greenlee
#
######################################################################

import os, string, stat
import project_utilities
from xmlerror import XMLError

# Stage definition class.

class StageDef:

    # Constructor.

    def __init__(self, stage_element, default_input_list, default_num_jobs, default_max_files_per_job, default_merge):

        # Assign default values.
        
        self.name = ''         # Stage name.
        self.fclname = ''      # Fcl name (just name, not path).
        self.outdir = ''       # Output directory.
        self.logdir = ''       # Log directory.
        self.workdir = ''      # Work directory.
        self.inputfile = ''    # Single input file.
        self.inputlist = ''    # Input file list.
        self.inputmode = ''    # Input file type (none or textfile)
        self.inputdef = ''     # Input sam dataset definition.
        self.pubs_input = 0    # Pubs input mode.
        self.input_run = 0     # Pubs input run.
        self.input_subrun = 0  # Pubs input subrun number.
        self.pubs_output = 0   # Pubs output mode.
        self.output_run = 0    # Pubs output run.
        self.output_subrun = 0 # Pubs output subrun number.
        self.num_jobs = default_num_jobs # Number of jobs.
        self.max_files_per_job = default_max_files_per_job #max num of files per job
        self.target_size = 0   # Target size for output files.
        self.defname = ''      # Sam dataset definition name.
        self.ana_defname = ''  # Sam dataset definition name.
        self.data_tier = ''    # Sam data tier.
        self.ana_data_tier = '' # Sam data tier.
        self.init_script = ''  # Worker initialization script.
        self.init_source = ''  # Worker initialization bash source script.
        self.end_script = ''   # Worker end-of-job script.
        self.merge = default_merge    # Histogram merging program
        self.resource = ''     # Jobsub resources.
        self.lines = ''        # Arbitrary condor commands.
        self.site = ''         # Site.
        self.parameters = {}   # Dictionary of metadata parameters.
        self.output = ''       # Art output file name.
        self.TFileName = ''    # TFile output file name.
	
        # Extract values from xml.

        # Stage name (attribute).

        if stage_element.attributes.has_key('name'):
            self.name = stage_element.attributes['name'].firstChild.data
        if self.name == '':
            raise XMLError, "Stage name not specified."

        # Fcl file name (subelement).

        fclname_elements = stage_element.getElementsByTagName('fcl')
        if fclname_elements:
            self.fclname = fclname_elements[0].firstChild.data
        if self.fclname == '':
            raise XMLError, 'Fcl name not specified for stage %s.' % self.name

        # Output directory (subelement).

        outdir_elements = stage_element.getElementsByTagName('outdir')
        if outdir_elements:
            self.outdir = outdir_elements[0].firstChild.data
        if self.outdir == '':
            raise XMLError, 'Output directory not specified for stage %s.' % self.name

        # Log directory (subelement).

        logdir_elements = stage_element.getElementsByTagName('logdir')
        if logdir_elements:
            self.logdir = logdir_elements[0].firstChild.data
        if self.logdir == '':
            self.logdir = self.outdir

        # Work directory (subelement).

        workdir_elements = stage_element.getElementsByTagName('workdir')
        if workdir_elements:
            self.workdir = workdir_elements[0].firstChild.data
        if self.workdir == '':
            raise XMLError, 'Work directory not specified for stage %s.' % self.name

        # Single input file (subelement).

        inputfile_elements = stage_element.getElementsByTagName('inputfile')
        if inputfile_elements:
            self.inputfile = inputfile_elements[0].firstChild.data

        # Input file list (subelement).

        inputlist_elements = stage_element.getElementsByTagName('inputlist')
        if inputlist_elements:
            self.inputlist = inputlist_elements[0].firstChild.data

        # Input file type (subelement).

        inputmode_elements = stage_element.getElementsByTagName('inputmode')
        if inputmode_elements:
            self.inputmode = inputmode_elements[0].firstChild.data

        # Input sam dataset dfeinition (subelement).

        inputdef_elements = stage_element.getElementsByTagName('inputdef')
        if inputdef_elements:
            self.inputdef = inputdef_elements[0].firstChild.data

        # It is an error to specify both input file and input list.

        if self.inputfile != '' and self.inputlist != '':
            raise XMLError, 'Input file and input list both specified for stage %s.' % self.name

        # It is an error to specify either input file or input list together
        # with a sam input dataset.

        if self.inputdef != '' and (self.inputfile != '' or self.inputlist != ''):
            raise XMLError, 'Input dataset and input files specified for stage %s.' % self.name

        # It is an error to use textfile inputmode without an inputlist or inputfile
        if self.inputmode == 'textfile' and self.inputlist == '' and self.inputfile == '':
            raise XMLError, 'Input list (inputlist) or inputfile is needed for textfile model.'

        # If none of input definition, input file, nor input list were specified, set
        # the input list to the dafault input list.

        if self.inputfile == '' and self.inputlist == '':
            self.inputlist = default_input_list

        # Number of jobs (subelement).

        num_jobs_elements = stage_element.getElementsByTagName('numjobs')
        if num_jobs_elements:
            self.num_jobs = int(num_jobs_elements[0].firstChild.data)

        # Max Number of files per jobs.

        max_files_per_job_elements = stage_element.getElementsByTagName('maxfilesperjob')
        if max_files_per_job_elements:
            self.max_files_per_job = int(max_files_per_job_elements[0].firstChild.data)

        # Target size for output files (subelement).

        target_size_elements = stage_element.getElementsByTagName('targetsize')
        if target_size_elements:
            self.target_size = int(target_size_elements[0].firstChild.data)
	

        # Sam dataset definition name (subelement).

        defname_elements = stage_element.getElementsByTagName('defname')
        if defname_elements:
            self.defname = defname_elements[0].firstChild.data

        # Sam analysis dataset definition name (subelement).

        ana_defname_elements = stage_element.getElementsByTagName('anadefname')
        if ana_defname_elements:
            self.ana_defname = ana_defname_elements[0].firstChild.data

        # Sam data tier (subelement).

        data_tier_elements = stage_element.getElementsByTagName('datatier')
        if data_tier_elements:
            self.data_tier = data_tier_elements[0].firstChild.data

        # Sam analysis data tier (subelement).

        ana_data_tier_elements = stage_element.getElementsByTagName('anadatatier')
        if ana_data_tier_elements:
            self.ana_data_tier = ana_data_tier_elements[0].firstChild.data

        # Worker initialization script (subelement).

        init_script_elements = stage_element.getElementsByTagName('initscript')
        if init_script_elements:
            self.init_script = init_script_elements[0].firstChild.data

        # Worker initialization source script (subelement).

        init_source_elements = stage_element.getElementsByTagName('initsource')
        if init_source_elements:
            self.init_source = init_source_elements[0].firstChild.data

        # Worker end-of-job script (subelement).

        end_script_elements = stage_element.getElementsByTagName('endscript')
        if end_script_elements:
            self.end_script = end_script_elements[0].firstChild.data

        # Histogram merging program.

        merge_elements = stage_element.getElementsByTagName('merge')
        if merge_elements:
            self.merge = merge_elements[0].firstChild.data
	
        # Resource (subelement).

        resource_elements = stage_element.getElementsByTagName('resource')
        if resource_elements:
            self.resource = resource_elements[0].firstChild.data
            self.resource = ''.join(self.resource.split())

        # Lines (subelement).

        lines_elements = stage_element.getElementsByTagName('lines')
        if lines_elements:
            self.lines = lines_elements[0].firstChild.data

        # Site (subelement).

        site_elements = stage_element.getElementsByTagName('site')
        if site_elements:
            self.site = site_elements[0].firstChild.data
            self.site = ''.join(self.site.split())

        # Dictionary of metadata parameters

        param_elements = stage_element.getElementsByTagName('parameter')
        for param_element in param_elements:
            name = param_element.attributes['name'].firstChild.data
            value = param_element.firstChild.data
            self.parameters[name] = value

        # Output file name (subelement).

        output_elements = stage_element.getElementsByTagName('output')
        if output_elements:
            self.output = output_elements[0].firstChild.data
	    
	# TFileName (subelement).

        TFileName_elements = stage_element.getElementsByTagName('TFileName')
        if TFileName_elements:
            self.TFileName = TFileName_elements[0].firstChild.data     

        # Done.

        return
        
    # String conversion.

    def __str__(self):
        result = 'Stage name = %s\n' % self.name
        result += 'Fcl filename = %s\n' % self.fclname
        result += 'Output directory = %s\n' % self.outdir
        result += 'Log directory = %s\n' % self.logdir
        result += 'Work directory = %s\n' % self.workdir
        result += 'Input file = %s\n' % self.inputfile
        result += 'Input list = %s\n' % self.inputlist
        result += 'Input mode = %s\n' % self.inputmode
        result += 'Input sam dataset = %s\n' % self.inputdef
        result += 'Pubs input mode = %d\n' % self.pubs_input
        result += 'Pubs input run number = %d\n' % self.input_run
        result += 'Pubs input subrun number = %d\n' % self.input_subrun
        result += 'Pubs output mode = %d\n' % self.pubs_output
        result += 'Pubs output run number = %d\n' % self.output_run
        result += 'Pubs output subrun number = %d\n' % self.output_subrun
        result += 'Output file name = %s\n' % self.output
        result += 'TFileName = %s\n' % self.TFileName	
        result += 'Number of jobs = %d\n' % self.num_jobs
        result += 'Max files per job = %d\n' % self.max_files_per_job
        result += 'Output file target size = %d\n' % self.target_size
        result += 'Dataset definition name = %s\n' % self.defname
        result += 'Analysis dataset definition name = %s\n' % self.ana_defname
        result += 'Data tier = %s' % self.data_tier
        result += 'Analysis data tier = %s' % self.ana_data_tier
        result += 'Worker initialization script = %s\n' % self.init_script
        result += 'Worker initialization source script = %s\n' % self.init_source
        result += 'Worker end-of-job script = %s\n' % self.end_script
        result += 'Special histogram merging program = %s\n' % self.merge
        result += 'Resource = %s\n' % self.resource
        result += 'Lines = %s\n' % self.lines
        result += 'Site = %s\n' % self.site
        result += 'Metadata parameters:\n'
        for key in self.parameters:
            result += '%s: %s\n' % (key,self.parameters[key])
        return result

    # Function to convert this stage for pubs input.

    def pubsify_input(self, run, subrun):

        # It never makes sense to specify pubs input mode if there are no 
        # input files (i.e. generation jobs).

        if self.inputfile == '' and self.inputlist == '' and self.inputdef == '':
            return

        # Set pubs input mode.

        self.pubs_input = 1

        # Save the run and subrun numbers.

        self.input_run = run;
        self.input_subrun = subrun;

        # Insert run and subrun into input file or input file list path.

        pubs_path = '%d/%d' % (run, subrun)
        if self.inputfile != '':
            dir = os.path.dirname(self.inputfile)
            base = os.path.basename(self.inputfile)
            self.inputfile = os.path.join(dir, pubs_path, base)
        if self.inputlist != '':
            dir = os.path.dirname(self.inputlist)
            base = os.path.basename(self.inputlist)
            self.inputlist = os.path.join(dir, pubs_path, base)


    # Function to convert this stage for pubs output.

    def pubsify_output(self, run, subrun):

        # Set pubs mode.

        self.pubs_output = 1

        # Save the run and subrun numbers.

        self.output_run = run;
        self.output_subrun = subrun;

        # Append run and subrun to workdir, outdir, and logdir.

        pubs_path = '%d/%d' % (run, subrun)
        self.workdir = os.path.join(self.workdir, pubs_path)
        self.outdir = os.path.join(self.outdir, pubs_path)
        self.logdir = os.path.join(self.logdir, pubs_path)


    # Raise an exception if any specified input file/list doesn't exist.
    # (We don't currently check sam input datasets).

    def checkinput(self):
        if self.inputfile != '' and not project_utilities.safeexist(self.inputfile):
            raise IOError, 'Input file %s does not exist.' % self.inputfile
        if self.inputlist != '' and not project_utilities.safeexist(self.inputlist):
            raise IOError, 'Input list %s does not exist.' % self.inputlist

        # If target size is nonzero, and input is from a file list, calculate
        # the ideal number of output jobs and override the current number 
        # of jobs.

        if self.target_size != 0 and self.inputlist != '':
            input_filenames = project_utilities.saferead(self.inputlist)
            size_tot = 0
            for line in input_filenames:
                filename = string.split(line)[0]
                filesize = os.stat(filename).st_size
                size_tot = size_tot + filesize
            new_num_jobs = size_tot / self.target_size
            if new_num_jobs < 1:
                new_num_jobs = 1
            if new_num_jobs > self.num_jobs:
                new_num_jobs = self.num_jobs
            print "Ideal number of jobs based on target file size is %d." % new_num_jobs
            if new_num_jobs != self.num_jobs:
                print "Updating number of jobs from %d to %d." % (self.num_jobs, new_num_jobs)
                self.num_jobs = new_num_jobs


    # Raise an exception if output directory, log directory, or work directory doesn't exist.

    def checkdirs(self):
        if not os.path.exists(self.outdir):
            raise IOError, 'Output directory %s does not exist.' % self.outdir
        if not os.path.exists(self.logdir):
            raise IOError, 'Log directory %s does not exist.' % self.logdir
        if not os.path.exists(self.workdir):
            raise IOError, 'Work directory %s does not exist.' % self.workdir
        return
    
    # Make output, log, and work directory, if they don't exist.

    def makedirs(self):
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)
        if not os.path.exists(self.logdir):
            os.makedirs(self.logdir)
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

        # If output is on dcache, make output directory group-writable.

        if self.outdir[0:6] == '/pnfs/':
            mode = os.stat(self.outdir).st_mode
            if not mode & stat.S_IWGRP:
                mode = mode | stat.S_IWGRP
                os.chmod(self.outdir, mode)
        if self.logdir[0:6] == '/pnfs/':
            mode = os.stat(self.logdir).st_mode
            if not mode & stat.S_IWGRP:
                mode = mode | stat.S_IWGRP
                os.chmod(self.logdir, mode)

        self.checkdirs()
        return
