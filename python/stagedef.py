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

import os, string, stat, math
import project_utilities
import uuid
from project_modules.xmlerror import XMLError
from project_modules.pubsinputerror import PubsInputError
from project_modules.pubsdeadenderror import PubsDeadEndError

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
        self.pubs_input_ok = 1 # Is pubs input allowed?
        self.pubs_input = 0    # Pubs input mode.
        self.input_run = 0     # Pubs input run.
        self.input_subruns = [] # Pubs input subrun number(s).
        self.input_version = 0 # Pubs input version number.
        self.pubs_output = 0   # Pubs output mode.
        self.output_run = 0    # Pubs output run.
        self.output_subruns = [] # Pubs output subrun number.
        self.output_version = 0 # Pubs output version number.
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
        self.jobsub = ''       # Arbitrary jobsub_submit options.
	
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

        # Pubs input flag.

        pubs_input_ok_elements = stage_element.getElementsByTagName('pubsinput')
        if pubs_input_ok_elements:
            self.pubs_input_ok = int(pubs_input_ok_elements[0].firstChild.data)

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

	# Jobsub.

        jobsub_elements = stage_element.getElementsByTagName('jobsub')
        if jobsub_elements:
            self.jobsub = jobsub_elements[0].firstChild.data     

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
        result += 'Pubs input allowed = %d\n' % self.pubs_input_ok
        result += 'Pubs input mode = %d\n' % self.pubs_input
        result += 'Pubs input run number = %d\n' % self.input_run
        for subrun in self.input_subruns:
            result += 'Pubs input subrun number = %d\n' % subrun
        result += 'Pubs input version number = %d\n' % self.input_version
        result += 'Pubs output mode = %d\n' % self.pubs_output
        result += 'Pubs output run number = %d\n' % self.output_run
        for subrun in self.output_subruns:
            result += 'Pubs output subrun number = %d\n' % subrun
        result += 'Pubs output version number = %d\n' % self.output_version
        result += 'Output file name = %s\n' % self.output
        result += 'TFileName = %s\n' % self.TFileName	
        result += 'Number of jobs = %d\n' % self.num_jobs
        result += 'Max files per job = %d\n' % self.max_files_per_job
        result += 'Output file target size = %d\n' % self.target_size
        result += 'Dataset definition name = %s\n' % self.defname
        result += 'Analysis dataset definition name = %s\n' % self.ana_defname
        result += 'Data tier = %s\n' % self.data_tier
        result += 'Analysis data tier = %s\n' % self.ana_data_tier
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
        result += 'Output file name = %s\n' % self.output
        result += 'TFile name = %s\n' % self.TFileName
        result += 'Jobsub_submit options = %s\n' % self.jobsub
        return result

    # The purpose of this method is to limit input to the specified run
    # and subruns.  There are several use cases.
    #
    # 1.  If xml element pubsinput is false (0), do not try to limit the 
    #     input in any way.  Just use whatever is the normal input.
    #
    # 2.  If input is from a sam dataset, assume that the input dataset
    #     has been limited to the specifiec run and subruns elsewhere in
    #     the workflow (e.g. condor_start_lar.sh).  Don't do anything in this
    #     method.
    #
    # 3.  If input is from a file list (and pubsinput is true), modify the 
    #     input list assuming that the input area as the standard pubs 
    #     diretory structure.  There are several subcases depending on
    #     whether there is one or multiple subruns, and whether input
    #     to output ratio is one-to-one or many-to-run.
    #
    #     a) One-to-one, single subrun, reset input list to point to 
    #        files.list from pubs input directory.  Raise PubsInputError
    #        if input files.list does not exist.
    #
    #     b) Many-to-one, single subrun (merging).  Generate a new input
    #        list with a unique name from all files.list files from all 
    #        pubs input subrun directories.  Raise PubsDeadEndError if this
    #        subrun is being merged into a different output subrun.  Raise
    #        PubsInputError if not all input files.list files are (as yet)
    #        available.
    #
    #     c) One-to-one, multiple subruns.  Generate a new input list with
    #        a unique name from all files.list files from all pubs input 
    #        subrun directories.  Raise PubsInputError if not all input
    #        files.list files are available.
    #
    #     d) Many-to-one, multiple subruns.  This use case is not (yet)
    #        supported.
    #
    # 4.  If input is from a singe file (and pubsinput is true), raise
    #     an exception.  This use case doesn't make sense and isn't
    #     supported.
    #
    # 5.  If no input is specified, don't do anything, since there is no
    #     input to limit.

    def pubsify_input(self, run, subruns, version, previous_stage):

        # Don't do anything if pubs input is disabled.

        if not self.pubs_input_ok:
            return

        # It never makes sense to specify pubs input mode if there are no 
        # input files (i.e. generation jobs).  This is not considered an error.

        if self.inputfile == '' and self.inputlist == '' and self.inputdef == '':
            return

        # The case if input from a single file is not supported.  Raise an exception.

        if self.inputfile != '':
            raise RuntimeError, 'Pubs input for single file input is not supported.'

        # Set pubs input mode.

        self.pubs_input = 1

        # Save the run, subrun, and version numbers.

        self.input_run = run;
        self.input_subruns = subruns;
        self.input_version = version;

        # if input is from a sam dataset, create a restricted dataset that limits
        # input files to selected run and subruns.

        if self.inputdef != '':
            self.inputdef = project_utilities.create_limited_dataset(self.inputdef,
                                                                     run,
                                                                     subruns)
            return

        # Raise an exception if there is no previous stage (with input file).

        if previous_stage == None:
            raise RuntimeError('No previous pubs stage.')

        # If we get to here, we have input from a file list and a previous stage
        # exists.  This normally indicates a daisy chain.  This is where subcases
        # 3 (a)-(d) are handled.

        # Case 3(a), one-to-one, single subrun.

        if self.num_jobs == previous_stage.num_jobs and len(subruns) == 1:

            # Insert run and subrun into input file list path.

            if version == None:
                pubs_path = '%d/%d' % (run, subruns[0])
            else:
                pubs_path = '%d/%d/%d' % (version, run, subruns[0])
            dir = os.path.dirname(self.inputlist)
            base = os.path.basename(self.inputlist)
            self.inputlist = os.path.join(dir, pubs_path, base)

            # Verify that the input list exists and is not empty.

            lines = []
            try:
                lines = open(self.inputlist).readlines()
            except:
                lines = []
            if len(lines) == 0:
                raise PubsInputError(run, subrun, version)

            # Everything OK (case 3(a)).

            return

        # Case 3(b), many-to-one, single subrun.

        elif self.num_jobs < previous_stage.num_jobs and len(subruns) == 1:

            # Extract the input subrun range.

            section = int((subruns[0] - 1) * self.num_jobs / previous_stage.num_jobs)
            first_input_subrun = int(math.ceil(float(previous_stage.num_jobs * section) \
                                                   / float(self.num_jobs))) + 1
            last_input_subrun = (previous_stage.num_jobs * section + previous_stage.num_jobs - 1) \
                            / self.num_jobs + 1

            # Only process the first input subrun.

            if subruns[0] != first_input_subrun:
                raise PubsDeadEndError(run, subrun, version)

            # Generate a new input file list, called "merged_files.list" and place 
            # it in the same directory as the first input subrun input list.

            first = True
            dir = os.path.dirname(self.inputlist)
            base = os.path.basename(self.inputlist)
            for input_subrun in range(first_input_subrun, last_input_subrun+1):
                if version == None:
                    pubs_path = '%d/%d' % (run, input_subrun)
                else:
                    pubs_path = '%d/%d/%d' % (version, run, input_subrun)
                inputlist = os.path.join(dir, pubs_path, base)
                if first:
                    first = False
                    merged_input_path = os.path.join(dir, pubs_path, 'merged_files.list')
                    self.inputlist = merged_input_path
                    print 'Generating merged input list %s' % merged_input_path
                    merged_input = open(merged_input_path, 'w')
                lines = []
                try:
                    lines = open(inputlist).readlines()
                except:
                    lines = []

                # Return an error if input can't be opened, or is empty.

                if len(lines) == 0:
                    raise PubsInputError(run, subrun, version)

                for line in lines:
                    merged_input.write(line)

            # Done looping over input lists.

            merged_input.close()

            # Set the number of jobs to one, so that batch job will process the entire
            # (possibly merged) input file list.

            self.num_jobs = 1

            # Everything OK (case 3(b)).

            return

        # Case 3(c), one-to-one, multiple subruns.

        if self.num_jobs == previous_stage.num_jobs and len(subruns) > 1:

            # Generate a new input file list with a unique name and place 
            # it in the same directory as the original input list.  Note that
            # the input list may not actually exist at this point.  If it
            # doesn't exist, just use the same name.  If it already exists,
            # generate a different name.

            dir = os.path.dirname(self.inputlist)
            base = os.path.basename(self.inputlist)
            new_inputlist_path = self.inputlist
            if os.path.exists(new_inputlist_path):
                new_inputlist_path = '%s/%s_%s.list' % (dir, base, str(uuid.uuid4()))
            self.inputlist = new_inputlist_path
            print 'Generating new input list %s\n' % new_inputlist_path
            new_inputlist_file = open(new_inputlist_path, 'w')

            # Loop over subruns.  Read contents of pubs input list for each subrun.
            # Each subrun should have only one input file (because of one-to-one).

            for subrun in subruns:

                if version == None:
                    pubs_path = '%d/%d' % (run, subrun)
                else:
                    pubs_path = '%d/%d/%d' % (version, run, subrun)

                subrun_inputlist = os.path.join(dir, pubs_path, base)
                lines = []
                try:
                    lines = open(subrun_inputlist).readlines()
                except:
                    lines = []
                if len(lines) == 0:
                    raise PubsInputError, 'Trouble reading input file from %s\n' % subrun_inputlist
                if len(lines) > 1:
                    raise PubsInputError, 'Found multiple input files in %s\n' % subrun_inputlist
                subrun_inputfile = lines[0].strip()
                #print 'Adding input file %s\n' % subrun_inputfile
                new_inputlist_file.write('%s\n' % subrun_inputfile)

            # Done looping over subruns.

            new_inputlist_file.close()

            # Everything OK (case 3(c)).

            return

        # Case 3(d), many-to-one, multiple subruns.

        elif self.num_jobs < previous_stage.num_jobs and len(subruns) != 1:

            raise RuntimeError, 'Many-to-one pubs input with multiple subruns is not supported.'

        # One-to-many case is not allows.

        else:

            raise RuntimeError, 'One-to-many pubs input is not supported.'

        # Shouldn't ever fall out of loop.

        return


    # Function to convert this stage for pubs output.

    def pubsify_output(self, run, subruns, version):

        # Set pubs mode.

        self.pubs_output = 1

        # Save the run, subrun, and version numbers.

        self.output_run = run;
        self.output_subruns = subruns;
        self.output_version = version;

        # Append run and subrun to workdir, outdir, and logdir.
        # In case of multiple subruns, encode the subdir directory as "@s",
        # which informs the batch worker to determine the subrun dynamically.

        if len(subruns) == 1:
            if version == None:
                pubs_path = '%d/%d' % (run, subruns[0])
            else:
                pubs_path = '%d/%d/%d' % (version, run, subruns[0])
            self.workdir = os.path.join(self.workdir, pubs_path)
        else:
            if version == None:
                pubs_path = '%d/@s' % run
            else:
                pubs_path = '%d/%d/@s' % (version, run)
            self.workdir = os.path.join(self.workdir, str(uuid.uuid4()))
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


    # Raise an exception if output directory or log directory doesn't exist.

    def check_output_dirs(self):
        if not os.path.exists(self.outdir):
            raise IOError, 'Output directory %s does not exist.' % self.outdir
        if not os.path.exists(self.logdir):
            raise IOError, 'Log directory %s does not exist.' % self.logdir
        return
    
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
