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

import os, string, stat, math, subprocess
import project_utilities
import larbatch_posix
import uuid
from project_modules.xmlerror import XMLError
from project_modules.pubsinputerror import PubsInputError
from project_modules.pubsdeadenderror import PubsDeadEndError

# Stage definition class.

class StageDef:

    # Constructor.

    def __init__(self, stage_element, default_input_lists, default_previous_stage, 
                 default_num_jobs, default_num_events, default_max_files_per_job, default_merge,
                 default_cpu, default_disk, default_memory):

        # Assign default values.
        
        self.name = ''         # Stage name.
        #self.fclname = ''      # Fcl name (just name, not path).
        self.fclname = []
	self.outdir = ''       # Output directory.
        self.logdir = ''       # Log directory.
        self.workdir = ''      # Work directory.
        self.bookdir = ''      # Bookkeeping directory.
        self.dynamic = 0       # Dynamic output/log directory.
        self.inputfile = ''    # Single input file.
        self.inputlist = ''    # Input file list.
        self.inputmode = ''    # Input file type (none or textfile)
        self.inputdef = ''     # Input sam dataset definition.
        self.inputstream = ''  # Input file stream.
        self.previousstage = '' # Previous stage name.
        self.mixinputdef = ''  # Mix input sam dataset definition.
        self.pubs_input_ok = 1 # Is pubs input allowed?
        self.pubs_input = 0    # Pubs input mode.
        self.input_run = 0     # Pubs input run.
        self.input_subruns = [] # Pubs input subrun number(s).
        self.input_version = 0 # Pubs input version number.
        self.pubs_output = 0   # Pubs output mode.
        self.output_run = 0    # Pubs output run.
        self.output_subruns = [] # Pubs output subrun number.
        self.output_version = 0 # Pubs output version number.
        self.maxfluxfilemb = 0 # MaxFluxFileMB (size of genie flux files to fetch).
        self.num_jobs = default_num_jobs # Number of jobs.
        self.num_events = default_num_events # Number of events.
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
        self.cpu = default_cpu # Number of cpus.
        self.disk = default_disk     # Disk space (string value+unit).
        self.memory = default_memory # Amount of memory (integer MB).
        self.parameters = {}   # Dictionary of metadata parameters.
        self.output = ''       # Art output file name.
        self.TFileName = ''    # TFile output file name.
        self.jobsub = ''       # Arbitrary jobsub_submit options.
        self.jobsub_start = '' # Arbitrary jobsub_submit options for sam start/stop jobs.
	
        # Extract values from xml.

        # Stage name (attribute).

        if stage_element.attributes.has_key('name'):
            self.name = stage_element.attributes['name'].firstChild.data
        if self.name == '':
            raise XMLError, "Stage name not specified."

        # Fcl file name (repeatable subelement).

        fclname_elements = stage_element.getElementsByTagName('fcl')
        
	for fcl in fclname_elements:
	    self.fclname.append(fcl.firstChild.data)
        if len(self.fclname) == 0:
            raise XMLError, 'No Fcl names specified for stage %s.' % self.name

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

        # Bookkeeping directory (subelement).

        bookdir_elements = stage_element.getElementsByTagName('bookdir')
        if bookdir_elements:
            self.bookdir = bookdir_elements[0].firstChild.data
        if self.bookdir == '':
            self.bookdir = self.logdir

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

        # Input stream (subelement).

        inputstream_elements = stage_element.getElementsByTagName('inputstream')
        if inputstream_elements:
            self.inputstream = inputstream_elements[0].firstChild.data

        # Previous stage name (subelement).

        previousstage_elements = stage_element.getElementsByTagName('previousstage')
        if previousstage_elements:
            self.previousstage = previousstage_elements[0].firstChild.data

        # Mix input sam dataset (subelement).

        mixinputdef_elements = stage_element.getElementsByTagName('mixinputdef')
        if mixinputdef_elements:
            self.mixinputdef = mixinputdef_elements[0].firstChild.data

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
        # the input list to the dafault input list.  If an input stream was specified,
        # insert it in front of the file type.

        if self.inputfile == '' and self.inputlist == '' and self.inputdef == '':

            # Get the default input list according to the previous stage.

            default_input_list = ''
            previous_stage_name = default_previous_stage
            if self.previousstage != '':
                previous_stage_name = self.previousstage
            if default_input_lists.has_key(previous_stage_name):
                default_input_list = default_input_lists[previous_stage_name]

            # Modify default input list according to input stream, if any.

            if self.inputstream == '' or default_input_list == '':
                self.inputlist = default_input_list
            else:
                n = default_input_list.rfind('.')
                if n < 0:
                    n = len(default_input_list)
                self.inputlist = '%s_%s%s' % (default_input_list[:n],
                                               self.inputstream,
                                               default_input_list[n:])

        # Pubs input flag.

        pubs_input_ok_elements = stage_element.getElementsByTagName('pubsinput')
        if pubs_input_ok_elements:
            self.pubs_input_ok = int(pubs_input_ok_elements[0].firstChild.data)

        # MaxFluxFileMB GENIEHelper fcl parameter (subelement).

        maxfluxfilemb_elements = stage_element.getElementsByTagName('maxfluxfilemb')
        if maxfluxfilemb_elements:
            self.maxfluxfilemb = int(maxfluxfilemb_elements[0].firstChild.data)
        else:

            # If this is a generator job, give maxfluxfilemb parameter a default
            # nonzero value.

            if self.inputfile == '' and self.inputlist == '' and self.inputdef == '':
                self.maxfluxfilemb = 500

        # Number of jobs (subelement).

        num_jobs_elements = stage_element.getElementsByTagName('numjobs')
        if num_jobs_elements:
            self.num_jobs = int(num_jobs_elements[0].firstChild.data)

        # Number of events (subelement).

        num_events_elements = stage_element.getElementsByTagName('numevents')
        if num_events_elements:
            self.num_events = int(num_events_elements[0].firstChild.data)

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

        # Make sure init script exists, and convert into a full path.

        if self.init_script != '':
            if larbatch_posix.exists(self.init_script):
                self.init_script = os.path.realpath(self.init_script)
            else:

                # Look for script on execution path.

                try:
                    jobinfo = subprocess.Popen(['which', self.init_script],
                                               stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE)
                    jobout, joberr = jobinfo.communicate()
                    rc = jobinfo.poll()
                    self.init_script = jobout.splitlines()[0].strip()
                except:
                    pass
            if not larbatch_posix.exists(self.init_script):
                raise IOError, 'Init script %s not found.' % self.init_script

        # Worker initialization source script (subelement).

        init_source_elements = stage_element.getElementsByTagName('initsource')
        if init_source_elements:
            self.init_source = init_source_elements[0].firstChild.data

        # Make sure init source script exists, and convert into a full path.

        if self.init_source != '':
            if larbatch_posix.exists(self.init_source):
                self.init_source = os.path.realpath(self.init_source)
            else:

                # Look for script on execution path.

                try:
                    jobinfo = subprocess.Popen(['which', self.init_source],
                                               stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE)
                    jobout, joberr = jobinfo.communicate()
                    rc = jobinfo.poll()
                    self.init_source = jobout.splitlines()[0].strip()
                except:
                    pass
            if not larbatch_posix.exists(self.init_source):
                raise IOError, 'Init source script %s not found.' % self.init_source

        # Worker end-of-job script (subelement).

        end_script_elements = stage_element.getElementsByTagName('endscript')
        if end_script_elements:
            self.end_script = end_script_elements[0].firstChild.data

        # Make sure end-of-job script exists, and convert into a full path.

        if self.end_script != '':
            if larbatch_posix.exists(self.end_script):
                self.end_script = os.path.realpath(self.end_script)
            else:

                # Look for script on execution path.

                try:
                    jobinfo = subprocess.Popen(['which', self.end_script],
                                               stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE)
                    jobout, joberr = jobinfo.communicate()
                    rc = jobinfo.poll()
                    self.end_script = jobout.splitlines()[0].strip()
                except:
                    pass
            if not larbatch_posix.exists(self.end_script):
                raise IOError, 'End-of-job script %s not found.' % self.end_script

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

        # Cpu (subelement).

        cpu_elements = stage_element.getElementsByTagName('cpu')
        if cpu_elements:
            self.cpu = int(cpu_elements[0].firstChild.data)

        # Disk (subelement).

        disk_elements = stage_element.getElementsByTagName('disk')
        if disk_elements:
            self.disk = disk_elements[0].firstChild.data
            self.disk = ''.join(self.disk.split())

        # Memory (subelement).

        memory_elements = stage_element.getElementsByTagName('memory')
        if memory_elements:
            self.memory = int(memory_elements[0].firstChild.data)

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

	# Jobsub start/stop.

        jobsub_start_elements = stage_element.getElementsByTagName('jobsub_start')
        if jobsub_start_elements:
            self.jobsub_start = jobsub_start_elements[0].firstChild.data     

        # Done.

        return
        
    # String conversion.

    def __str__(self):
        result = 'Stage name = %s\n' % self.name
        #result += 'Fcl filename = %s\n' % self.fclname
        for fcl in self.fclname:
	  result += 'Fcl filename = %s\n' % fcl 
	result += 'Output directory = %s\n' % self.outdir
        result += 'Log directory = %s\n' % self.logdir
        result += 'Work directory = %s\n' % self.workdir
        result += 'Bookkeeping directory = %s\n' % self.bookdir
        result += 'Dynamic directories = %d\n' % self.dynamic
        result += 'Input file = %s\n' % self.inputfile
        result += 'Input list = %s\n' % self.inputlist
        result += 'Input mode = %s\n' % self.inputmode
        result += 'Input sam dataset = %s\n' % self.inputdef
        result += 'Input stream = %s\n' % self.inputstream
        result += 'Previous stage name = %s\n' % self.previousstage
        result += 'Mix input sam dataset = %s\n' % self.mixinputdef
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
        result += 'Number of events = %d\n' % self.num_events
        result += 'Max flux MB = %d\n' % self.maxfluxfilemb
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
        result += 'Cpu = %d\n' % self.cpu
        result += 'Disk = %s\n' % self.disk
        result += 'Memory = %d MB\n' % self.memory
        result += 'Metadata parameters:\n'
        for key in self.parameters:
            result += '%s: %s\n' % (key,self.parameters[key])
        result += 'Output file name = %s\n' % self.output
        result += 'TFile name = %s\n' % self.TFileName
        result += 'Jobsub_submit options = %s\n' % self.jobsub
        result += 'Jobsub_submit start/stop options = %s\n' % self.jobsub_start
        return result

    # The purpose of this method is to limit input to the specified run
    # and subruns.  There are several use cases.
    #
    # 1.  If xml element pubsinput is false (0), do not try to limit the 
    #     input in any way.  Just use whatever is the normal input.
    #
    # 2.  If input is from a sam dataset, create a restricted dataset that
    #     is limited to the specified run and subruns.
    #
    # 3.  If input is from a file list (and pubsinput is true), modify the 
    #     input list assuming that the input area has the standard pubs 
    #     diretory structure.  There are several subcases depending on
    #     whether there is one or multiple subruns.
    #
    #     a) Single subrun.  Reset input list to point to an input
    #        list from pubs input directory (assuming input area has
    #        standard pubs structure).  Raise PubsInputError if input
    #        files.list does not exist.
    #
    #     b) Multiple subruns.  Generate a new input list with a
    #        unique name which will get input from the union of the
    #        specified run and subruns input lists (assuming pubs
    #        directory structure).
    #
    #    In each case 2, 3(a), and 3(b), the original stage parameter
    #    num_jobs is ignored.  Instead, num_jobs is recalculated as
    #
    #     num_jobs = (len(subruns) + max_files_per_job - 1) / max_files_per_job
    #
    #     This formula has the feature that if len(subruns) == 1, or
    #     if max_files_per_job is equal to or greater than the number
    #     of suburns (or effectively infinite), then the final
    #     num_jobs paramater will always be one, meaning all input
    #     files will read in a single job.  On the other hand if
    #     max_files_per_job == 1, then num_jobs will be equal to the
    #     number of subruns, so each batch job will process one subrun.
    #
    # 4.  If input is from a singe file (and pubsinput is true), raise
    #     an exception.  This use case doesn't make sense and isn't
    #     supported.
    #
    # 5.  If no input is specified, don't do anything, since there is no
    #     input to limit.

    def pubsify_input(self, run, subruns, version):

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
            newdef = project_utilities.create_limited_dataset(self.inputdef,
                                                              run,
                                                              subruns)
            if not newdef:
                raise PubsInputError(run, subruns[0], version)                
            self.inputdef = newdef

            # Set the number of submitted jobs assuming each worker will get 
            # self.max_files_per_job files.

            files_per_job = self.max_files_per_job
            if files_per_job == 0:
                files_per_job = 1
            self.num_jobs = (len(subruns) + files_per_job - 1) / files_per_job

            # Done.

            return

        # If we get to here, we have input from a file list and a previous stage
        # exists.  This normally indicates a daisy chain.  This is where subcases
        # 3 (a), (b) are handled.

        # Case 3(a), single subrun.

        if len(subruns) == 1:

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
                lines = larbatch_posix.readlines(self.inputlist)
            except:
                lines = []
            if len(lines) == 0:
                raise PubsInputError(run, subruns[0], version)

            # Verify that input files actually exist.

            for line in lines:
                input_file = line.strip()
                if not larbatch_posix.exists(input_file):
                    raise PubsInputError(run, subruns[0], version)

            # Specify that there will be exactly one job submitted.

            self.num_jobs = 1

            # Everything OK (case 3(a)).

            return

        # Case 3(b), multiple subruns.

        if len(subruns) > 1:

            # Generate a new input file list with a unique name and place 
            # it in the same directory as the original input list.  Note that
            # the input list may not actually exist at this point.  If it
            # doesn't exist, just use the original name.  If it already exists,
            # generate a different name.

            dir = os.path.dirname(self.inputlist)
            base = os.path.basename(self.inputlist)
            new_inputlist_path = self.inputlist
            if larbatch_posix.exists(new_inputlist_path):
                new_inputlist_path = '%s/%s_%s.list' % (dir, base, str(uuid.uuid4()))
            self.inputlist = new_inputlist_path

            # Defer opening the new input list file until after the original
            # input file is successfully opened.

            new_inputlist_file = None

            # Loop over subruns.  Read contents of pubs input list for each subrun.

            nsubruns = 0
            total_size = 0
            actual_subruns = []
            truncate = False
            for subrun in subruns:

                if truncate:
                    break

                nsubruns += 1

                if version == None:
                    pubs_path = '%d/%d' % (run, subrun)
                else:
                    pubs_path = '%d/%d/%d' % (version, run, subrun)

                subrun_inputlist = os.path.join(dir, pubs_path, base)
                lines = []
                try:
                    lines = larbatch_posix.readlines(subrun_inputlist)
                except:
                    lines = []
                if len(lines) == 0:
                    raise PubsInputError(run, subruns[0], version)
                for line in lines:
                    subrun_inputfile = line.strip()

                    # Test size and accessibility of this input file.

                    sr_size = -1
                    try:
                        sr = larbatch_posix.stat(subrun_inputfile)
                        sr_size = sr.st_size
                    except:
                        sr_size = -1

                    if sr_size > 0:
                        actual_subruns.append(subrun)
                        if new_inputlist_file == None:
                            print 'Generating new input list %s\n' % new_inputlist_path
                            new_inputlist_file = larbatch_posix.open(new_inputlist_path, 'w')
                        new_inputlist_file.write('%s\n' % subrun_inputfile)
                        total_size += sr.st_size

                    # If at this point the total size exceeds the target size,
                    # truncate the list of subruns and break out of the loop.

                    if self.target_size != 0 and total_size >= self.target_size:
                        truncate = True
                        break

            # Done looping over subruns.

            new_inputlist_file.close()

            # Raise an exception if the actual list of subruns is empty.

            if len(actual_subruns) == 0:
                raise PubsInputError(run, subruns[0], version)

            # Update the list of subruns to be the actual list of subruns.

            if len(actual_subruns) != len(subruns):
                print 'Truncating subrun list: %s' % str(actual_subruns)
                del subruns[:]
                subruns.extend(actual_subruns)

            # Set the number of submitted jobs assuming each worker will get 
            # self.max_files_per_job files.

            files_per_job = self.max_files_per_job
            if files_per_job == 0:
                files_per_job = 1
            self.num_jobs = (len(subruns) + files_per_job - 1) / files_per_job

            # Everything OK (case 3(b)).

            return

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

        # Append run and subrun to workdir, outdir, logdir, and bookdir.
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
            self.dynamic = 1
        self.outdir = os.path.join(self.outdir, pubs_path)
        self.logdir = os.path.join(self.logdir, pubs_path)
        self.bookdir = os.path.join(self.bookdir, pubs_path)


    # Raise an exception if any specified input file/list doesn't exist.
    # (We don't currently check sam input datasets).

    def checkinput(self):
        if self.inputfile != '' and not larbatch_posix.exists(self.inputfile):
            raise IOError, 'Input file %s does not exist.' % self.inputfile
        if self.inputlist != '' and not larbatch_posix.exists(self.inputlist):
            raise IOError, 'Input list %s does not exist.' % self.inputlist

        # If target size is nonzero, and input is from a file list, calculate
        # the ideal number of output jobs and override the current number 
        # of jobs.

        if self.target_size != 0 and self.inputlist != '':
            input_filenames = larbatch_posix.readlines(self.inputlist)
            size_tot = 0
            for line in input_filenames:
                filename = string.split(line)[0]
                filesize = larbatch_posix.stat(filename).st_size
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
        if not larbatch_posix.exists(self.outdir):
            raise IOError, 'Output directory %s does not exist.' % self.outdir
        if not larbatch_posix.exists(self.logdir):
            raise IOError, 'Log directory %s does not exist.' % self.logdir
        return
    
    # Raise an exception if output, log, work, or bookkeeping directory doesn't exist.

    def checkdirs(self):
        if not larbatch_posix.exists(self.outdir):
            raise IOError, 'Output directory %s does not exist.' % self.outdir
        if self.logdir != self.outdir and not larbatch_posix.exists(self.logdir):
            raise IOError, 'Log directory %s does not exist.' % self.logdir
        if not larbatch_posix.exists(self.workdir):
            raise IOError, 'Work directory %s does not exist.' % self.workdir
        if self.bookdir != self.logdir and not larbatch_posix.exists(self.bookdir):
            raise IOError, 'Bookkeeping directory %s does not exist.' % self.bookdir
        return
    
    # Make output, log, work, and bookkeeping directory, if they don't exist.

    def makedirs(self):
        if not larbatch_posix.exists(self.outdir):
            larbatch_posix.makedirs(self.outdir)
        if self.logdir != self.outdir and not larbatch_posix.exists(self.logdir):
            larbatch_posix.makedirs(self.logdir)
        if not larbatch_posix.exists(self.workdir):
            larbatch_posix.makedirs(self.workdir)
        if self.bookdir != self.logdir and not larbatch_posix.exists(self.bookdir):
            larbatch_posix.makedirs(self.bookdir)

        # If output is on dcache, make output directory group-writable.

        if self.outdir[0:6] == '/pnfs/':
            mode = stat.S_IMODE(larbatch_posix.stat(self.outdir).st_mode)
            if not mode & stat.S_IWGRP:
                mode = mode | stat.S_IWGRP
                larbatch_posix.chmod(self.outdir, mode)
        if self.logdir[0:6] == '/pnfs/':
            mode = stat.S_IMODE(larbatch_posix.stat(self.logdir).st_mode)
            if not mode & stat.S_IWGRP:
                mode = mode | stat.S_IWGRP
                larbatch_posix.chmod(self.logdir, mode)

        self.checkdirs()
        return
