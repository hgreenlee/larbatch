#!/usr/bin/env python
#----------------------------------------------------------------------
#
# Name: project_utilities.py
#
# Purpose: A python module containing various python utility functions
#          and classes used by project.py and other python scripts.
#
# Created: 28-Oct-2013  H. Greenlee
#
#----------------------------------------------------------------------

import sys, os, stat, time, types
import pycurl, StringIO
import datetime
import socket
import subprocess
import shutil
import threading
import Queue
import uuid
import samweb_cli
from project_modules.ifdherror import IFDHError
import larbatch_posix
import larbatch_utilities
from larbatch_utilities import get_experiment, get_user, get_role, get_prouser
from larbatch_utilities import test_ticket, test_kca, test_proxy, get_kca, get_proxy
from larbatch_utilities import dimensions
from larbatch_utilities import wait_for_subprocess
from larbatch_utilities import get_bluearc_server
from larbatch_utilities import get_dcache_server
from larbatch_utilities import get_dropbox
from larbatch_utilities import get_sam_metadata
from larbatch_utilities import get_ups_products
from larbatch_utilities import get_setup_script_path

# Prevent root from printing garbage on initialization.
if os.environ.has_key('TERM'):
    del os.environ['TERM']

# Hide command line arguments from ROOT module.
myargv = sys.argv
sys.argv = myargv[0:1]
import ROOT
ROOT.gErrorIgnoreLevel = ROOT.kError
sys.argv = myargv

# Global variables.

samweb_obj = None       # Initialized SAMWebClient object


# Like os.path.isdir, but faster by avoiding unnecessary i/o.

def fast_isdir(path):
    result = False
    if path[-5:] != '.list' and \
            path[-5:] != '.root' and \
            path[-4:] != '.txt' and \
            path[-4:] != '.fcl' and \
            path[-4:] != '.out' and \
            path[-4:] != '.err' and \
            path[-3:] != '.sh' and \
            path[-5:] != '.stat' and \
            larbatch_posix.isdir(path):
        result = True
    return result

# Wait for file to appear on local filesystem.

def wait_for_stat(path):

    ntry = 60
    while ntry > 0:
        if larbatch_posix.access(path, os.R_OK):
            return 0
        print 'Waiting ...'

        # Reading the parent directory seems to make files be visible faster.

        larbatch_posix.listdir(os.path.dirname(path))
        time.sleep(1)
        ntry = ntry - 1

    # Timed out.

    return 1

# Function to return the path of a scratch directory which can be used
# for creating large temporary files.  The scratch directory should not 
# be in dCache.  The default implementation here uses the following algorithm.
#
# 1.  Environment variable TMPDIR.
#
# 2.  Environment variable SCRATCH.
#
# 3.  Path /scratch/<experiment>/<user>
#
# 4.  Path /<experiment>/data/users/<user>
#
# Raise an exception if the scratch directory doesn't exist or is not writeable.

def get_scratch_dir():
    scratch = ''

    # Get scratch directory path.

    if os.environ.has_key('TMPDIR'):
        scratch = os.environ['TMPDIR']

    elif os.environ.has_key('SCRATCH'):
        scratch = os.environ['SCRATCH']

    else:
        scratch = '/scratch/%s/%s' % (get_experiment(), get_user())
        if not larbatch_posix.isdir(scratch) or not larbatch_posix.access(scratch, os.W_OK):
            scratch = '/%s/data/users/%s' % (get_experiment(), get_user())

    # Checkout.

    if scratch == '':
        raise RuntimeError, 'No scratch directory specified.'

    if not larbatch_posix.isdir(scratch) or not larbatch_posix.access(scratch, os.W_OK):
        raise RuntimeError, 'Scratch directory %s does not exist or is not writeable.' % scratch

    return scratch

# Function to return the mountpoint of a given path.

def mountpoint(path):

    # Handle symbolic links and relative paths.

    path = os.path.realpath(path)

    # Find mountpoint.

    while not os.path.ismount(path):
        dir = os.path.dirname(path)
        if len(dir) >= len(path):
            return dir
        path = dir

    return path


# Function to escape dollar signs in string by prepending backslash (\).

def dollar_escape(s):

    result = ''
    for c in s:
        if c == '$' and ( len(result) == 0 or result[-1] != '\\'):
            result += '\\'
        result += c
    return result


# Function to parse a string containing a comma- and hyphen-separated 
# representation of a collection of positive integers into a sorted list 
# of ints.  Raise ValueError excpetion in case of unparseable string.

def parseInt(s):

    result = set()

    # First split string into tokens separated by commas.

    for token in s.split(','):

        # Plain integers handled here.

        if token.strip().isdigit():
            result.add(int(token))
            continue

        # Hyphenenated ranges handled here.

        limits = token.split('-')
        if len(limits) == 2 and limits[0].strip().isdigit() and limits[1].strip().isdigit():
            result |= set(range(int(limits[0]), int(limits[1])+1))
            continue

        # Don't understand.

        raise ValueError, 'Unparseable range token %s.' % token

    # Return result in form of a sorted list.

    return sorted(result)


# Function to construct a new dataset definition from an existing definition
# such that the new dataset definition will be limited to a specified run and
# set of subruns.
#
# The name of the new definition is returned as the return value of
# the function.
#
# If the new query does not return any files, the new dataset is not created, 
# and the function returns the empty string ('').

def create_limited_dataset(defname, run, subruns):

    if len(subruns) == 0:
        return ''

    # Construct comma-separated list of run-subrun pairs in a form that is
    # acceptable as sam dimension constraint.

    run_subrun_dim = ''
    for subrun in subruns:
        if run_subrun_dim != '':
            run_subrun_dim += ','
        run_subrun_dim += "%d.%d" % (run, subrun)

    # Construct dimension including run and subrun constraints.

    dim = "defname: %s and run_number %s" % (defname, run_subrun_dim)

    # Test the new dimension.

    nfiles = samweb().countFiles(dimensions=dim)
    if nfiles == 0:
        return ''

    # Make sure we have a kca certificate.

    test_kca()

    # Construct a new unique definition name.

    newdefname = defname + '_' + str(uuid.uuid4())

    # Create definition.

    samweb().createDefinition(newdefname, dim, user=get_user(), group=get_experiment())

    # Done (return definition name).

    return newdefname

# Return initialized SAMWebClient object.

def samweb():

    global samweb_obj

    if samweb_obj == None:
        samweb_obj = samweb_cli.SAMWebClient(experiment=get_experiment())

    os.environ['SSL_CERT_DIR'] = '/etc/grid-security/certificates'

    return samweb_obj

# Start sam project.

def start_project(defname, default_prjname, max_files, force_snapshot):

    # Check project name.

    s = samweb()
    prjname = default_prjname
    if prjname == '':
        prjname = s.makeProjectName(defname)
    print 'Starting project %s' % prjname

    # Make sure we have a certificate.

    test_kca()

    # Figure out how many files are in the input dataset.

    nf = s.countFiles('defname: %s' % defname)
    print 'Input dataset has %d files.' % nf
    if nf == 0:
        return 1

    # Make limited dataset?

    if max_files > 0 and nf > max_files:
        limitdef = '%s_limit_%d' % (defname, max_files)

        # Figure out whether limitdef already exists.

        def_exists = False
        try:
            s.descDefinition(limitdef)
            def_exists = True
        except:
            def_exists = False

        if def_exists:
            print 'Using already created limited dataset definition %s.' % limitdef
        else:
            print 'Creating limited dataset definition %s.' % limitdef
            dim = 'defname: %s with limit %d' % (defname, max_files)
            s.createDefinition(limitdef, dim, user=get_user(), group=get_experiment())

        defname = limitdef
        nf = max_files

    # Force snapshot?

    if force_snapshot:
        print 'Forcing snapthot.'
        defname = '%s:force' % defname

    # Start the project.

    print 'Starting project %s.' % prjname
    s.startProject(prjname,
                   defname=defname, 
                   station=get_experiment(),
                   group=get_experiment(),
                   user=get_user())

    # Done.

    return 0

# Return a list of active projects associated with a particular dataset definition stem
# based on project start and end times.  The particular criteria used in this function
# are:
#
# 1.  Project started within the last 72 hours.
#
# 2.  Project ended within the last 12 hours, or no end time.

def active_projects2(defname = ''):

    result = set()

    # Get project name stem.

    s = samweb()
    prjstem = ''
    if defname != '':
        prjstem = '%s_' % s.makeProjectName(defname).rsplit('_',1)[0]

    # Query a list of projects started within the last 72 hours.

    dt = datetime.timedelta(3, 0)
    tmin = datetime.datetime.utcnow() - dt
    tminstr = tmin.strftime('%Y-%m-%dT%H:%M:%S')
    prjnames = s.listProjects(started_after = tminstr)

    # Loop over projects to check end times.

    for prjname in prjnames:
        if prjstem == '' or prjname.startswith(prjstem):

            # This project is a candidate for inclusion in result.
            # Check end time.

            age = 0
            prjurl = s.findProject(project=prjname, station=get_experiment())
            if prjurl != '':
                prjsum = s.projectSummary(prjurl)
                if prjsum.has_key('project_end_time'):
                    tendstr = prjsum['project_end_time']
                    if len(tendstr) >= 19:
                        try:
                            tend = datetime.datetime.strptime(tendstr[:19], '%Y-%m-%dT%H:%M:%S')
                            tage = datetime.datetime.utcnow() - tend
                            age = tage.total_seconds()
                        except:
                            pass

            # Keep this project if the end time is within 12 hourse of the current
            # time, or if there is no end time.

            if age < 12*3600:
                result.add(prjname)

    # Done.

    return result


# Return a list of active projects associated with a particular dataset definition stem.
# If the definition argument is the empty string, return all active projects.

def active_projects(defname = ''):

    result = set()

    # Get project name stem.

    s = samweb()
    prjstem = ''
    if defname != '':
        prjstem = '%s_' % s.makeProjectName(defname).rsplit('_',1)[0]

    # Dump station

    url = '%s/dumpStation?station=%s' % (s.get_baseurl(), get_experiment())
    buffer = StringIO.StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.USERPWD, 'uboone:argon!')
    c.setopt(c.FOLLOWLOCATION, True)
    c.setopt(c.WRITEFUNCTION, buffer.write)
    c.perform()
    c.close()

    # Parse response.

    buffer.seek(0)
    for line in buffer.readlines():
        words = line.split()
        if len(words) > 0 and words[0] == 'project':
            prjname = words[1].split('(')[0]
            if prjstem == '' or prjname.startswith(prjstem):
                result.add(prjname)

    # Done.

    return result

# Make active projects dataset definition.
# 
# defname        - Dataset definition associated with active projects.
# dropbixwait    - Dropbox wait interval (float days).
# active_defname - Name of dataset definition to create.
# wait_defname   - Name of dropbox waiting dataset to create.

def make_active_project_dataset(defname, dropboxwait, active_defname, wait_defname):

    s = samweb()
    test_kca()

    # Get list of active projects.

    prjs = active_projects(defname) | active_projects2(defname)

    # Make sam dimension.

    dim = ''
    for prj in prjs:
        if dim == '':
            dim = 'snapshot_for_project_name %s' % prj
        else:
            dim += ',%s' % prj

    # If there were no matching projects, make up some legal dimension that won't
    # match any files.

    if dim == '':
        dim = 'file_id 0'

    # Create or update active_defname.

    def_exists = False
    try:
        s.descDefinition(active_defname)
        def_exists = True
    except:
        def_exists = False

    if def_exists:
        print 'Updating dataset definition %s' % active_defname
        s.deleteDefinition(active_defname)
    else:
        print 'Creating dataset definition %s' % active_defname

    s.createDefinition(active_defname, dim, user=get_user(), group=get_experiment())

    # If the dropbox waiting interval is nonzero, create a dataset for 
    # dropbox waiting files.

    dim = ''
    if dropboxwait > 0.:

        # Convert the waiting interval to a datetime.timedelta object.

        dt = datetime.timedelta(int(dropboxwait), int(dropboxwait % 1 * 86400))

        # Get the earliest allowed time.

        tmin = datetime.datetime.utcnow() - dt

        # Format time in a form acceptable to sam.

        tminstr = tmin.strftime('%Y-%m-%dT%H:%M:%S')

        # Append sam dimension.

        dim = "isparentof: (create_date > '%s' and availability: virtual)" % tminstr

    else:

        # Otherwise make dummy dataset.

        dim = 'file_id 0'

    # Create or update active_defname.

    def_exists = False
    try:
        s.descDefinition(wait_defname)
        def_exists = True
    except:
        def_exists = False

    if def_exists:
        print 'Updating dataset definition %s' % wait_defname
        s.deleteDefinition(wait_defname)
    else:
        print 'Creating dataset definition %s' % wait_defname

    s.createDefinition(wait_defname, dim, user=get_user(), group=get_experiment())


# Function to ensure that files in dCache have layer two.
# This function is included here as a workaround for bugs in the dCache nfs interface.

def addLayerTwo(path, recreate=True):

    # Don't do anything if this file is not located in dCache (/pnfs/...)
    # or has nonzero size.

    if larbatch_posix.exists(path) and path[0:6] == '/pnfs/' and larbatch_posix.stat(path).st_size == 0:

        if recreate:
            print 'Adding layer two for path %s.' % path
        else:
            print 'Deleting empty file %s.' % path

        # Now we got a zero size file in dCache, which kind of files may be
        # missing layer two.
        # Delete the file and recreate it using ifdh.

        larbatch_posix.remove(path)
        if not recreate:
            return
        test_proxy()

        # Make sure environment variables X509_USER_CERT and X509_USER_KEY
        # are not defined (they confuse ifdh).

        save_vars = {}
        for var in ('X509_USER_CERT', 'X509_USER_KEY'):
            if os.environ.has_key(var):
                save_vars[var] = os.environ[var]
                del os.environ[var]

        # Do ifdh cp.

        command = ['ifdh', 'cp', '/dev/null', path]
        jobinfo = subprocess.Popen(command, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        q = Queue.Queue()
        thread = threading.Thread(target=wait_for_subprocess, args=[jobinfo, q])
        thread.start()
        thread.join(timeout=60)
        if thread.is_alive():
            print 'Terminating subprocess.'
            jobinfo.terminate()
            thread.join()
        rc = q.get()
        jobout = q.get()
        joberr = q.get()
        if rc != 0:
            for var in save_vars.keys():
                os.environ[var] = save_vars[var]
            raise IFDHError(command, rc, jobout, joberr)

        # Restore environment variables.

        for var in save_vars.keys():
            os.environ[var] = save_vars[var]

# This function returns jobsub_submit options that should be included for 
# all batch submissions.

def default_jobsub_submit_options():
    opt = '--append_condor_requirements=\'(TARGET.HAS_CVMFS_%s_opensciencegrid_org==true)\'' % get_experiment()
    return opt

# Check the health status of the batch system and any other resources that 
# are required to submit batch jobs successfully.  The idea is that this 
# function may be called before submitting batch jobs.  If this function 
# returns false, batch jobs should not be submitted, and this failure should
# not be counted as an error.  The default implementation here always returns
# true, but may be overridden in experiment_utilities.

def batch_status_check():
    return True

# The following functions are included for backward compatibility.
# The actual implementations have been moved to larbatch_posix or 
# larbatch_utilities, with a different name.

def path_to_srm_url(path):
    return larbatch_utilities.srm_uri(path)

def safeexist(path):
    return larbatch_posix.exists(path)

def saferead(path):
    if safeexist(path):
        return larbatch_posix.readlines(path)
    else:
        return []

def safecopy(src, dest):
    return larbatch_posix.copy(src, dest)

# The following functions are depracated and function as no-ops.
# They are included for backward compatibility.

def path_to_url(path):
    return path

def path_to_local(path):
    return path

# Class SafeTFile is retired.  For compatibility, calls to the former 
# constructor of class SafeTFile are now simply passed to the ROOT
# TFile open method.  Note that class SafeTFile only ever supported
# opening root files for reading.

def SafeTFile(path):
    return ROOT.TFile.Open(path)
