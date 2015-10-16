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
import socket
import subprocess
import getpass
import uuid
import samweb_cli

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

proxy_ok = False
kca_ok = False
ticket_ok = False
kca_user = ''
samweb_obj = None       # Initialized SAMWebClient object


# Function to return the current experiment.
# The following places for obtaining this information are
# tried (in order):
#
# 1.  Environment variable $EXPERIMENT.
# 2.  Environment variable $SAM_EXPERIMENT.
# 3.  Hostname (up to "gpvm").
#
# Raise an exception if none of the above methods works.
#

def get_experiment():

    exp = ''
    for ev in ('EXPERIMENT', 'SAM_EXPERIMENT'):
        if os.environ.has_key(ev):
            exp = os.environ[ev]
            break

    if not exp:
        hostname = socket.gethostname()
        n = hostname.find('gpvm')
        if n > 0:
            exp = hostname[:n]

    if not exp:
        raise RuntimeError, 'Unable to determine experiment.'

    return exp

# Function to return the fictitious disk server node
# name used by sam for bluearc disks.

def get_bluearc_server():
    return get_experiment() + 'data:'

# Function to return the fictitious disk server node
# name used by sam for dCache disks.

def get_dcache_server():
    return 'fnal-dcache:'

# Function to determine dropbox directory based on sam metadata.
# Raise an exception if the specified file doesn't have metadata.
# This function should be overridden in <experiment>_utilities module.

def get_dropbox(filename):
    raise RuntimeError, 'Function get_dropbox not implemented.'

# Function to return string containing sam metadata in the form 
# of an fcl configuraiton.  It is intended that this function
# may be overridden in experiment_utilities.py.

def get_sam_metadata(project, stage):
    result = ''
    return result

# Get role (normally 'Analysis' or 'Production').

def get_role():

    # If environment variable ROLE is defined, use that.  Otherwise, make
    # an educated guess based on user name.

    result = 'Analysis'   # Default role.

    # Check environment variable $ROLE.

    if os.environ.has_key('ROLE'):
        result = os.environ['ROLE']

    # Otherwise, check user.

    else:
        prouser = get_experiment() + 'pro'
        user = getpass.getuser()
        if user == prouser:
            result = 'Production'

    return result

# Get authenticated user (from kerberos ticket, not $USER).

def get_user():

    # See if we have a cached value for user.

    global kca_user
    if kca_user != '':
        return kca_user

    # Return production user name if Role is Production

    if get_role() == 'Production':
        return get_prouser()

    else:

        # First make sure we have a kca certificate (raise exception if not).

        test_kca()

        # Return user name from certificate if Role is Analysis

        subject = ''
        if os.environ.has_key('X509_USER_PROXY'):
            subject = subprocess.check_output(['voms-proxy-info',
                                               '-file', os.environ['X509_USER_PROXY'],
                                               '-subject'], stderr=-1)
        elif os.environ.has_key('X509_USER_CERT') and os.environ.has_key('X509_USER_KEY'):
            subject = subprocess.check_output(['voms-proxy-info',
                                               '-file', os.environ['X509_USER_CERT'],
                                               '-subject'], stderr=-1)
        else:
            subject = subprocess.check_output(['voms-proxy-info', '-subject'], stderr=-1)

        # Get the last non-numeric CN

        cn = ''
        while cn == '':
            n = subject.rfind('/CN=')
            if n >= 0:
                cn = subject[n+4:]
                if cn.strip().isdigit():
                    cn = ''
                    subject = subject[:n]
            else:
                break

        # Truncate everything after the first '/'.

        n = cn.find('/')
        if n >= 0:
            cn = cn[:n]

        # Truncate everything after the first newline.

        n = cn.find('\n')
        if n >= 0:
            cn = cn[:n]

        # Truncate everything before the first ":" (UID:).

        n = cn.find(':')
        if n >= 0:
            cn = cn[n+1:]

        # Done (maybe).

        if cn != '':
            return cn

    # Something went wrong...

    raise RuntimeError, 'Unable to determine authenticated user.'        

# Function to optionally convert a filesystem path into an xrootd url.
# Only affects paths in /pnfs space.

def path_to_url(path):
    url = path
    #if path[0:6] == '/pnfs/':
    #    url = 'root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr/' + path[6:]
    return url

# Function to optionally convert a filesystem path into an srm url.
# Only affects paths in /pnfs space.

def path_to_srm_url(path):
    srm_url = path
    if path[0:6] == '/pnfs/':
        srm_url = 'srm://fndca1.fnal.gov:8443/srm/managerv2?SFN=/pnfs/fnal.gov/usr/' + path[6:]
    return srm_url

# dCache-safe method to test whether path exists without opening file.

def safeexist(path):
    try:
        os.stat(path)
        return True
    except:
        return False

# Test whether user has a valid kerberos ticket.  Raise exception if no.

def test_ticket():
    global ticket_ok
    if not ticket_ok:
        ok = subprocess.call(['klist', '-s'], stdout=-1, stderr=-1)
        if ok != 0:
            raise RuntimeError, 'Please get a kerberos ticket.'
        ticket_ok = True
    return ticket_ok


# Get kca certificate.

def get_kca():

    global kca_ok
    kca_ok = False

    # First, make sure we have a kerberos ticket.

    krb_ok = test_ticket()
    if krb_ok:

        # Get kca certificate.

        kca_ok = False
        try:
            subprocess.check_call(['kx509'], stdout=-1, stderr=-1)
            kca_ok = True
        except:
            pass

    # Done

    return kca_ok


# Get grid proxy.
# This implementation should be good enough for experiments in the fermilab VO.
# Experiments not in the fermilab VO (lbne/dune) should override this function
# in experiment_utilities.py.

def get_proxy():

    global proxy_ok
    proxy_ok = False

    # Make sure we have a valid certificate.

    test_kca()

    # Get proxy using either specified cert+key or default cert.

    if os.environ.has_key('X509_USER_CERT') and os.environ.has_key('X509_USER_KEY'):
        cmd=['voms-proxy-init',
             '-cert', os.environ['X509_USER_CERT'],
             '-key', os.environ['X509_USER_KEY'],
             '-valid', '48:0',
             '-voms', 'fermilab:/fermilab/%s/Role=%s' % (get_experiment(), get_role())]
        try:
            subprocess.check_call(cmd, stdout=-1, stderr=-1)
            proxy_ok = True
        except:
            pass
        pass
    else:
        cmd=['voms-proxy-init',
             '-noregen',
             '-rfc',
             '-voms',
             'fermilab:/fermilab/%s/Role=%s' % (get_experiment(), get_role())]
        try:
            subprocess.check_call(cmd, stdout=-1, stderr=-1)
            proxy_ok = True
        except:
            pass

    # Done

    return proxy_ok


# Test whether user has a valid kca certificate.  If not, try to get a new one.

def test_kca():
    global kca_ok
    if not kca_ok:
        try:
            if os.environ.has_key('X509_USER_PROXY'):
                subprocess.check_call(['voms-proxy-info',
                                       '-file', os.environ['X509_USER_PROXY'],
                                       '-exists'], stdout=-1, stderr=-1)
            elif os.environ.has_key('X509_USER_CERT') and os.environ.has_key('X509_USER_KEY'):
                subprocess.check_call(['voms-proxy-info',
                                       '-file', os.environ['X509_USER_CERT'],
                                       '-exists'], stdout=-1, stderr=-1)
            else:
                subprocess.check_call(['voms-proxy-info', '-exists'], stdout=-1, stderr=-1)

                # Workaround jobsub bug by setting environment variable X509_USER_PROXY to
                # point to the default location of the kca certificate.

                x509_path = subprocess.check_output(['voms-proxy-info', '-path'], stderr=-1)
                os.environ['X509_USER_PROXY'] = x509_path.strip()

            kca_ok = True
        except:
            pass

    # If at this point we don't have a kca certificate, try to get one.

    if not kca_ok:
        get_kca()

    # Final checkout.

    if not kca_ok:
        try:
            if os.environ.has_key('X509_USER_PROXY'):
                subprocess.check_call(['voms-proxy-info',
                                       '-file', os.environ['X509_USER_PROXY'],
                                       '-exists'], stdout=-1, stderr=-1)
            elif os.environ.has_key('X509_USER_CERT') and os.environ.has_key('X509_USER_KEY'):
                subprocess.check_call(['voms-proxy-info',
                                       '-file', os.environ['X509_USER_CERT'],
                                       '-exists'], stdout=-1, stderr=-1)
            else:
                subprocess.check_call(['voms-proxy-info', '-exists'], stdout=-1, stderr=-1)
            kca_ok = True
        except:
            raise RuntimeError, 'Please get a kca certificate.'
    return kca_ok


# Test whether user has a valid grid proxy.  If not, try to get a new one.

def test_proxy():
    global proxy_ok
    if not proxy_ok:
        try:
            subprocess.check_call(['voms-proxy-info', '-exists'], stdout=-1, stderr=-1)
            subprocess.check_call(['voms-proxy-info', '-exists', '-acissuer'], stdout=-1, stderr=-1)
            proxy_ok = True
        except:
            pass

    # If at this point we don't have a grid proxy, try to get one.

    if not proxy_ok:
        get_proxy()

    # Final checkout.

    if not proxy_ok:
        try:
            subprocess.check_call(['voms-proxy-info', '-exists'], stdout=-1, stderr=-1)
            subprocess.check_call(['voms-proxy-info', '-exists', '-acissuer'], stdout=-1, stderr=-1)
            proxy_ok = True
        except:
            raise RuntimeError, 'Please get a grid proxy.'
    return proxy_ok

# dCache-safe method to return contents (list of lines) of file.

def saferead(path):
    lines = []
    if os.path.getsize(path) == 0:
        return lines
    #if path[0:6] == '/pnfs/':
    #    test_ticket()
    #    lines = subprocess.check_output(['ifdh', 'cp', path, '/dev/fd/1']).splitlines()
    #else:
    lines = open(path).readlines()
    return lines

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
            os.path.isdir(path):
        result = True
    return result

# Wait for file to appear on local filesystem.

def wait_for_stat(path):

    ntry = 60
    while ntry > 0:
        if os.access(path, os.R_OK):
            return 0
        print 'Waiting ...'

        # Reading the parent directory seems to make files be visible faster.

        os.listdir(os.path.dirname(path))
        time.sleep(1)
        ntry = ntry - 1

    # Timed out.

    return 1

# Method to optionally make a copy of a pnfs file.

def path_to_local(path):

    # Depending on the input path and the environment, this method
    # will do one of the following things.
    #
    # 1.  If the input path is a pnfs path (starts with "/pnfs/"), and
    #     if $TMPDIR is defined and is accessible, the pnfs file will
    #     be copied to $TMPDIR using ifdh, and the path of the local
    #     copy will be returned.
    #
    # 2.  If the input path is a pnfs path, and if $TMPDIR is not
    #     defined, is not accessible, or if the ifdh copy fails,
    #     this method will return the empty string ("").
    #
    # 3.  If the input path is anything except a pnfs path, this
    #     method will not do any copy and will return the input path.
    #     

    #global proxy_ok
    #result = ''

    # Is this a pnfs path?
    # Turn off special treatment of pnfs paths (always use posix access).

    #if path[0:6] == '/pnfs/':

    #    # Is there a temp directory?

    #    local = ''
    #    if os.environ.has_key('TMPDIR'):
    #        tmpdir = os.environ['TMPDIR']
    #        mode = os.stat(tmpdir).st_mode
    #        if stat.S_ISDIR(mode) and os.access(tmpdir, os.W_OK):
    #            local = os.path.join(tmpdir, os.path.basename(path))

    #    if local != '':

    #        # Do local copy.

    #        test_ticket()

    #        # Make sure local path doesn't already exist (ifdh cp may fail).

    #        if os.path.exists(local):
    #            os.remove(local)

    #        # Use ifdh to make local copy of file.

    #        #print 'Copying %s to %s.' % (path, local)
    #        rc = subprocess.call(['ifdh', 'cp', path, local], stdout=sys.stdout, stderr=sys.stderr)
    #        if rc == 0:
    #            rc = wait_for_stat(local)
    #            if rc == 0:

    #                # Copy succeeded.

    #                result = local

    #else:

    #    # Not a pnfs path.

    result = path

    return result


# DCache-safe TFile-like class for opening files for reading.
#
# Class SafeTFile acts as follows.
#
# 1.  When initialized with a pnfs path (starts with "/pnfs/"), SafeTFile uses
#     one of the following methods to open the file.
#
#     a) Open as a regular file (posix open).
#
#     b) Convert the path to an xrootd url (xrootd open).
#
#     c) Copy the file to a local temp disk using ifdh (copy to $TMPDIR or
#        local directory) using ifdh, and open the local copy.
#
# 2.  When initialized with anything except a pnfs path (including regular
#     filesystem paths and urls), SafeTFile acts exactly like TFile.
#
# Implementation notes.
#
# This class has the following data member.
#
# root_tfile - a ROOT.TFile object.
#
# This class aggregates rather than inherits from ROOT.TFile because the owned
# TFile can itself be polymorphic.
#
#

class SafeTFile:

    # Default constructor.

    def __init__(self):
        self.root_tfile = None

    # Initializing constructor.

    def __init__(self, path):
        self.Open(path)

    # Destructor.

    def __del__(self):
        self.Close()

    # Unbound (static) Open method.

    def Open(path):
        return SafeTFile(path)

    # Bound Open method.

    def Open(self, path):

        self.root_tfile = None

        # Open file, with special handling for pnfs paths.

        local = path_to_local(path)
        if local != '':

            # Open file or local copy.

            self.root_tfile = ROOT.TFile.Open(local)

            # Now that the local copy is open, we can safely delete it already.

            if local != path:
                os.remove(local)

        else:

            # Input path is pnfs, but we could not get a local copy.
            # Get xrootd url instead.a

            global proxy_ok
            if not proxy_ok:
                proxy_ok = test_proxy()
            url = path_to_url(path)
            #print 'Using url %s' % url
            self.root_tfile = ROOT.TFile.Open(url)

    # Close method.

    def Close(self):

        # Close file and delete temporary file (if any and not already deleted).

        if self.root_tfile != None and self.root_tfile.IsOpen():
            self.root_tfile.Close()
            self.root_tfile = None

    # Copies of regular TFile methods used in project.py.

    def IsOpen(self):
        return self.root_tfile.IsOpen()

    def IsZombie(self):
        return self.root_tfile.IsZombie()

    def GetListOfKeys(self):
        return self.root_tfile.GetListOfKeys()

    def Get(self, objname):
        return self.root_tfile.Get(objname)

# Function to return a comma-separated list of run-time top level ups products.

def get_ups_products():
    return get_experiment() + 'code'

# Function to return path of experiment bash setup script that is valid
# on the node where this script is being executed.
# This function should be overridden in <experiment>_utilities.py.

def get_setup_script_path():
    raise RuntimeError, 'Function get_setup_script_path not implemented.'

# Function to return dimension string for project, stage.
# This function should be overridden in experiment_utilities.py

def dimensions(project, stage, ana=False):
    raise RuntimeError, 'Function dimensions not implemented.'

# Function to return the production user name

def get_prouser():
    return get_experiment() + 'pro'

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
        if not os.path.isdir(scratch) or not os.access(scratch, os.W_OK):
            scratch = '/%s/data/users/%s' % (get_experiment(), get_user())

    # Checkout.

    if scratch == '':
        raise RuntimeError, 'No scratch directory specified.'

    if not os.path.isdir(scratch) or not os.access(scratch, os.W_OK):
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

    # Take a snapshot of the original dataset definition.

    snapid = None
    try:

        # Make sure we have a kca certificate.

        test_kca()

        # Take the snapshot

        snapid = samweb().takeSnapshot(defname, group=get_experiment())
    except:
        snapid = None
    if snapid == None:
        print 'Failed to make snapshot of dataset definition %s' % defname
        return ''

    # Construct dimension including run and subrun constraints.

    dim = "snapshot_id %d and run_number %s" % (snapid, run_subrun_dim)

    # Test the new dimension.

    nfiles = samweb().countFiles(dimensions=dim)
    if nfiles == 0:
        return ''

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


# Import experiment-specific utilities.  In this imported module, one can 
# override any function or symbol defined above, or add new ones.

from experiment_utilities import *
