#! /usr/bin/env python
######################################################################
#
# Name: larbatch_utilities.py
#
# Purpose: This module contains low level utilities that are used in
#          either modules project_utilities or larbatch_posix.
#
# Created: 13-Jun-2016  Herbert Greenlee
#
# The following functions are provided as interfaces to ifdh.  These
# functions are equipped with authentication checking, timeouts and
# other protections.
#
# ifdh_cp - Interface for "ifdh cp."
# ifdh_ls - Interface for "ifdh ls."
# ifdh_ll - Interface for "ifdh ll."
# ifdh_mkdir - Interface for "ifdh mkdir."
# ifdh_rmdir - Interface for "ifdh rmdir."
# ifdh_mv - Interface for "ifdh mv."
# ifdh_rm - Interface for "ifdh rm."
# ifdh_chmod - Interface for "ifdh chmod."
#
# The following functions are provided as interfaces to posix tools
# with additional protections or timeouts.
#
# posix_cp - Copy file with timeout.
#
# Authentication functions.
#
# test_ticket - Raise an exception of user does not have a valid kerberos ticket.
# get_kca - Get a kca certificate.
# get_proxy - Get a grid proxy.
# test_kca - Get a kca certificate if necessary.
# text_proxy - Get a grid proxy if necessary.
# get_experiment - Get standard experiment name.
# get_user - Get authenticated user.
# get_prouser - Get production user.
# get_role - Get VO role.
#
# SAM functions.
#
# dimensions - Return sam query dimensions for stage.
# get_sam_metadata - Return sam metadata fcl parameters for stage.
# get_bluearc_server - Sam fictitious server for bluearc.
# get_dcache_server - Sam fictitious server for dCache.
# get_dropbox - Return dropbox based on sam metadata.
#
# Other functions.
#
# get_ups_products - Top level ups products.
# get_setup_script_path - Full path of experiment setup script.
# wait_for_subprocess - For use with subprocesses with timeouts.
# dcache_server - Return dCache server.
# dcache_path - Convert dCache local path to path on server.
# xrootd_server_port - Return xrootd server and port (as <server>:<port>).
# xrootd_uri - Convert dCache path to xrootd uri.
# gridftp_uri - Convert dCache path to gridftp uri.
# srm_uri - Convert dCache path to srm uri.
# nfs_server - Node name of a computer in which /pnfs filesystem is nfs-mounted.
# parse_mode - Parse the ten-character file mode string ("ls -l").
#
######################################################################

import os
import stat
import subprocess
import getpass
import threading
import Queue
from project_modules.ifdherror import IFDHError

# Global variables.

ticket_ok = False
kca_ok = False
proxy_ok = False
kca_user = ''

# Copy file using ifdh, with timeout.

def ifdh_cp(source, destination):

    # Get proxy.

    test_proxy()

    # Make sure environment variables X509_USER_CERT and X509_USER_KEY
    # are not defined (they confuse ifdh, or rather the underlying tools).

    save_vars = {}
    for var in ('X509_USER_CERT', 'X509_USER_KEY'):
        if os.environ.has_key(var):
            save_vars[var] = os.environ[var]
            del os.environ[var]

    # Do copy.

    cmd = ['ifdh', 'cp', source, destination]
    jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    q = Queue.Queue()
    thread = threading.Thread(target=wait_for_subprocess, args=[jobinfo, q])
    thread.start()
    thread.join(timeout=600)
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
        raise IFDHError(cmd, rc, jobout, joberr)

    # Restore environment variables.

    for var in save_vars.keys():
        os.environ[var] = save_vars[var]


# Ifdh ls, with timeout.
# Return value is list of lines returned by "ifdh ls" command.

def ifdh_ls(path, depth):

    # Get proxy.

    test_proxy()

    # Make sure environment variables X509_USER_CERT and X509_USER_KEY
    # are not defined (they confuse ifdh, or rather the underlying tools).

    save_vars = {}
    for var in ('X509_USER_CERT', 'X509_USER_KEY'):
        if os.environ.has_key(var):
            save_vars[var] = os.environ[var]
            del os.environ[var]

    # Do listing.

    cmd = ['ifdh', 'ls', path, '%d' % depth]
    jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
        raise IFDHError(cmd, rc, jobout, joberr)

    # Restore environment variables.

    for var in save_vars.keys():
        os.environ[var] = save_vars[var]

    # Done.

    return jobout.splitlines()


# Ifdh ll, with timeout.
# Return value is list of lines returned by "ifdh ls" command.

def ifdh_ll(path, depth):

    # Get proxy.

    test_proxy()

    # Make sure environment variables X509_USER_CERT and X509_USER_KEY
    # are not defined (they confuse ifdh, or rather the underlying tools).

    save_vars = {}
    for var in ('X509_USER_CERT', 'X509_USER_KEY'):
        if os.environ.has_key(var):
            save_vars[var] = os.environ[var]
            del os.environ[var]

    # Do listing.

    cmd = ['ifdh', 'll', path, '%d' % depth]
    jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
        raise IFDHError(cmd, rc, jobout, joberr)

    # Restore environment variables.

    for var in save_vars.keys():
        os.environ[var] = save_vars[var]

    # Done.

    return jobout.splitlines()


# Ifdh rmdir, with timeout.

# Ifdh rmdir, with timeout.

def ifdh_mkdir(path):

    # Get proxy.

    test_proxy()

    # Make sure environment variables X509_USER_CERT and X509_USER_KEY
    # are not defined (they confuse ifdh, or rather the underlying tools).

    save_vars = {}
    for var in ('X509_USER_CERT', 'X509_USER_KEY'):
        if os.environ.has_key(var):
            save_vars[var] = os.environ[var]
            del os.environ[var]

    # Do mkdir.

    cmd = ['ifdh', 'mkdir', path]
    jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
        raise IFDHError(cmd, rc, jobout, joberr)

    # Restore environment variables.

    for var in save_vars.keys():
        os.environ[var] = save_vars[var]

    # Done.

    return


def ifdh_rmdir(path):

    # Get proxy.

    test_proxy()

    # Make sure environment variables X509_USER_CERT and X509_USER_KEY
    # are not defined (they confuse ifdh, or rather the underlying tools).

    save_vars = {}
    for var in ('X509_USER_CERT', 'X509_USER_KEY'):
        if os.environ.has_key(var):
            save_vars[var] = os.environ[var]
            del os.environ[var]

    # Do rmdir.

    cmd = ['ifdh', 'rmdir', path]
    jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
        raise IFDHError(cmd, rc, jobout, joberr)

    # Restore environment variables.

    for var in save_vars.keys():
        os.environ[var] = save_vars[var]

    # Done.

    return


# Ifdh chmod, with timeout.

def ifdh_chmod(path, mode):

    # Get proxy.

    test_proxy()

    # Make sure environment variables X509_USER_CERT and X509_USER_KEY
    # are not defined (they confuse ifdh, or rather the underlying tools).

    save_vars = {}
    for var in ('X509_USER_CERT', 'X509_USER_KEY'):
        if os.environ.has_key(var):
            save_vars[var] = os.environ[var]
            del os.environ[var]

    # Do chmod.

    cmd = ['ifdh', 'chmod', '%o' % mode, path]
    jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
        raise IFDHError(cmd, rc, jobout, joberr)

    # Restore environment variables.

    for var in save_vars.keys():
        os.environ[var] = save_vars[var]

    # Done.

    return


# Ifdh mv, with timeout.

def ifdh_mv(src, dest):

    # Get proxy.

    test_proxy()

    # Make sure environment variables X509_USER_CERT and X509_USER_KEY
    # are not defined (they confuse ifdh, or rather the underlying tools).

    save_vars = {}
    for var in ('X509_USER_CERT', 'X509_USER_KEY'):
        if os.environ.has_key(var):
            save_vars[var] = os.environ[var]
            del os.environ[var]

    # Do rename.

    cmd = ['ifdh', 'mv', src, dest]
    jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
        raise IFDHError(cmd, rc, jobout, joberr)

    # Restore environment variables.

    for var in save_vars.keys():
        os.environ[var] = save_vars[var]

    # Done.

    return


# Ifdh rm, with timeout.

def ifdh_rm(path):

    # Get proxy.

    test_proxy()

    # Make sure environment variables X509_USER_CERT and X509_USER_KEY
    # are not defined (they confuse ifdh, or rather the underlying tools).

    save_vars = {}
    for var in ('X509_USER_CERT', 'X509_USER_KEY'):
        if os.environ.has_key(var):
            save_vars[var] = os.environ[var]
            del os.environ[var]

    # Do delete.

    cmd = ['ifdh', 'rm', path]
    jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
        raise IFDHError(cmd, rc, jobout, joberr)

    # Restore environment variables.

    for var in save_vars.keys():
        os.environ[var] = save_vars[var]

    # Done.

    return


# Posix copy with timeout.

def posix_cp(source, destination):

    cmd = ['cp', source, destination]

    # Fork buffer process.

    buffer_pid = os.fork()
    if buffer_pid == 0:

        # In child process.
        # Launch cp subprocess.

        jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        q = Queue.Queue()
        thread = threading.Thread(target=wait_for_subprocess, args=[jobinfo, q])
        thread.start()
        thread.join(timeout=60)
        if thread.is_alive():

            # Subprocess did not finish (may be hanging and unkillable).
            # Try to kill the subprocess and exit process.
            # Unkillable process will become detached.

            print 'Terminating subprocess.'
            jobinfo.kill()
            os._exit(1)

        else:

            # Subprocess finished normally.

            rc = q.get()
            jobout = q.get()
            joberr = q.get()
            os._exit(rc)

    else:

        # In parent process.
        # Wait for buffer subprocess to finish.

        buffer_result = os.waitpid(buffer_pid, 0)
        rc = buffer_result[1]/256
        if rc != 0:
            raise IFDHError(cmd, rc, '', '')

    # Done.

    return


# Function to wait for a subprocess to finish and fetch return code,
# standard output, and standard error.
# Call this function like this:
#
# q = Queue.Queue()
# jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# wait_for_subprocess(jobinfo, q, input)
# rc = q.get()      # Return code.
# jobout = q.get()  # Standard output
# joberr = q.get()  # Standard error


def wait_for_subprocess(jobinfo, q, input=None):
    jobout, joberr = jobinfo.communicate(input)
    rc = jobinfo.poll()
    q.put(rc)
    q.put(jobout)
    q.put(joberr)
    return


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
             '-rfc',
             '-cert', os.environ['X509_USER_CERT'],
             '-key', os.environ['X509_USER_KEY'],
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

# Return dCache server.

def dcache_server():
    return "fndca1.fnal.gov"


# Convert a local pnfs path to the path on the dCache server.
# Return the input path unchanged if it isn't on dCache.

def dcache_path(path):
    if path.startswith('/pnfs/') and not path.startswith('/pnfs/fnal.gov/usr/'):
        return '/pnfs/fnal.gov/usr/' + path[6:]


# Return xrootd server and port.

def xrootd_server_port():
    return dcache_server() + ':1094'


# Convert a pnfs path to xrootd uri.
# Return the input path unchanged if it isn't on dCache.

def xrootd_uri(path):
    if path.startswith('/pnfs/'):
        return 'root://' + xrootd_server_port() + dcache_path(path)
    else:
        return path


# Convert a pnfs path to gridftp uri.
# Return the input path unchanged if it isn't on dCache.

def gridftp_uri(path):
    if path.startswith('/pnfs/'):
        return 'gsiftp://' + dcache_server() + dcache_path(path)
    else:
        return path


# Convert a pnfs path to srm uri.
# Return the input path unchanged if it isn't on dCache.

def srm_uri(path):
    if path.startswith('/pnfs/'):
        return 'srm://fndca1.fnal.gov:8443/srm/managerv2?SFN=/pnfs/fnal.gov/usr/' + path[6:]
    else:
        return path


# Return the name of a computer with login access that has the /pnfs 
# filesystem nfs-mounted.  This function makes use of the $EXPERIMENT
# environment variable (as does ifdh), which must be set.

def nfs_server():
    return '%sgpvm01.fnal.gov' % os.environ['EXPERIMENT']


# Parse the ten-character file mode string as returned by "ls -l" 
# and return mode bit masek.

def parse_mode(mode_str):

    mode = 0

    # File type.

    if mode_str[0] == 'b':
        mode += stat.S_IFBLK
    elif mode_str[0] == 'c':
        mode += stat.S_IFCHR
    elif mode_str[0] == 'd':
        mode += stat.S_IFDIR
    elif mode_str[0] == 'l':
        mode += stat.S_IFLNK
    elif mode_str[0] == 'p':
        mode += stat.S_IFIFO
    elif mode_str[0] == 's':
        mode += stat.S_IFSOCK
    elif mode_str[0] == '-':
        mode += stat.S_IFREG

    # File permissions.

    # User triad (includes setuid).

    if mode_str[1] == 'r':
        mode += stat.S_IRUSR
    if mode_str[2] == 'w':
        mode += stat.S_IWUSR
    if mode_str[3] == 'x':
        mode += stat.S_IXUSR
    elif mode_str[3] == 's':
        mode += stat.S_ISUID
        mode += stat.S_IXUSR
    elif mode_str[3] == 'S':
        mode += stat.S_ISUID

    # Group triad (includes setgid).

    if mode_str[4] == 'r':
        mode += stat.S_IRGRP
    if mode_str[5] == 'w':
        mode += stat.S_IWGRP
    if mode_str[6] == 'x':
        mode += stat.S_IXGRP
    elif mode_str[6] == 's':
        mode += stat.S_ISGID
        mode += stat.S_IXGRP
    elif mode_str[6] == 'S':
        mode += stat.S_ISGID

    # World triad (includes sticky bit).
                    
    if mode_str[7] == 'r':
        mode += stat.S_IROTH
    if mode_str[8] == 'w':
        mode += stat.S_IWOTH
    if mode_str[9] == 'x':
        mode += stat.S_IXOTH
    elif mode_str[9] == 't':
        mode += stat.S_ISVTX
        mode += stat.S_IXOTH
    elif mode_str[9] == 'T':
        mode += stat.S_ISVTX

    # Done

    return mode

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


# Import experiment-specific utilities.  In this imported module, one can 
# override any function or symbol defined above, or add new ones.

from experiment_utilities import *
