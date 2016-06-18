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
# functions are equipped with timeouts and other protections.
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
# Authentication functions.
#
# test_ticket - Raise an exception of user does not have a valid kerberos ticket.
# get_kca - Get a kca certificate.
# get_proxy - Get a grid proxy.
# test_kca - Get a kca certificate if necessary.
# text_proxy - Get a grid proxy if necessary.
#
# Other functions.
#
# wait_for_subprocess - For use with subprocesses with timeouts.
# dcache_server - Return dCache server.
# dcache_path - Convert dCache local path to path on server.
# xrootd_server_port - Return xrootd server and port (as <server>:<port>).
# xrootd_uri - Convert dCache path to xrootd uri.
# gridftp_uri - Convert dCache path to gridftp uri.
# nfs_server - Node name of a computer in which /pnfs filesystem is nfs-mounted.
# parse_mode - Parse the ten-character file mode string ("ls -l").
#
######################################################################

# Copy file using ifdh, with timeout.

def ifdh_cp(source, destination):

    # Get proxy.

    larbatch_utilities.test_proxy()

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
    thread = threading.Thread(target=larbatch_utilities.wait_for_subprocess, args=[jobinfo, q])
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


# Ifdh ls, with timeout.
# Return value is list of lines returned by "ifdh ls" command.

def ifdh_ls(path, depth):

    # Get proxy.

    larbatch_utilities.test_proxy()

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
    thread = threading.Thread(target=larbatch_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    larbatch_utilities.test_proxy()

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
    thread = threading.Thread(target=larbatch_utilities.wait_for_subprocess, args=[jobinfo, q])
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

def ifdh_rmdir(path):

    # Get proxy.

    larbatch_utilities.test_proxy()

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
    thread = threading.Thread(target=larbatch_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    larbatch_utilities.test_proxy()

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
    thread = threading.Thread(target=larbatch_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    larbatch_utilities.test_proxy()

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
    thread = threading.Thread(target=larbatch_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    larbatch_utilities.test_proxy()

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
    thread = threading.Thread(target=larbatch_utilities.wait_for_subprocess, args=[jobinfo, q])
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
        mode += statmod.S_IFBLK
    elif mode_str[0] == 'c':
        mode += statmod.S_IFCHR
    elif mode_str[0] == 'd':
        mode += statmod.S_IFDIR
    elif mode_str[0] == 'l':
        mode += statmod.S_IFLNK
    elif mode_str[0] == 'p':
        mode += statmod.S_IFIFO
    elif mode_str[0] == 's':
        mode += statmod.S_IFSOCK
    elif mode_str[0] == '-':
        mode += statmod.S_IFREG

    # File permissions.

    # User triad (includes setuid).

    if mode_str[1] == 'r':
        mode += statmod.S_IRUSR
    if mode_str[2] == 'w':
        mode += statmod.S_IWUSR
    if mode_str[3] == 'x':
        mode += statmod.S_IXUSR
    elif mode_str[3] == 's':
        mode += statmod.S_ISUID
        mode += statmod.S_IXUSR
    elif mode_str[3] == 'S':
        mode += statmod.S_ISUID

    # Group triad (includes setgid).

    if mode_str[4] == 'r':
        mode += statmod.S_IRGRP
    if mode_str[5] == 'w':
        mode += statmod.S_IWGRP
    if mode_str[6] == 'x':
        mode += statmod.S_IXGRP
    elif mode_str[6] == 's':
        mode += statmod.S_ISGID
        mode += statmod.S_IXGRP
    elif mode_str[6] == 'S':
        mode += statmod.S_ISGID

    # World triad (includes sticky bit).
                    
    if mode_str[7] == 'r':
        mode += statmod.S_IROTH
    if mode_str[8] == 'w':
        mode += statmod.S_IWOTH
    if mode_str[9] == 'x':
        mode += statmod.S_IXOTH
    elif mode_str[9] == 't':
        mode += statmod.S_ISVTX
        mode += statmod.S_IXOTH
    elif mode_str[9] == 'T':
        mode += statmod.S_ISVTX

    # Done

    return mode
