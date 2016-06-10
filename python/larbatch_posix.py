#! /usr/bin/env python
######################################################################
#
# Name: larbatch_posix.py
#
# Purpose: Python module containing posix-like interfaces for 
#          accessing files in dCache (/pnfs/...) that do not
#          require the /pnfs filesystem to be nfs-mounted.
#
# Created: 8-Dec-2016  Herbert Greenlee
#
# Classes:
#
# dcache_file - File-like class for dCache files.  This class operates
#               on a local copy of the file, which is transferred
#               to/from dCache when the file is closed or opened using
#               ifdh.  For compatibility, this class can be used for
#               non-dCache files, in which case calls are simply
#               passed to standard python File calls, and there is no
#               initial or final file transfer.  This class implements
#               the following methods.
#
#               - close
#               - flush
#               - fileno
#               - next
#               - read
#               - readline
#               - readlines
#               - xreadlines
#               - seek
#               - tell
#               - truncate
#               - write
#               - writelines
#
# Functions:
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
# The following posix-like global functions are provided by this
# module.  These mostly correspond to functions having the same name
# in the os module.  Typically, these functions handle paths
# corresponding to dCache files (/pnfs/...) in some special way.  The
# /pnfs filesystem does not have to be nfs-mounted.  Non-dCache paths
# are simply passed to corresponding standard python functions.
#
# open - Similar as built-in python function open.  Returns a
#        file-like object of type dcache_file (in cast of dCache
#        paths) or built-in type File (in case of regular files).
# listdir - Similar as os.listdir.  List directory contents.
# exists - Similar as os.path.exists.  Return True if path exists.
# isdir - Simmilar as os.path.isdir.  Return True if path is a directory.
# stat - Similar as os.stat.  Return status information about a file.
# access - Similar as os.access.  Test file access.
# walk - Similar as os.walk.  Walk directory tree.
# mkdir - Similar as os.mkdir.  Make directory.
# makedirs - Similar as os.makedirs.  Make directory and parent directories.
# rename - Similar as os.rename.  Rename file.
# remove - Similar as os.remove.  Delete file.
# rmdir - Similar as os.rmdir.  Delete empty directory.
# rmtree - Similar as shutil.rmtree.  Delete a directory tree.
# chmod - Similar as os.chmod.  Change file permissions.
# symlink - Similar as os.symlink.  Make a symbolic link.
# readlink - Similar as os.readlink.  Read a symbolic link.
#
# Utility functions:
#
# dcache_server - Return dCache server.
# dcache_path - Convert dCache local path to path on server.
# xrootd_server_port - Return xrootd server and port (as <server>:<port>).
# xrootd_uri - Convert dCache path to xrootd uri.
# gridftp_uri - Convert dCache path to gridftp uri.
# nfs_server - Node name of a computer in which /pnfs filesystem is nfs-mounted.
# parse_mode - Parse the ten-character file mode string ("ls -l").
#
######################################################################

import os, shutil
import stat as statmod
import subprocess
import threading
import Queue
import StringIO
import project_utilities
from project_modules.ifdherror import IFDHError

# File-like class for dCache files.

class dcache_file:

    # Default constructor.

    def __init__(self):
        self.path = ''            # Path of file.
        self.mode = ''            # File mode.
        self.local_path = ''      # Path of local copy of file.
        self.local_file = None    # Open File object of local copy of file.

    # Initializing constructor.

    def __init__(self, path, mode='r', buf=-1):

        self.path = path
        self.mode = mode
        if path.startswith('/pnfs/'):
            self.local_path = os.path.basename(path)
        else:
            self.local_path = path

        # Fetch copy of file from dCache, if necessary.

        if path != self.local_path and (mode.find('r') >= 0 or mode.find('a') >= 0):
            ifdh_cp(path, self.local_path)

        # Open local copy of file.

        self.local_file = __builtins__['open'](self.local_path, mode, buf)

    # Destructor.

    def __del__(self):
        self.close()

    # Close file.

    def close(self):

        if self.local_file and not self.local_file.closed:

            # Close local copy of file.

            self.local_file.close()

            # If the local path and real path are different, do some cleanups.

            if self.path != self.local_path:

                # If file was opend for writing, transfer local copy to dCache.

                if self.mode.find('w') >= 0 or self.mode.find('a') >= 0 or self.mode.find('+') >= 0:
                    ifdh_cp(self.local_path, self.path)

                # Delete the local copy regardless of whether the file was open for
                # reading or writing.

                os.remove(self.local_path)

    # Flush file.

    def flush(self):
        self.local_file.flush()

    # File descriptor.

    def fileno(self):
        return self.local_file.fileno()

    # Iterator.

    def next(self):
        return self.local_file.next()

    # Read the specified number of bytes.

    def read(self, size=-1):
        return self.local_file.read(size)

    # Read one line, up to specified number of bytes.

    def readline(self, size=-1):
        return self.local_file.readline(size)

    # Read multiple lines.

    def readlines(self, sizehint=-1):
        return self.local_file.readlines()

    # Same as next.

    def xreadlines(self):
        return self.local_file.xreadlines()

    # Return file position.

    def tell(self):
        return self.local_file.tell()

    # Truncate file (no argument version).

    def truncate(self):
        self.local_file.truncate()

    # Truncate file (position argument).

    def truncate(self, pos):
        self.local_file.truncate(pos)

    # Write a string.

    def write(self, str):
        self.local_file.write(str)

    # Write multiple strings.

    def writelines(self, strs):
        self.local_file.writelines(strs)

# Global functions.


# Copy file using ifdh, with timeout.

def ifdh_cp(source, destination):

    # Get proxy.

    project_utilities.test_proxy()

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
    thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    project_utilities.test_proxy()

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
    thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    project_utilities.test_proxy()

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
    thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    project_utilities.test_proxy()

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
    thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    project_utilities.test_proxy()

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
    thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    project_utilities.test_proxy()

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
    thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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

    project_utilities.test_proxy()

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
    thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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


# Open file

def open(path, mode='r', buf=-1):
    if path.startswith('/pnfs/'):
        return dcache_file(path, mode, buf)
    else:
        return __builtins__['open'](path, mode, buf)


# List directory contents.

def listdir(path):

    result = []
    if path.startswith('/pnfs/'):

        # Get normalized tail.

        tail = os.path.normpath(path[-6:])

        # Call "ifdh ls".

        contents = ifdh_ls(path, 1)

        # Loop over contents returned by ifdh.
        # Normalize the paths returned by "ifdh ls", which in this context mainly
        # has the effect of stripping off trailing '/' on directories.
        # Filter out parent directory, which ifdh sometimes (usually?) includes in result.

        for c in contents:
            nc = os.path.normpath(c.strip())
            if not nc.endswith(tail):
                result.append(os.path.basename(nc))

    else:
        result = os.listdir(path)

    # Done.

    return result


# Test existence.  Works for files and directories.

def exists(path):

    result = False
    if path.startswith('/pnfs/'):

        # Do "ifdh ls."

        try:
            ifdh_ls(path, 0)
            result = True
        except:
            result = False

    else:
        result = os.path.exists(path)

    # Done.

    return result


# Test existence if directory.

def isdir(path):

    result = False
    if path.startswith('/pnfs/'):

        # Make sure path exists before trying to determine if it is a directory.

        if exists(path):

            # The only reliable way to get information about a directory is 
            # to get a listing of the parent directory.

            npath = os.path.normpath(path)      # Strip trailing '/'
            name = os.path.basename(npath)
            dir = os.path.dirname(npath)
            lines = ifdh_ll(dir, 1)
            for line in lines:
                words = line.split()
                if len(words) > 5 and words[-1] == name:
                    if words[0][0] == 'd':
                        result = True


    else:
        result = os.path.isdir(path)

    # Done.

    return result


# Get file status.  
#
# This function is a partial emulation of os.stat.  The return value
# is a type os.stat_result, which is a 10-tuple of values.  The following
# values are filled by this function, since information is not always
# available from grid tools.
#
# mode  - File type and permissions.
# uid   - Owner uid.  For compatibility, always set to the process uid.
# gid   - Owner gid.  For compatibility, always set to the process gid.
# nlink - Number of links.
# size  - Object size.

def stat(path):

    result = os.stat_result((0,0,0,0,0,0,0,0,0,0))
    if path.startswith('/pnfs/'):

        # The only reliable way to get information about a directory is 
        # to get a listing of the parent directory.

        npath = os.path.normpath(path)      # Strip trailing '/'
        name = os.path.basename(npath)
        dir = os.path.dirname(npath)
        lines = ifdh_ll(dir, 1)
        for line in lines:
            words = line.split()
            if len(words) >5 and words[-1] == name:

                # Found thie path.  Fill result.

                # Interpret mode string.

                mode = parse_mode(words[0])

                # Get remaining fields.
                    
                nlinks = int(words[1])
                size = int(words[4])
                result = os.stat_result((mode,         # Mode
                                         0,            # Inode
                                         0,            # Device
                                         nlinks,       # Number of links
                                         os.getuid(),  # Uid
                                         os.getgid(),  # Gid
                                         size,         # Size
                                         0,            # Access time
                                         0,            # Mod time
                                         0))           # Creation time

    else:
        result = os.stat(path)

    # Done.

    return result


# Test file access.
# This implementation only tests access to dCache files via the
# user permission, which is a limitation of grid tools.

def access(path, mode):

    result = False
    if path.startswith('/pnfs/'):
        sr = stat(path)
        if sr.st_mode != 0:

            # File exists.

            result = True

            # Test permissions.

            if mode & os.R_OK:
                if not sr.st_mode & statmod.S_IRUSR:
                    result = False
            if mode & os.W_OK:
                if not sr.st_mode & statmod.S_IWUSR:
                    result = False
            if mode & os.X_OK:
                if not sr.st_mode & statmod.S_IXUSR:
                    result = False

    else:
        result = os.access(path, mode)

    # Done.

    return result


# Walk directory tree.  Like os.walk, this function returns a list of
# 3-tuples, one for each directory in the tree rooted in the specified
# top directory.  Each 3-tuple contains the following information.
#
# 1.  Path of directory.
# 2.  List of dictory names in this directory.
# 3.  List of non-directory files in this directory.
#
# The dCache implementation differs from the standard python implementation 
# of os.walk in the following ways.
#
# 1.  Python os.walk returns an iterator.  The dCache implementation returns
#     a completed list.
# 2.  The dCache implementation respects the topdown argument, but ignores
#     the remaining arguments (they are passed to os.walk for non-dCache files).

def walk(top, topdown=True, onerror=None, followlinks=False):
    result = []
    if top.startswith('/pnfs/'):

        # Retrieve the contents of this directory.

        lines = ifdh_ll(top, 1)
        dirs = []
        files = []
        for line in lines:
            words = line.split()
            if len(words) > 5:
                if words[0][0] == 'd':
                    dirs.append(words[-1])
                else:
                    files.append(words[-1])
        result.append((top, dirs, files))

        # Visit subdirectories recursively.

        for dir in dirs:
            result.extend(walk(os.path.join(top, dir)))   # Always top down, reverse at end.

        # Top down or bottom up?

        if not topdown:
            result.reverse()

    else:
        result = os.walk(top, topdown, onerror, followlinks)

    # Done

    return result


# Make directory (parent directory must exist).
# Mode argument is ignored for dCache files, but is passed to os.mkdir
# for non-dCache files.

def mkdir(path, mode=0777):
    if path.startswith('/pnfs/'):
        ifdh_mkdir(path)
    else:
        os.mkdir(path, mode)


# Make directory and parents.
# Mode argument is ignored for dCache files, but is passed to os.mkdir
# for non-dCache files.
# "ifdh mkdir_p" is buggy, so we do the recursion locally.

def makedirs(path, mode=0777):
    if path.startswith('/pnfs/'):

        # Make sure parent directory exists.

        np = os.path.normpath(path)    # Stip trailing '/', if present.
        parent = os.path.dirname(np)
        if not isdir(parent):
            makedirs(parent, mode)

        # Now make directory itself.

        ifdh_mkdir(path)
    else:
        os.mkdir(path, mode)


# Rename file.
# "ifdh mv" seems to be buggy, so use uberftp.

def rename(src, dest):
    if src.startswith('/pnfs/'):
        src_uri = gridftp_uri(src)
        dest_path = dcache_path(dest)
        cmd = ['uberftp', '-rename', src_uri, dest_path]
        jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        q = Queue.Queue()
        thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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
            raise IFDHError(cmd, rc, jobout, joberr)
    else:
        os.rename(src, dest)


# Delete file.

def remove(path):
    if path.startswith('/pnfs/'):
        ifdh_rm(path)
    else:
        os.remove(path)

# Delete empty directory.

def rmdir(path):
    if path.startswith('/pnfs/'):
        ifdh_rmdir(path)
    else:
        os.rmdir(path)

# Delete directory tree.

def rmtree(path):
    if path.startswith('/pnfs/'):

        # Delete contents recursively.

        lines = ifdh_ll(path, 1)
        for line in lines:
            words = line.split()
            if len(words) > 5:
                if words[0][0] == 'd':
                    rmtree(os.path.join(path, words[-1]))
                else:
                    remove(os.path.join(path, words[-1]))

        # Directory should be empty when we get to here.

        rmdir(path)

    else:
        shutil.rmtree(path)

    # Done

    return


# Change mode.

def chmod(path, mode):
    if path.startswith('/pnfs/'):
        ifdh_chmod(path, mode)
    else:
        os.chmod(path, mode)


# Make symbolic link.
# Use nfs.

def symlink(src, dest):

    # Make sure we have a kerberos ticket.

    project_utilities.test_ticket()

    if src.startswith('/pnfs/'):
        cmd = ['ssh', nfs_server(), 'ln', '-s', src, dest]
        jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        q = Queue.Queue()
        thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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
            raise IFDHError(cmd, rc, jobout, joberr)

    else:
        os.symlink(src, dest)


# Read a symbolic link.
# Use nfs.

def readlink(path):

    result = ''

    # Make sure we have a kerberos ticket.

    project_utilities.test_ticket()

    if path.startswith('/pnfs/'):
        cmd = ['ssh', nfs_server(), 'readlink', path]
        jobinfo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        q = Queue.Queue()
        thread = threading.Thread(target=project_utilities.wait_for_subprocess, args=[jobinfo, q])
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
            raise IFDHError(cmd, rc, jobout, joberr)
        result = jobout.strip()

    else:
        result = os.readlink(path)

    # Done.

    return result


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
