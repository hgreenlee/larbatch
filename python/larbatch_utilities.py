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
######################################################################

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

