#!/usr/bin/env python

import errno
import fcntl
import logging
import os
import signal
import subprocess
import time

"""
Ensure that the provided command line is run only once.
Kill it if it is already running; abort on failure.
"""

logger = logging.getLogger()

lockfiles = {}

def get_lockfile(name):
    lockfile_path = "/tmp/{}.lock".format(name)

    # Create lock file if it doesn't exist.
    # Be careful to make it world-writable so that other users can use it as well.
    old_umask = os.umask(0)
    lockfile = open(lockfile_path, 'w')
    os.umask(old_umask)

    # Keep open filehandle
    lockfiles[lockfile_path] = lockfile
    return lockfile, lockfile_path

def lock(name):
    """
    Lock exclusively, or abort.
    """
    lockfile, lockfile_path = get_lockfile(name)
    fcntl.lockf(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    logger.debug("Locked '%s' by pid %s", lockfile_path, os.getpid())

def unlock(name):
    lockfile, lockfile_path = get_lockfile(name)
    fcntl.lockf(lockfile, fcntl.LOCK_UN)
    logger.debug("Unlocked '%s' by pid %s", lockfile_path, os.getpid())
    lockfile.close()
    del lockfiles[lockfile_path]

def ensure_lock(name):
    """
    Try to lock, until succeeding.
    """
    while True:
        try:
            lock(name)
            return
        except EnvironmentError as e:
            if e.errno not in [errno.EAGAIN, errno.EACCES]:
                raise # Unexpected exception

        logger.info("Resource for '%s' is already locked, checking by whom...", name)
        lockfile, lockfile_path = get_lockfile(name)

        lines = subprocess.check_output(['lsof', '-Fl', lockfile_path]).splitlines()
        # lines: ['p8008', 'l ', 'p31119', 'l ', 'p31918', 'lW']

        locks = dict(zip([p[1:] for p in lines[0::2]],
                         [l[1:] for l in lines[1::2] if l.lower() == 'lw']))
        # locks: {'31918': 'W'}

        # Kill anyone who locks our resource
        for pid in locks:
            kill_process(pid)

def kill_process(pid):
    """
    Kill the process.
    """

    pid = int(pid)
    start_time = time.time()
    soft_deadline = start_time + 20
    hard_deadline = soft_deadline + 5
    sig = signal.SIGTERM

    while time.time() < hard_deadline:
        logging.debug("Killing process %s with signal %s", pid, sig)
        try:
            os.kill(pid, signal.SIGTERM)
        except EnvironmentError as e:
            if e.errno == errno.ESRCH:
                return

        logging.debug("Process not dead yet, sleeping...")
        if time.time() > soft_deadline:
            sig = signal.SIGKILL

        time.sleep(1)
