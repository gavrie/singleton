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

open_lockfiles = {}

def get_lockfile(name):
    lockfile_path = "/tmp/{}.lock".format(name)

    if lockfile_path in open_lockfiles:
        # If we have an open filehandle, return it.
        # This is necessary and not just an optimization, since closing the filehandle
        # (even though the lock is on another filehandle!) will release the lock.
        # See the man page for fcntl(2).
        return (open_lockfiles[lockfile_path], lockfile_path)

    # Create lock file if it doesn't exist.
    # Be careful to make it world-writable so that other users can use it as well.
    old_umask = os.umask(0)
    lockfile = open(lockfile_path, 'w')
    os.umask(old_umask)

    # Keep open filehandle
    open_lockfiles[lockfile_path] = lockfile
    return (lockfile, lockfile_path)

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
    del open_lockfiles[lockfile_path]

def parse_lsof_output(lines):
    users = set()
    lockers = set()
    pid = None

    for line in lines:
        if line[0] == 'p':
            pid = int(line[1:])
        elif line.lower() == 'lw':
            assert pid is not None, "Encountered lock line before process line in lsof output"
            lockers.add(pid)
        elif line == 'l ':
            assert pid is not None, "Encountered lock line before process line in lsof output"
            users.add(pid)
        else:
            raise LookupError("Encountered unexpected line in lsof output: '%s'" % line)

    return (users, lockers)

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

        lsof_output = subprocess.check_output(['lsof', '-Fl', lockfile_path])
        logger.debug("lsof output: %s", lsof_output)
        lines = lsof_output.splitlines()
        lockfile_users, lockfile_lockers = parse_lsof_output(lines)

        # Kill anyone who locks our resource
        if lockfile_lockers:
            for pid in lockfile_lockers:
                logger.debug("Process %s has locked the resource; going to kill it", pid)
                kill_process(pid)
        else:
            logger.debug("No processes found that lock the resource; "
                         "since lock detection may be unreliable, "
                         "we're going to kill all processes that have the resource open")
            for pid in lockfile_users:
                logger.debug("Process %s is using the resource and may have locked it; going to kill it", pid)
                kill_process(pid)

def kill_process(pid):
    """
    Kill the process.
    """

    pid = int(pid)
    if pid == os.getpid():
        logger.debug("Refusing to commit suicide, skipping")
        return

    remaining_signals = [(signal.SIGTERM, 10),
                         (signal.SIGKILL, 5),]

    while remaining_signals:
        sig, time_to_wait = remaining_signals.pop(0)
        logger.debug("Killing process %s with signal %s", pid, sig)
        try:
            os.kill(pid, sig)
        except EnvironmentError as e:
            if e.errno == errno.ESRCH:
                return

        start_time = time.time()
        deadline = start_time + time_to_wait

        while time.time() < deadline:
            logger.debug("Waiting for process %s to terminate...", pid)
            try:
                os.kill(pid, 0)
            except EnvironmentError as e:
                if e.errno == errno.ESRCH:
                    return

            logger.debug("Process not dead yet, sleeping...")
            time.sleep(1)

    logger.info("Killing process %s was unsuccessful!", pid)
