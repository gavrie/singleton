import multiprocessing
import logging
import time
import os
import pytest

import singleton

log_format = '%(asctime)s %(process)d %(levelname)s %(filename)s:%(lineno)d %(name)s: %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format)

name = 'foo'

class TestSingleton(object):

    def test_parse_lsof_output(self):
        expectations = [
            (['p8184', 'lW'], ([], [8184])),
            (['p8184', 'l '], ([8184], [])),
            (['p8008', 'l ', 'p31119', 'l ', 'p31918', 'lW'], ([8008, 31119], [31918])),
            (['p8008', 'l ', 'l ', 'p31119', 'l ', 'p31918', 'lW'], ([8008, 31119], [31918])),
            (['p8008', 'lW', 'p31119', 'l ', 'p31918', 'lW'], ([31119], [8008, 31918])),
            (['p8184', 'l ', 'p8261', 'l ', 'l '], ([8184, 8261], [])),
        ]
        for raw, parsed in expectations:
            users, lockers = singleton.parse_lsof_output(raw)
            assert (users, lockers) == tuple(map(set, parsed))

        with pytest.raises(LookupError):
            singleton.parse_lsof_output(['p8008', 'l ', 'p31119', 'lx', 'p31918', 'lW'])
        with pytest.raises(LookupError):
            singleton.parse_lsof_output(['p8008', 'l ', 'p31119', 'xl', 'p31918', 'lW'])

    def test_single_lock(self):
        singleton.lock(name)
        singleton.unlock(name)

    def test_single_ensure_lock(self):
        singleton.ensure_lock(name)
        singleton.unlock(name)

    def test_locking_twice_kills_first_process(self):

        def lock_and_sleep(timeout):
            logging.info("In process %s, locking and sleeping...", os.getpid())
            singleton.ensure_lock(name)
            time.sleep(timeout)

        # First locker
        p1 = multiprocessing.Process(target=lock_and_sleep, args=(600,))
        p1.start()

        # Second locker, should kill first
        p2 = multiprocessing.Process(target=lock_and_sleep, args=(0,))
        p2.start()

        # Ensure first is dead and second is alive
        p1.join()
        p2.join()
        assert p1.exitcode != 0
        assert p2.exitcode == 0
