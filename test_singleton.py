import multiprocessing
import logging
import time
import os

import singleton

logging.basicConfig(level=logging.DEBUG)

name = 'foo'

class TestSingleton(object):

    def test_single_lock(self):
        singleton.lock(name)
        singleton.unlock(name)

    def test_single_ensure_lock(self):
        singleton.ensure_lock(name)
        singleton.unlock(name)

    def test_locking_twice_kills_first_process(self):

        def lock_and_sleep():
            logging.info("In process %s, locking and sleeping...", os.getpid())
            singleton.ensure_lock(name)
            time.sleep(600)

        # First locker
        p1 = multiprocessing.Process(target=lock_and_sleep)
        p1.start()

        # Second locker, should kill first
        p2 = multiprocessing.Process(target=lock_and_sleep)
        p2.start()

        # Ensure first is dead and second is alive
        p1.join()
        assert p1.exitcode != 0
        assert p2.is_alive() == True
        p2.terminate()
        p2.join()
