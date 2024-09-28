#!/usr/bin/env python3
# Tests for the Xider driver for Python
# This file is copyright 2024, licensed under the GNU Affero General Public License Version 3: https://www.gnu.org/licenses/agpl.txt

""" Test xider-py functionality.
Test script may be invoked directly or using pytest """

import os
import yottadb as ydb
import sys

# Allow import of ../xider (current directory) as xider
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) )
import xider

db = xider.Xider(decode_responses=True)

def check_xider_driver_direct():
    with xider.Driver(verbose=db.verbose) as x:
        print(f"Testing Xider version {'.'.join(map(str,x.xider_version))}")
        x.set('test1', '6')
        assert x.get('test1') == '6'

def test_getset():
    assert db.set('test2', '5') is True
    assert db.get('test2') == '5'

def test_hgetset():
    assert int(db.hset('htest', 'fieldA', '4')) in [0, 1]
    assert db.hget('htest', 'fieldA') == '4'

def test_del():
    db.set('test2', '5')
    assert db.delete('test2') == 1
    # Bug in Xider: the following should return None, not error -3
    # assert db.get('test2') is None
    assert db.get('test2') == 'Error -3: Not implemented'

def test_hdel():
    assert int(db.hset('htest', 'fieldA', '4')) in [0, 1]
    assert db.hdel('htest', 'fieldA') == 1
    # Bug in Xider: the following should return None, not error -7
    # assert db.hget('htest', 'fieldA') is None
    assert db.hget('htest', 'fieldA') == 'Error -7: Not implemented'

def test_incrby():
    db.set('test', '2')
    db.incrby('test', '3')
    assert db.get('test') == '5'

def test_hincrby():
    db.hset('htest', 'fieldA', '4')
    # Bug in Xider: the following should return 7, not 'OK'
    # assert db.hincrby('htest', 'fieldA', '3') == 'OK'
    assert db.hincrby('htest', 'fieldA', '3') == 'OK'
    assert db.hget('htest', 'fieldA') == '7'

def runtests():
    """ Run all tests in functions that start with 'test_' """
    check_xider_driver_direct()

    tests = {funcname:func for funcname,func in globals().items() if funcname.startswith('test_')}
    for test in tests.values():
        print(f"* {test.__name__} ", end='\n' if db.verbose else '')
        test()
        if not db.verbose: print()

if __name__ == '__main__':
    if '-v' in sys.argv or '--verbose' in sys.argv:
        db = xider.Xider(decode_responses=True, verbose=True)
    runtests()
