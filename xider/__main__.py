#!/usr/bin/env python3

""" Makes the xider module runs as a xider-cli (akin to redis-cli), e.g.:
        python -m xider SET x 3 ...
"""

if __name__ == '__main__':
    import sys
    import os

    # Allow import of ../xider (current directory) as xider
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) )
    import xider

    verbose = False
    if '-v' in sys.argv: verbose = True; sys.argv.remove('-v')
    if '--verbose' in sys.argv: verbose = True; sys.argv.remove('--verbose')

    with xider.Driver(verbose=verbose) as x:
        result = x.command(*sys.argv[1:])
        print('(nil)' if result is None else result)
