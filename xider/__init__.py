""" Implement a Xider driver for Python based on subclassing Redis-py for compatibility with existing Redis-py clients. """

import os
from redis import Redis
from redis.connection import (
    Connection,
    ConnectionPool,
)
try:
    from redis.connection import AbstractConnection
except ImportError:
    from redis.connection import Connection as AbstractConnection  # Make it work with older versions of redis

import yottadb as ydb

class Mfuncs:
    """ Provide access to M functions defined in `xider.ci` as if they were methods of this class.
        `xider.ci` must reside in the the same directory as this python file """
    def __init__(self, ci_file):
        """ Initialise M access using the supplied call-in file ci_file """
        self.ci_file = ci_file
        self.ci_handle = ydb.open_ci_table(ci_file)

    def __getattr__(self, key):
        """ Make Mfuncs instance.ROUTINE() invoke M routine ROUTINE via call-in table """
        def caller(*args, has_retval=None):
            if type(has_retval) != bool:
                func = caller.__qualname__
                raise TypeError(f"{func}: keyword argument 'has_retval' must be boolean")
            return ydb.cip(key, args, has_retval=has_retval)
        old_handle = ydb.switch_ci_table(self.ci_handle)
        ret = caller
        if old_handle is not None: ydb.switch_ci_table(old_handle)
        return ret

M = Mfuncs(os.path.join(os.path.dirname(__file__), 'xider.ci'))

class Driver:
    """ Python driver for Xider """
    name = 'Python'
    driver_version = (0, 0, 1)  # store as a tuple for easy version number comparison
    description = 'Redis-compatible Python driver for fast in-process API for Xider database'
    
    def __init__ (self, verbose=False, decode_responses=True):
        """ Initialize Xider
            decode_responses if True will decode in utf-8; otherwise using whatever decoder (string) it is set to """
        self.decode_responses = 'utf-8' if decode_responses is True else decode_responses
        self.verbose = verbose
        self.init()

    def __enter__(self): return self
    def __exit__ (self, _exc_type, _exc_val, _exc_tb): self.terminate()
    def close(self): self.terminate()

    def init(self):
        """ Initialize Xider; return Xider meta constants in self.xider_version and self.keyFieldMaxLength. """
        ydb.set('xider', ('driverName',), self.name)
        ydb.set('xider', ('driverVersion',), '.'.join(map(str, self.driver_version)))
        ydb.set('xider', ('description',), self.description)
        ydb.set('xider', ('noParamsValidation',), '0')
        M.init(has_retval=False)
        self.keyFieldMaxLength = int(ydb.get('xider', ('ret', 'keyFieldMaxLength')))
        xider_version = ydb.get('xider', ('ret', 'version')).decode('utf-8')
        self.xider_version = tuple(map(int, xider_version.split('.')))  # split into a tuple for easy version number comparison

    def terminate(self):
        """ Opposite of init(): terminate use of xider, which removes the xider session.
            If this doesn't get called then the session will be removed when the Helper process
            detects that the PID doesn't exists anymore. """
        M.terminate(has_retval=False)

    # Map args to Xider-specific argnames for this command.
    # Longer term, these should all be changed in Xider to make all params called paramN
    arg_names_default = ('key', 'value')
    arg_names = {
        'SET': ('key', 'value', lambda i: (f'params{i-2}',)),
        'DEL': (lambda i: ('keys', str(i)),),
        'HSET': ('key', lambda i: ('data',str(i//2),'field'), lambda i: ('data',str(i//2),'value')),
        'HGET': ('key', 'field'),
        'HDEL': ('key', lambda i: ('fields', str(i-1))),
        'HINCRBY': ('key', 'field', 'increment'),
        'INCRBY': ('key', 'increment'),
        'WATCH': (lambda i: ('keys', str(i)),),
        # Other commands use the default above
    }

    def command(self, cmd, *args):
        if self.verbose: print("Command:", cmd, *args, end=' => ')
        cmd = cmd.upper()
        # Map args to Xider-specific arg_names for this particular command.
        # This up to M.call() can go away once Xider supports all params called paramN
        arg_names = self.arg_names.get(cmd, self.arg_names_default)
        backtrack = 0
        subscripts = []
        for i, arg in enumerate(args):
            if i-backtrack >= len(arg_names):
                while callable(arg_names[i-1-backtrack]): backtrack += 1  # repeat the callables for the remaining args
            subscript = arg_names[i-backtrack]
            if callable(subscript): subscript = subscript(i+1)
            subscripts = subscript if isinstance(subscript, tuple) else (subscript,)
            ydb.set('xider', subscripts, arg)
        ret = int(M.call(cmd, has_retval=True), 10)
        # the next lines should be made orthogonal in Xider so that ret returns 2 and xider('ret') return n
        if ret >= 0 and cmd in ['DEL', 'HSET', 'HDEL']: return ret
        if ret == 0: return b'OK'.decode(self.decode_responses) if self.decode_responses else b'OK'
        elif ret == 1:
            retval = ydb.get('xider', ('ret',))
            if self.decode_responses and isinstance(retval, bytes): retval = retval.decode(self.decode_responses)
            if self.verbose: print(retval[:100] if isinstance(retval, (str, bytes)) else retval)
            return retval
        return f"Error {ret}: Not implemented"  # still need to handle case of ret==2 (large string returned) and error codes: fix in Xider to return error messages

    def __getattr__(self, cmd):
        """ Make driver instance.CMD() invoke Redis command CMD via Xider """
        return lambda *args: self.command(cmd, *args)


# Subclass redis-py classes to make them use in-process Xider instead of a RESP connection

class XiderConnection(AbstractConnection):
    """ Manages in-process communication to and from a Xider server
        by overriding send_command and receive """

    def __init__(self, gbldir=None, verbose=False, **kwargs):
        """ Open a connection to YottaDB database at gbldir; defaults to os.getenv(gbldir)
            set verbose=True to print commands and replies """
        if not gbldir:
            gbldir = os.environ.get('ydb_gbldir', "")
        if gbldir:
            os.environ['ydb_gbldir'] = gbldir
        self.gbldir = gbldir
        self.verbose = verbose
        super().__init__(**kwargs)
        self.driver = Driver(verbose=verbose, decode_responses=False)

    def repr_pieces(self):
        """ Return essential identifiers relating to the connection (shown when printing a connection) """
        pieces = [("gbldir", self.gbldir), ("db", self.db)]
        if self.client_name:
            pieces.append(("client_name", self.client_name))
        return pieces

    def _connect(self):
        """ Connect to database (which does init() for Xider) """
        # let connection do the decoding rather than Xider driver
        self.driver.init()  # This is ok to call twice: it is also called by Driver.__init__()
        return self.driver

    def disconnect(self, *args):
        """ Disconnect from Xider (just calls Xider terminate() as there are no actual connections) """
        if hasattr(self, 'driver'): self.driver.terminate()

    def _host_error(self):
        """ Return path to database gbldir (used when printing errors) """
        return self.gbldir

    def send_command(self, *args, **kwargs):
        """ Pack and send in-process command to Xider """
        if self.verbose: print("Sending:", *args, f"kwargs={kwargs}" if kwargs else '', end='=> ')
        self._response = self.driver.command(args[0], *args[1:])
        # save self._response to return via read_response()

    def read_response(self, disable_decoding=False, **kwargs):
        """ Read the response from a previously sent command """
        response = self._response
        if not disable_decoding:
            response = self.encoder.decode(response)
        if self.verbose: print(response[:100] if isinstance(response, (str, bytes)) else response)
        return response

class XiderConnectionPool(ConnectionPool):
    """ Subclass of ConnectionPool to override its default connection_class to use a XiderConnection """

    def __init__(self, *args, verbose=False, **kwargs):
        self.verbose = verbose
        super().__init__(*args, connection_class=XiderConnection, **kwargs)

    def get_connection(self, command_name, *keys, **options):
        """ Get a connection from the pool """
        # Overrides the default which re-uses connections from a pool;
        # Xider doesn't need a pool because a 'connection' doesn't really exist;
        # it's just a class instance to hold attributes required by Redis redis-py
        conn = self.make_connection()
        conn.verbose = self.verbose
        return conn

class Xider(Redis):
    """ Subclass of Redis that make a connection to Xider instead """

    def __init__(self, *args, verbose=False, **kwargs):
        self.verbose = verbose
        super().__init__(*args,
            connection_pool=XiderConnectionPool(verbose=verbose, **kwargs),
            **kwargs)
        self.auto_close_connection_pool = True   # auto-calls self.close() which calls self.disconnect()
