# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

"""Module holding server definitions

"""

import MySQLdb as _connector
import collections
import warnings

from mysql.replicant import (
    configmanager,
    roles,
    )

#pylint: disable-msg=W0232
class Position(collections.namedtuple('Position', ['file', 'pos'])):
    """A binlog position for a specific server.

    """

    def __cmp__(self, other):
        """Compare two positions lexicographically.  If the positions
        are from different servers, a ValueError exception will be
        raised.
        """
        return cmp((self.file, self.pos), (other.file, other.pos))

#pylint: disable-msg=C0103
User = collections.namedtuple('User', ['name', 'passwd'])

class Server(object):
    """A representation of a MySQL server.

    A server object is used as a proxy for operating with the
    server. The basic primitives include connecting to the server and
    executing SQL statements and/or shell commands."""

    class Row(object):
        """A row (iterator) returned when executing an SQL statement.

        For statements that return a single row, the object can be
        treated as a row as well.

        """

        def __init__(self, cursor):
            self.__cursor = cursor
            self.__row = cursor.fetchone()

        def __iter__(self):
            return self

        def next(self):
            row = self.__row
            if row is None:
                raise StopIteration
            else:
                self.__row = self.__cursor.fetchone()
                return row
        
        def __getitem__(self, key):
            from mysql.replicant.errors import EmptyRowError
            if self.__row is not None:
                return self.__row[key]
            else:
                raise EmptyRowError

        def __str__(self):
            from mysql.replicant.errors import EmptyRowError
            if len(self.__row) == 1:
                return str(self.__row.values()[0])
            else:
                raise EmptyRowError
    
    def __init__(self, name, sql_user, ssh_user, machine,
                 config_manager=configmanager.ConfigManagerFile(),
                 role=roles.Vagabond(), 
                 server_id=None, host='localhost', port=3306,
                 socket='/tmp/mysqld.sock', defaults_file=None,
                 config_section='mysqld'):
        """Initialize the server object with data.

        If a configuration file path is provided, it will be used to
        read server options that were not provided. There are three
        mandatory options:

        sql_user
           This is a user for connecting to the server to execute SQL
           commands. This is a MySQL server user.

        ssh_user
           This is a user for connecting to the machine where the
           server is installed in order to perform tasks that cannot
           be done through the MySQL client interface.

        machine
           This is a machine object for performing basic operations on
           the server such as starting and stopping the server.

        The following additional keyword parameters are used:

        name
           This parameter is used to create names for the pid-file,
           log-bin, and log-bin-index options. If it is not provided,
           the name will be deduced from the pid-file, log-bin, or
           log-bin-index (in that order), or a default will be used.

        host
           The hostname of the server, which defaults to 'localhost',
           meaning that it will connect using the socket and not
           TCP/IP.

        socket
           Socket to use when connecting to the server. This parameter
           is only used if host is 'localhost'. It defaults to
           '/tmp/mysqld.sock'.

        port
           Port to use when connecting to the server when host is not
           'localhost'. It defaults to 3306.

        server_id
           Server ID to use. If none is assigned, the server ID is
           fetched from the configuration file. If the configuration
           files does not contain a server ID, no server ID is
           assigned.

        """

        if not defaults_file:
            defaults_file = machine.defaults_file

        self.name = name
        self.sql_user = sql_user
        self.ssh_user = ssh_user

        # These attributes are explicit right now, we have to
        # implement logic for fetching them from the configuration if
        # necessary.
        self.host = host
        self.port = port
        self.server_id = server_id
        self.socket = socket
        self.defaults_file = defaults_file
        
        self.config_section = config_section

        self.__machine = machine
        self.__config_manager = config_manager
        self.__conn = None
        self.__config = None
        self.__tmpfile = None
        self.__warnings = None

        self.__role = role
        self.imbue(role)
            
    def _connect(self, database=''):
        """Method to connect to the server, preparing for execution of
        SQL statements.  If a connection is already established, this
        function does nothing."""
        if not self.__conn:
            self.__conn = _connector.connect(
                host=self.host, port=self.port,
                unix_socket=self.socket,
                db=database,
                user=self.sql_user.name,
                passwd=self.sql_user.passwd)
        elif database:
            self.__conn.select_database(database)
                                      
    def imbue(self, role):
        """Imbue a server with a new role."""
        self.__role.unimbue(self)
        self.__role = role
        self.__role.imbue(self)
        
    def disconnect(self):
        """Method to disconnect from the server."""
        self.__conn = None
        return self
                                      
    def sql(self, command, args=None, database=''):
        """Execute a SQL command on the server.

        This first requires a connection to the server.

        The function will return an iteratable to the result of the
        execution with one row per iteration.  The function can be
        used in the following way::

           for database in server.sql("SHOW DATABASES")
              print database["Database"]

         """

        self._connect(database)
        cur = self.__conn.cursor(_connector.cursors.DictCursor)
        with warnings.catch_warnings(record=True) as warn:
            cur.execute(command, args)
            self.__warnings = warn
        return Server.Row(cur)

    def ssh(self, command):
        """Execute a shell command on the server.

        The function will return an iteratable (currently a list) to
        the result of the execution with one line of the output for
        each iteration.  The function can be used in the following
        way:

        for line in server.ssh(["ls"])
            print line

        For remote commands we do not allow X11 forwarding, and the
        stdin will be redirected to /dev/null.

        """

        from subprocess import Popen, PIPE, STDOUT

        if self.host == "localhost":
            cmd = ["sudo", "-u" + self.ssh_user.name] + command
            process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        else:
            fullname = self.ssh_user.name + "@" + self.host
            process = Popen(["ssh", "-fqTx", fullname, ' '.join(command)],
                            stdout=PIPE, stderr=STDOUT)
        output = process.communicate()[0]
        return output.split("\n")

    def fetch_config(self, path=None):
        return self.__config_manager.fetch_config(self, path)

    def replace_config(self, config, path=None):
        self.__config_manager.replace_config(self, config, path)
        return self

    def stop(self):
        self.__machine.stop_server(self)
        return self

    def start(self):
        self.__machine.start_server(self)
        return self

