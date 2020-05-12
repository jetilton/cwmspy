# -*- coding: utf-8 -*-

import sys
import cx_Oracle
import os
from os.path import join, dirname
import logging

from .cwms_ts import CwmsTsMixin
from .cwms_loc import CwmsLocMixin
from .cwms_level import CwmsLevelMixin
from .utils import log_decorator


LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)
FORMAT = "%(levelname)s - %(asctime)s - %(name)s - %(message)s"


class CWMS(CwmsLocMixin, CwmsTsMixin, CwmsLevelMixin):
    def __init__(self, conn=None, verbose=False):
        self.conn = conn
        if verbose:
            logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format=FORMAT)
        else:
            logging.basicConfig(stream=sys.stderr, level=logging.ERROR, format=FORMAT)

    @LD
    def connect(
        self,
        host=None,
        service_name=None,
        port=1521,
        user=None,
        password=None,
        dsn=None,
    ):
        """Make connection to Oracle CWMS database. Oracle connections are
            expensive, so it is best to have a class connection for all methods.
            There are 4 ways to create a connection to the database.  
            Creating CWMSPY_USER, CWMSPY_PASSWORD, CWMSPY_HOST, CWMSPY_SERVICE_NAME 
            environment variables is the most convenient for fast easy connection.  
            You can also pass `user`, `password`, `host`, and `service_name` as arguments 
            to the connect method.  You can establish a connection to a database and 
            pass that connection when you instantiate.  Finally, you can use a dsn string 
            if you have a tnsnames.ora file.

        Parameters
        ----------
        host : (str):
            Host to connect to.
        service_name : str
            SID alias.
        port : int
            Oracle SQL*Net Listener port (the default is 1521).
        user : str
            DB username.
        password : str
            User password.


        Returns
        -------
        bool
            True for success, False otherwise.

        Examples
        -------
        ```python
        # using .env
        import CWMS
        cwms = CWMS()
        cwms.connect()
        `True`

        # passing connection  
        import CWMS
        import cx_Oracle
        ...
        conn = cx_Oracle.connect(**conn_dict)
        cwms = CWMS(conn=conn)
        cwms.connect()
        `True`

        # passing args
        import CWMS
        cwms = CWMS()
        cwms.connect(host='host',service_name='service_name',user='user',password='password')
        `True`

        # passing dns
        import CWMS
        cwms = CWMS()
        cwms.connect(dsn='dns_string', user='user',password='password')
        `True`

        ```

        """
        if not dsn:
            dsn_dict = {}
            if host:
                dsn_dict.update({"host": host})
            elif os.getenv("CWMSPY_HOST"):
                host = os.getenv("CWMSPY_HOST")
                dsn_dict.update({"host": host})
            else:
                msg = "Missing host"
                LOGGER.error(msg)
                raise ValueError("Missing host")
            LOGGER.info(f"Host: {host}")
            if service_name:
                dsn_dict.update({"service_name": service_name})
            elif os.getenv("CWMSPY_SERVICE_NAME"):
                service_name = os.getenv("CWMSPY_SERVICE_NAME")
                dsn_dict.update({"service_name": os.getenv("CWMSPY_SERVICE_NAME")})
            else:
                msg = "Missing service_name"
                LOGGER.error(msg)
                raise ValueError(msg)
            LOGGER.info(f"service_name: {service_name}")
            if port:
                dsn_dict.update({"port": port})
            elif os.getenv("CWMSPY_PORT"):
                dsn_dict.update({"port": os.getenv("CWMSPY_PORT")})
            else:
                msg = "Missing port"
                LOGGER.error(msg)
                raise ValueError(msg)
            LOGGER.info(f"port: {port}")
            self.host = dsn_dict["host"]
            dsn = cx_Oracle.makedsn(**dsn_dict)

        conn_dict = {"dsn": dsn}

        if user:
            conn_dict.update({"user": user})
        elif os.getenv("CWMSPY_USER"):
            conn_dict.update({"user": os.getenv("CWMSPY_USER")})
        if password:
            conn_dict.update({"password": password})
        elif os.getenv("CWMSPY_PASSWORD"):
            conn_dict.update({"password": os.getenv("CWMSPY_PASSWORD")})

        # close any current open connection to minimize # of connections to DB
        if self.conn:
            self.close()

        try:
            self.conn = cx_Oracle.connect(**conn_dict)
            msg = "Connected to {host}".format(**dsn_dict)
            LOGGER.info(msg)
            return True
        except Exception as e:
            msg = "Failed to connect to {host}".format(**dsn_dict)
            LOGGER.error(msg)
            LOGGER.error(e)
            return False

    @LD
    def close(self):
        """Close self.conn

        Args:
            self

        Returns:
            bool: The return value. True for success, False otherwise.

        """

        if not self.conn:
            return False
        host = self.host
        try:
            self.conn.close()
            LOGGER.info(f"Disconnected from {host}.")
        except Exception as e:
            LOGGER.error(f"Error disconnecting from {host}")
            LOGGER.error(e)
        return True
