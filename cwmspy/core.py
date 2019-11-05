# -*- coding: utf-8 -*-

import sys
import cx_Oracle
from dotenv import load_dotenv
import os
from os.path import join, dirname
import logging

from .cwms_ts import CwmsTsMixin
from .cwms_loc import CwmsLocMixin
from .cwms_level import CwmsLevelMixin
from .utils import log_decorator


logger = logging.getLogger(__name__)
ld = log_decorator(logger)
format = "%(levelname)s - %(asctime)s - %(name)s - %(message)s"

dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path)


class CWMS(CwmsLocMixin, CwmsTsMixin, CwmsLevelMixin):
    def __init__(self, conn=None, verbose=True):
        self.conn = conn
        if verbose:
            logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=format)
        else:
            logging.basicConfig(stream=sys.stderr, level=logging.ERROR, format=format)

    @ld
    def connect(
        self, host=None, service_name=None, port=1521, user=None, password=None
    ):
        """Make connection to Oracle CWMS database. Oracle connections are
            expensive, so it is best to have a class connection for all methods.
            There are 3 ways to create a connection to the database.  
            Creating a .env file with USER, PASSWORD, HOST, SERVICE_NAME variables
            is the most convenient for fast easy connection.  You can also pass these
            as arguments to the connect method.  Finally you can establish a connection
            to a database and pass that connection when you instantiate.

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

        ```

        """

        dsn_dict = {}
        if host:
            dsn_dict.update({"host": host})
        elif os.getenv("HOST"):
            dsn_dict.update({"host": os.getenv("HOST")})
        else:
            msg = "Missing host"
            logger.error(msg)
            raise ValueError("Missing host")
        if service_name:
            dsn_dict.update({"service_name": service_name})
        elif os.getenv("SERVICE_NAME"):
            dsn_dict.update({"service_name": os.getenv("SERVICE_NAME")})
        else:
            msg = "Missing service_name"
            logger.error(msg)
            raise ValueError(msg)
        if port:
            dsn_dict.update({"port": port})
        elif os.getenv("PORT"):
            dsn_dict.update({"port": os.getenv("PORT")})
        else:
            msg = "Missing port"
            logger.error(msg)
            raise ValueError(msg)
        self.host = dsn_dict["host"]
        dsn = cx_Oracle.makedsn(**dsn_dict)

        conn_dict = {"dsn": dsn}

        if user:
            conn_dict.update({"user": user})
        elif os.getenv("USER"):
            conn_dict.update({"user": os.getenv("USER")})
        else:
            conn_dict.update({"user": "cwmsview"})
        if password:
            conn_dict.update({"password": password})
        elif os.getenv("PASSWORD"):
            conn_dict.update({"password": os.getenv("PASSWORD")})
        else:
            conn_dict.update({"password": "cwmsview"})

        # close any current open connection to minimize # of connections to DB
        if self.conn:
            self.close()

        try:
            self.conn = cx_Oracle.connect(**conn_dict)
            msg = "Connected to {host}".format(**dsn_dict)
            logger.info(msg)
            return True
        except Exception as e:
            msg = "Failed to connect to {host}".format(**dsn_dict)
            logger.error(msg)
            logger.error(e)
            return False

    @ld
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
            logger.info(f"Disconnected from {host}.")
        except Exception as e:
            logger.error(f"Error disconnecting from {host}")
            logger.error(e)
        return True
