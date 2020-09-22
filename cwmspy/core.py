# -*- coding: utf-8 -*-

import sys
import cx_Oracle
import os
from os.path import join, dirname
import logging
from shutil import copyfile

import yaml

from .cwms_ts import CwmsTsMixin
from .cwms_loc import CwmsLocMixin
from .cwms_level import CwmsLevelMixin
from .cwms_sec import CwmsSecMixin
from .utils import log_decorator


LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)
FORMAT = "%(levelname)s - %(asctime)s - %(name)s - %(message)s"


class CWMS(CwmsLocMixin, CwmsTsMixin, CwmsLevelMixin, CwmsSecMixin):
    def __init__(self, conn=None, verbose=False):
        self.conn = conn
        if verbose:
            logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format=FORMAT)
        else:
            logging.basicConfig(stream=sys.stderr, level=logging.ERROR, format=FORMAT)

    @LD
    def connect(
        self,
        name=None,
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
        if name:
            with open(".env", "r") as stream:
                try:
                    config = yaml.safe_load(stream)
                except yaml.YAMLError as e:
                    LOGGER.error("Error loading config")
                    raise (e)

            config = [d for d in config if d["name"] == name][0]
            del config["name"]
        else:
            config = None

        if not dsn:
            dsn_dict = {}
            if host:
                dsn_dict.update({"host": host})
            elif config:
                host = config["host"]
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
            elif config:
                service_name = config["service_name"]
                dsn_dict.update({"service_name": service_name})
            elif os.getenv("CWMSPY_SERVICE_NAME"):
                service_name = os.getenv("CWMSPY_SERVICE_NAME")
                dsn_dict.update({"service_name": service_name})
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
        elif config:
            conn_dict.update({"user": config["user"]})
        elif os.getenv("CWMSPY_USER"):
            conn_dict.update({"user": os.getenv("CWMSPY_USER")})
        if password:
            conn_dict.update({"password": password})
        elif config:
            conn_dict.update({"password": config["password"]})
        elif os.getenv("CWMSPY_PASSWORD"):
            conn_dict.update({"password": os.getenv("CWMSPY_PASSWORD")})

        # close any current open connection to minimize # of connections to DB
        if self.is_open:
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
        host = self.host
        if self.is_closed():
            LOGGER.info(f"Already disconnectd from {host}.")
            return True
        if not self.conn:
            return False
        try:
            self.conn.close()
            LOGGER.info(f"Disconnected from {host}.")
        except Exception as e:
            LOGGER.error(f"Error disconnecting from {host}")
            LOGGER.error(e)
        return True

    @LD
    def is_open(self):
        try:
            return self.conn.ping() is None
        except:
            return False

    @LD
    def is_closed(self):
        try:
            return self.conn.ping() is not None
        except:
            return True

    @staticmethod
    def add_env(filename):
        path = os.path.split(os.path.abspath(__file__))
        path = [x for x in path[:-1]] + [".env"]
        dst = os.path.join(*path)
        copyfile(filename, dst)
