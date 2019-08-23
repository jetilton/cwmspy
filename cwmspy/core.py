# -*- coding: utf-8 -*-

import sys
import cx_Oracle
from dotenv import load_dotenv
import os
from .cwms_ts import CwmsTsMixin
from .cwms_loc import CwmsLocMixin
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

class CWMS(CwmsLocMixin, CwmsTsMixin):
    def __init__(self, conn=None):
        self.conn = conn

    def connect(
        self,
        host=None,
        service_name=None,
        port=1521,
        user=None,
        password=None,

    ):
        """Make connection to Oracle CWMS database. Oracle connections are
            expensive, so it is best to have a class connection for all methods.

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
        import CWMS
        cwms = CWMS()
        cwms.connect()
        `True`
        ```

        """


        dsn_dict = {}
        if host:
            dsn_dict.update({"host": host})
        elif os.getenv("HOST"):
            dsn_dict.update({"host": os.getenv("HOST")})
        else:
            raise ValueError("Missing host")
        if service_name:
            dsn_dict.update({"service_name": service_name})
        elif os.getenv("SERVICE_NAME"):
            dsn_dict.update({"service_name": os.getenv("SERVICE_NAME")})
        else:
            raise ValueError("Missing service_name")
        if port:
            dsn_dict.update({"port": port})
        elif os.getenv("PORT"):
            dsn_dict.update({"port": os.getenv("PORT")})
        else:
            raise ValueError("Missing port")

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
            return True
        except Exception as e:
            sys.stderr.write(e.__str__())
            return False

    def close(self):
        """Close self.conn

        Args:
            self

        Returns:
            bool: The return value. True for success, False otherwise.

        """

        if not self.conn:
            return False
        self.conn.close()
        return True
