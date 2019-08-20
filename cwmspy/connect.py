import sys
import cx_Oracle
import cwmspy.cwms_ts as cwms_ts
import cwmspy._cwms_loc as _cwms_loc
import cwmspy._extra as _extra
from dotenv import load_dotenv
import os

class Connect:
    def __init__(self, conn=None):
        self.conn = conn
    def connect(
        self,
        host=None,
        service_name=None,
        port=1521,
        user=None,
        password=None,
        threaded=True,
    ):
        """Make connection to Oracle CWMS database. Oracle connections are 
            expensive, so it is best to have a class connection for all methods
    
    
        Args:
            host (str): host to connect to
            service_name: SID alias
            port: Oracle SQL*Net Listener port
            username (str): DB username.
            password (str): DB password.
    
        Returns:
            bool: The return value. True for success, False otherwise.
    
        """
        load_dotenv()
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
            return self.conn
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