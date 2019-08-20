# -*- coding: utf-8 -*-

import sys
import cx_Oracle
from cwmspy.cwms_ts import CWMS_TS
import cwmspy._cwms_loc as _cwms_loc
import cwmspy._extra as _extra
from dotenv import load_dotenv
import os
from cwmspy.connect import Connect


class CWMS:
    def __init__(self, conn):
        self.cwms_ts = CWMS_TS(conn)
    

    

