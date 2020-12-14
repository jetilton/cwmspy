# -*- coding: utf-8 -*-
import os
from datetime import datetime
import math
import logging

import pandas as pd
import pytest
import numpy as np

from cwmspy import CWMS


@pytest.fixture(scope="function")
def connection(name):
    cwms = CWMS(verbose=True)
    cwms.connect(name=name)
    yield cwms
    cwms.close()


@pytest.fixture(scope="function")
def loc(connection, name):

    try:
        connection.delete_location("CWMSPY", "DELETE TS DATA")
        connection.delete_location("CWMSPY", "DELETE TS ID")
        connection.delete_location("CWMSPY")
    except:
        pass
    alter_session_sql = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
    cur = connection.conn.cursor()
    cur.execute(alter_session_sql)
    cur.close()
    connection.store_location("CWMSPY")
    yield connection
    # test
    try:
        connection.delete_location("CWMSPY", "DELETE TS DATA")
        connection.delete_location("CWMSPY", "DELETE TS ID")
        connection.delete_location("CWMSPY")
    except:
        pass


@pytest.fixture(scope="function")
def test_data(name, loc, units, tz):
    cwms = loc
    p_cwms_ts_id = "CWMSPY.Flow.Inst.0.0.REV"
    p_units = units
    times = [
        x.strftime("%Y/%m/%d")
        for x in pd.date_range(datetime(2016, 12, 31), periods=400)
    ]
    values = [math.sin(x) for x in range(len(times))]

    try:
        cwms.store_ts(
            p_cwms_ts_id=p_cwms_ts_id,
            p_units=p_units,
            times=times,
            values=values,
            qualities=None,
            p_override_prot="T",
            timezone=tz,
        )
    except Exception as e:
        raise e
    yield cwms, times, values, p_cwms_ts_id, units, tz


class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        # https://stackoverflow.com/a/50375022/4296857
        self._caplog = caplog

    @pytest.mark.parametrize(
        "name, units, tz",
        [
            ("pm3", "cms", "UTC"),
            ("pm3", "cfs", "US/Pacific"),
            ("pt7", "cms", "UTC"),
            ("pt7", "cfs", "US/Pacific"),
        ],
    )
    def test_successful_get_ts_code(self, name, units, tz, test_data):
        """
        get_ts_code: Testing successful ts code
        """

        cwms, times, values, p_cwms_ts_id, units, tz = test_data
        ts_code_sql = f"""select ts_code 
                        from cwms_20.at_cwms_ts_id
                        where cwms_ts_id = '{p_cwms_ts_id}'"""
        cur = cwms.conn.cursor()
        cur.execute(ts_code_sql)
        ts_code = cur.fetchall()[0][0]
        cur.close()
        cwms_ts_code = cwms.get_ts_code(p_cwms_ts_id)

        assert cwms_ts_code == str(ts_code)
