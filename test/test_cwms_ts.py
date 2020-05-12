# -*- coding: utf-8 -*-
import os
from datetime import datetime
import math

import pandas as pd
import pytest
import numpy as np

from cwmspy import CWMS


@pytest.fixture(params=[["cms", "UTC"], ["cfs", "US/Pacific"]])
def cwms_data(request):

    cwms = CWMS(verbose=True)
    cwms.connect()
    units, tz = ["cms", "UTC"]
    units, tz = request.param

    alter_session_sql = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
    cur = cwms.conn.cursor()
    cur.execute(alter_session_sql)
    cur.close()

    try:
        cwms.delete_location("CWMSPY", "DELETE TS DATA")
        cwms.delete_location("CWMSPY", "DELETE TS ID")
        cwms.delete_location("CWMSPY")
    except:
        pass
    cwms.store_location("CWMSPY")
    p_cwms_ts_id = "CWMSPY.Flow.Inst.0.0.REV"
    p_units = units
    times = (
        pd.date_range(datetime(2016, 12, 31), periods=400).strftime("%Y/%m/%d").tolist()
    )
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
        try:
            cwms.delete_location("CWMSPY", "DELETE TS DATA")
            cwms.delete_location("CWMSPY", "DELETE TS ID")
            cwms.delete_location("CWMSPY")
        except:
            pass
        c = cwms.close()
        raise e
    yield cwms, times, values, p_cwms_ts_id, units, tz
    cwms.delete_location("CWMSPY", "DELETE TS DATA")
    cwms.delete_location("CWMSPY", "DELETE TS ID")
    cwms.delete_location("CWMSPY")
    c = cwms.close()


@pytest.fixture()
def cwms():

    cwms = CWMS()
    cwms.connect()

    alter_session_sql = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
    cur = cwms.conn.cursor()
    cur.execute(alter_session_sql)
    cur.close()
    cwms.store_location("CWMSPY")
    yield cwms
    try:
        cwms.delete_location("CWMSPY", "DELETE TS DATA")
        cwms.delete_location("CWMSPY", "DELETE TS ID")
        cwms.delete_location("CWMSPY")
    except:
        pass
    c = cwms.close()


class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        # https://stackoverflow.com/a/50375022/4296857
        self._caplog = caplog

    def test_successful_get_ts_code(self, cwms_data):
        """
        get_ts_code: Testing successful ts code
        """
        cwms, times, values, p_cwms_ts_id, units, tz = cwms_data
        ts_code_sql = f"""select ts_code 
                        from cwms_20.at_cwms_ts_id
                        where cwms_ts_id = '{p_cwms_ts_id}'"""
        cur = cwms.conn.cursor()
        cur.execute(ts_code_sql)
        ts_code = cur.fetchall()[0][0]
        cur.close()
        cwms_ts_code = cwms.get_ts_code(p_cwms_ts_id)

        assert cwms_ts_code == str(ts_code)

    def test_get_ts_max_date(self, cwms_data):
        cwms, times, values, p_cwms_ts_id, units, tz = cwms_data
        max_date = cwms.get_ts_max_date(
            p_cwms_ts_id="CWMSPY.Flow.Inst.0.0.REV",
            p_time_zone=tz,
            version_date="1111/11/11",
            p_office_id=None,
        )

        assert max_date == datetime.strptime(times[-1], "%Y/%m/%d")

    def test_get_ts_min_date(self, cwms_data):
        cwms, times, values, p_cwms_ts_id, units, tz = cwms_data
        min_date = cwms.get_ts_min_date(
            p_cwms_ts_id, p_time_zone=tz, version_date="1111/11/11", p_office_id=None
        )
        assert min_date == datetime.strptime(times[0], "%Y/%m/%d")

    def test_retrieve_time_series(self, cwms_data):
        cwms, times, values, p_cwms_ts_id, units, tz = cwms_data
        df = cwms.retrieve_time_series(
            [p_cwms_ts_id],
            units=[units],
            p_datums=None,
            p_start="2015-12-01",
            p_end="2020-01-02",
            p_timezone=tz,
            p_office_id=None,
            as_json=False,
        )
        # assert times == list(df["date_time"].values)
        # assert len(list(df["value"].values)) == len(values)
        # assert list(df["value"].values) == values
        assert [x.strftime("%Y/%m/%d") for x in df["date_time"]] == times
        assert [np.round(float(x)) for x in list(df["value"].values)] == [
            np.round(float(x)) for x in values
        ]

    def test_retrieve_ts(self, cwms_data):
        cwms, times, values, p_cwms_ts_id, units, tz = cwms_data
        df = cwms.retrieve_ts(
            p_cwms_ts_id,
            p_units=units,
            start_time="2015/12/01",
            end_time="2020/01/02",
            p_timezone=tz,
            p_office_id=None,
        )
        # assert times == list(df["date_time"].values)
        # assert len(list(df["value"].values)) == len(values)
        # assert list(df["value"].values) == values
        assert [x.strftime("%Y/%m/%d") for x in df["date_time"]] == times
        assert [np.round(float(x)) for x in list(df["value"].values)] == [
            np.round(float(x)) for x in values
        ]

    def test_store_by_df(self, cwms):

        sql = """
                SELECT * FROM
                (SELECT cwms_ts_id,ts_code, unit_id
                FROM cwms_20.at_cwms_ts_id
                where rownum <501
                ORDER BY dbms_random.value
                )
                where rownum < 5
                """

        df = pd.DataFrame()

        while df.empty:

            cur = cwms.conn.cursor()
            cur.execute(sql)
            paths = cur.fetchall()
            cur.close()
            p_cwms_ts_id = [path[0] for path in paths]
            units = [path[-1] for path in paths]
            p_start = "2019-12-01"
            p_end = "2020-01-02"

            df = cwms.retrieve_time_series(
                p_cwms_ts_id,
                units=units,
                p_start=p_start,
                p_end=p_end,
                p_timezone="UTC",
                p_office_id=None,
            )

        df_cwms = df.copy()

        def switch_path(path):
            split_path = path.split(".")
            split_path[0] = "CWMSPY"
            return ".".join(split_path)

        df_cwms["ts_id"] = df_cwms.apply(lambda row: switch_path(row["ts_id"]), axis=1)
        # units are not in the same order as above and I need them to get the data
        # again for the actual test
        firsts = df_cwms.groupby("ts_id").first()["units"]
        units = list(firsts.values)
        paths = list(firsts.index)
        cwms.store_by_df(df_cwms, timezone="UTC")

        new_df = cwms.retrieve_time_series(
            paths,
            units=units,
            p_start=p_start,
            p_end=p_end,
            p_timezone="UTC",
            p_office_id=None,
        )

        grp = new_df.groupby("ts_id")

        for g, v in grp:
            df_cwms[df_cwms["ts_id"] == g].equals(v)
