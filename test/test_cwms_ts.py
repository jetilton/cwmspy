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
def cwms_loc(connection, name):

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
def cwms_data(name, cwms_loc, units, tz):
    cwms = cwms_loc
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


data_tests = [
    ("pm3", "cms", "UTC"),
    ("pm3", "cfs", "US/Pacific"),
    ("pt7", "cms", "UTC"),
    ("pt7", "cfs", "US/Pacific"),
]

loc_tests = [("pm3"), ("pt7")]


class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        # https://stackoverflow.com/a/50375022/4296857
        self._caplog = caplog

    @pytest.mark.parametrize(
        "name, units, tz", data_tests,
    )
    def test_successful_get_ts_code(self, name, units, tz, cwms_data):
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

    @pytest.mark.parametrize(
        "name, units, tz", data_tests,
    )
    def test_get_ts_max_date(self, name, units, tz, cwms_data):
        cwms, times, values, p_cwms_ts_id, units, tz = cwms_data
        max_date = cwms.get_ts_max_date(
            p_cwms_ts_id="CWMSPY.Flow.Inst.0.0.REV",
            p_time_zone=tz,
            version_date="1111/11/11",
            p_office_id=None,
        )

        assert max_date == datetime.strptime(times[-1], "%Y/%m/%d")

    @pytest.mark.parametrize(
        "name, units, tz", data_tests,
    )
    def test_get_ts_min_date(self, name, units, tz, cwms_data):
        cwms, times, values, p_cwms_ts_id, units, tz = cwms_data
        min_date = cwms.get_ts_min_date(
            p_cwms_ts_id, p_time_zone=tz, version_date="1111/11/11", p_office_id=None
        )
        assert min_date == datetime.strptime(times[0], "%Y/%m/%d")

    @pytest.mark.parametrize(
        "name, units, tz", data_tests,
    )
    def test_retrieve_time_series(self, name, units, tz, cwms_data):
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

    @pytest.mark.parametrize(
        "name, units, tz", data_tests,
    )
    def test_retrieve_ts(self, name, units, tz, cwms_data):
        cwms, times, values, p_cwms_ts_id, units, tz = cwms_data
        df = cwms.retrieve_ts(
            p_cwms_ts_id,
            p_units=units,
            start_time="2015-12-01",
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

    @pytest.mark.parametrize(
        "name", loc_tests,
    )
    def test_store_by_df(self, name, cwms_loc):
        cwms = cwms_loc
        df = pd.read_json("test/data/data.json")
        # units are not in the same order as above and I need them to get the data
        # again for the actual test
        firsts = df.groupby("ts_id").first()["units"]
        units = list(firsts.values)
        paths = list(firsts.index)
        df["date_time"] = pd.to_datetime(df["date_time"])
        p_start = df["date_time"].min().strftime(format="%Y-%m-%d")
        p_end = df["date_time"].max().strftime(format="%Y-%m-%d")
        cwms.store_by_df(df, timezone="UTC")
        df_list = []
        for p, u in zip(paths, units):
            d = cwms.retrieve_ts_out(
                p,
                p_units=u,
                start_time=p_start,
                end_time=p_end,
                p_timezone="UTC",
                p_office_id=None,
            )
            df_list.append(d)
        new_df = pd.concat(df_list)
        grp = new_df.groupby("ts_id")
        for g, v in grp:
            df[df["ts_id"] == g][["value"]].equals(v[["value"]])

    @pytest.mark.parametrize(
        "name", loc_tests,
    )
    def test_store_by_df_no_new_data(self, name, cwms_loc):
        cwms = cwms_loc
        df = pd.read_json("test/data/data.json")
        cwms.store_by_df(df)
        with self._caplog.at_level(logging.INFO):
            cwms.store_by_df(df)
            assert "No new data to load for" in self._caplog.records[-1].message

    @pytest.mark.parametrize(
        "name", loc_tests,
    )
    def test_store_by_df_no_new_data_diff_timezone(self, name, cwms_loc):
        cwms = cwms_loc
        df = pd.read_json("test/data/data.json")
        cwms.store_by_df(df, timezone="UTC")
        df.set_index("date_time", inplace=True)
        df.index = df.index.tz_localize(tz="UTC")
        df.index = df.index.tz_convert(tz="US/Pacific")
        df.reset_index(inplace=True, drop=False)
        df["time_zone"] = "US/Pacific"
        with self._caplog.at_level(logging.INFO):
            cwms.store_by_df(df)
            assert "No new data to load for" in self._caplog.records[-1].message

    @pytest.mark.parametrize(
        "name", loc_tests,
    )
    def test_store_by_df_protected_data(self, name, cwms_loc):
        cwms = cwms_loc
        # loading the protected data
        df = pd.read_json("test/data/data.json")
        grp = df.groupby("ts_id")
        df = grp.get_group(df["ts_id"][0]).reset_index(drop=True)
        df["quality_code"] = 2 ** 31 + 5
        cwms.store_by_df(df)

        # changing the data, making it non protected then loading
        # again
        new_df = df.copy()
        new_df["value"] = new_df["value"] + 5
        new_df["quality_code"] = 0
        cwms.store_by_df(new_df)

        # retrieving the data from the database to make sure it
        # is still the original protected data
        ts_id = df["ts_id"][0]
        units = df["units"][0]
        tz = df["time_zone"][0]
        start_time = df[["date_time"]].min()[0]
        end_time = df[["date_time"]].max()[0]
        retrieved_df = cwms.retrieve_ts(
            ts_id,
            p_units=units,
            start_time=start_time,
            end_time=end_time,
            p_timezone=tz,
            p_office_id=None,
        )

        assert (
            retrieved_df.sort_values("date_time")[["value"]]
            .dropna()
            .reset_index(drop=True)
            .equals(
                df.sort_values("date_time")[["value"]].dropna().reset_index(drop=True)
            )
        )

    @pytest.mark.parametrize(
        "name", loc_tests,
    )
    def test_delete_by_df(self, name, cwms_loc):
        cwms = cwms_loc

        df = pd.read_json("test/data/data.json")
        df = df.head(n=100).copy()
        cwms.store_by_df(df)
        ts_id = df["ts_id"].values[0]
        start = df["date_time"].min()
        end = df["date_time"].max()
        units = df["units"].values[0]
        cwms.delete_by_df(df.iloc[[1, 5, 10], :])
        dropped_df = df.drop([1, 5, 10]).copy().reset_index(drop=True).dropna()

        retrieved_df = cwms.retrieve_ts(
            ts_id, p_units=units, start_time=start, end_time=end, p_office_id=None,
        )
        assert dropped_df.dropna()[["value"]].equals(
            retrieved_df[["value"]].dropna().reset_index(drop=True)
        )

    @pytest.mark.parametrize(
        "name", loc_tests,
    )
    def test_delete_by_df_1500_values(self, name, cwms_loc):
        cwms = cwms_loc

        df = pd.read_json("test/data/data.json")
        df = df.head(n=2500).copy()
        cwms.store_by_df(df)
        ts_id = df["ts_id"].values[0]
        start = df["date_time"].min()
        end = df["date_time"].max()
        units = df["units"].values[0]
        cwms.delete_by_df(df.iloc[:1500, :])
        dropped_df = df.iloc[1500:, :].copy().reset_index(drop=True).dropna()

        retrieved_df = cwms.retrieve_ts(
            ts_id, p_units=units, start_time=start, end_time=end, p_office_id=None,
        )
        assert dropped_df.dropna()[["value"]].equals(
            retrieved_df[["value"]].dropna().reset_index(drop=True)
        )

