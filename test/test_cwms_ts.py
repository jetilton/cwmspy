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

    cwms = CWMS()
    cwms.connect()

    units, tz = request.param

    alter_session_sql = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
    cur = cwms.conn.cursor()
    cur.execute(alter_session_sql)
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
        cwms.delete_ts(p_cwms_ts_id, "DELETE TS DATA")
        cwms.delete_ts(p_cwms_ts_id, "DELETE TS ID")
        cwms.delete_location("CWMSPY")
        c = cwms.close()
        raise e
    yield cwms, times, values, p_cwms_ts_id
    cwms.delete_ts(p_cwms_ts_id, "DELETE TS DATA")
    cwms.delete_ts(p_cwms_ts_id, "DELETE TS ID")
    cwms.delete_location("CWMSPY")
    c = cwms.close()


@pytest.fixture()
def cwms():

    cwms = CWMS()
    cwms.connect()

    alter_session_sql = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
    cur = cwms.conn.cursor()
    cur.execute(alter_session_sql)

    yield cwms
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
        cwms, times, values, p_cwms_ts_id = cwms_data
        ts_code_sql = f"""select ts_code 
                        from cwms_20.at_cwms_ts_id
                        where cwms_ts_id = '{p_cwms_ts_id}'"""
        cur = cwms.conn.cursor()
        cur.execute(ts_code_sql)
        ts_code = cur.fetchall()[0][0]

        cwms_ts_code = cwms.get_ts_code(p_cwms_ts_id)

        assert cwms_ts_code == str(ts_code)

    @pytest.mark.parametrize("tz", [("UTC"), ("US/Pacific")])
    def test_get_ts_max_date(self, cwms_data, tz):
        cwms, times, values, p_cwms_ts_id = cwms_data
        max_date = cwms.get_ts_max_date(
            p_cwms_ts_id="CWMSPY.Flow.Inst.0.0.REV",
            p_time_zone=tz,
            version_date="1111/11/11",
            p_office_id=None,
        )

        assert max_date == datetime.strptime(times[-1], "%Y/%m/%d")

    @pytest.mark.parametrize("tz", [("UTC"), ("US/Pacific")])
    def test_get_ts_min_date(self, cwms_data, tz):
        cwms, times, values, p_cwms_ts_id = cwms_data
        min_date = cwms.get_ts_min_date(
            p_cwms_ts_id, p_time_zone=tz, version_date="1111/11/11", p_office_id=None
        )
        assert min_date == datetime.strptime(times[0], "%Y/%m/%d")

    def test_retrieve_time_series(self, cwms_data):
        cwms, times, values, p_cwms_ts_id = cwms_data
        df = cwms.retrieve_time_series(
            [p_cwms_ts_id],
            units=["cms"],
            p_datums=None,
            p_start="2015-12-01",
            p_end="2020-01-02",
            p_timezone="UTC",
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

    # def test_three(self):
    #     """
    #     get_ts_code: Testing unsuccessful ts code bc of bad p_cwms_ts_id
    #     """
    #     try:
    #         self.cwms.get_ts_code("Not a code")
    #     except ValueError as e:
    #         msg = 'ORA-20001: TS_ID_NOT_FOUND: The timeseries identifier "Not a code"'
    #         assert msg in e.__repr__()

    # def test_four(self):
    #     """
    #     get_ts_code: Testing unsuccessful ts code bc of bad p_db_office_code
    #     """

    #     try:
    #         self.cwms.get_ts_code("ALF.Elev-Forebay.Ave.~1Day.1Day.CBT-RAW", 35)
    #     except ValueError as e:
    #         msg = 'ORA-20001: TS_ID_NOT_FOUND: The timeseries identifier "ALF.Elev-Forebay.Ave.~1Day.1Day.CBT-RAW" was not found for office "POH"'
    #         assert msg in e.__repr__()

    # def test_five(self):
    #     """
    #     retrieve_ts: Testing successful retrieve_ts
    #     """

    #     l = self.cwms.retrieve_ts(
    #         "TDA.Flow-Spill.Ave.1Hour.1Hour.CBT-RAW",
    #         "2019/1/1",
    #         "2019/9/1",
    #         return_df=False,
    #     )
    #     assert isinstance(l, list)
    #     assert np.floor(l[1600][1] + 1) == 40

    #     df = self.cwms.retrieve_ts(
    #         "TDA.Flow-Spill.Ave.1Hour.1Hour.CBT-RAW", "2019/1/1", "2019/9/1"
    #     )
    #     assert isinstance(df, pd.core.frame.DataFrame)
    #     assert np.floor(df["value"].values[1600] + 1) == 40

    # def test_six(self):
    #     """
    #     retrieve_ts: Testing unsuccessful retrieve_ts
    #     """

    #     try:
    #         self.cwms.retrieve_ts(
    #             "this is not valid", "2019/1/1", "2019/9/1", "cms", return_df=False
    #         )
    #     except Exception as e:
    #         msg = 'ORA-06502: PL/SQL: numeric or value error\nORA-06512: at "CWMS_20.CWMS_TS"'
    #         assert msg in e.__str__()

    # def test_seven(self):
    #     """
    #     retrieve_ts: Testing convert to local_tz
    #     the LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV pathname inputs data at 12:00
    #     local time, so it should only have 1 hour when on local, but 2 hours
    #     when utc
    #     """

    #     df = self.cwms.retrieve_ts(
    #         "LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #         local_tz=True,
    #     )

    #     assert len(set([x.hour for x in df["date_time"]])) == 1

    #     df = self.cwms.retrieve_ts(
    #         "LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #         local_tz=False,
    #     )

    #     assert len(set([x.hour for x in df["date_time"]])) == 2

    # def test_eight(self):
    #     """
    #     store_ts: Testing store_ts and delete_ts
    #     """

    #     df = self.cwms.retrieve_ts(
    #         "LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )
    #     if not self.cwms.retrieve_location("TST"):
    #         self.cwms.store_location("TST")
    #     p_cwms_ts_id = "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"
    #     p_units = "cms"
    #     values = list(df["value"])
    #     p_qualities = list(df["quality_code"])
    #     times = list(df["date_time"])

    #     self.cwms.store_ts(
    #         p_cwms_ts_id, p_units, times, values, p_qualities, p_override_prot="T"
    #     )
    #     df2 = self.cwms.retrieve_ts(
    #         "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )

    #     assert df.equals(df2)

    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS DATA")
    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS ID")

    #     try:
    #         df2 = self.cwms.retrieve_ts(
    #             "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #             "2019/1/1",
    #             "2019/9/1",
    #             "cms",
    #             return_df=True,
    #         )
    #     except ValueError as e:
    #         msg = 'TS_ID_NOT_FOUND: The timeseries identifier "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"'
    #         assert msg in e.__str__()

    #     # testing store_ts with times as string and qualities = None
    #     times = [x.strftime("%Y-%m-%d %H:%M:%S") for x in times]

    #     self.cwms.store_ts(p_cwms_ts_id, p_units, times, values, qualities=None)
    #     df2 = self.cwms.retrieve_ts(
    #         "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )

    #     assert df.equals(df2)

    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS DATA")
    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS ID")

    #     try:
    #         df2 = self.cwms.retrieve_ts(
    #             "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #             "2019/1/1",
    #             "2019/9/1",
    #             "cms",
    #             return_df=True,
    #         )
    #     except ValueError as e:
    #         msg = 'TS_ID_NOT_FOUND: The timeseries identifier "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"'
    #         assert msg in e.__str__()

    #     # testing store_ts with times as datetime.datetime and qualities = None
    #     times = [datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S") for x in times]

    #     self.cwms.store_ts(p_cwms_ts_id, p_units, times, values, qualities=None)
    #     df2 = self.cwms.retrieve_ts(
    #         "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )

    #     assert df.equals(df2)

    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS DATA")
    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS ID")
    #     self.cwms.delete_location("TST")
    #     try:
    #         df2 = self.cwms.retrieve_ts(
    #             "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #             "2019/1/1",
    #             "2019/9/1",
    #             "cms",
    #             return_df=True,
    #         )
    #     except ValueError as e:
    #         msg = 'TS_ID_NOT_FOUND: The timeseries identifier "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"'
    #         assert msg in e.__str__()

    # def test_nine(self):
    #     """
    #     store_ts: Testing rename_ts
    #     """

    #     df = self.cwms.retrieve_ts(
    #         "LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )
    #     if not self.cwms.retrieve_location("TST"):
    #         self.cwms.store_location("TST")
    #     p_cwms_ts_id_old = "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"
    #     p_cwms_ts_id_new = "TST.Flow-In.Ave.~1Day.1Day.CBT-REV"
    #     p_units = "cms"
    #     values = list(df["value"])
    #     p_qualities = list(df["quality_code"])
    #     times = list(df["date_time"])

    #     self.cwms.store_ts(p_cwms_ts_id_old, p_units, times, values, p_qualities)
    #     df2 = self.cwms.retrieve_ts(
    #         p_cwms_ts_id_old, "2019/1/1", "2019/9/1", "cms", return_df=True
    #     )

    #     assert df.equals(df2)
    #     self.cwms.rename_ts(
    #         p_cwms_ts_id_old, p_cwms_ts_id_new, p_utc_offset_new=None, p_office_id=None
    #     )

    #     df3 = self.cwms.retrieve_ts(
    #         p_cwms_ts_id_new, "2019/1/1", "2019/9/1", "cms", return_df=True
    #     )

    #     assert df2.equals(df3)

    #     self.cwms.delete_ts(p_cwms_ts_id_new, "DELETE TS DATA")
    #     self.cwms.delete_ts(p_cwms_ts_id_new, "DELETE TS ID")
    #     self.cwms.delete_location("TST")

    # def test_ten(self):
    #     """
    #     get_max_date/get_min_date: Testing for successful min/max dates
    #     """

    #     min_date = self.cwms.get_ts_min_date("LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV")
    #     max_date = self.cwms.get_ts_max_date("LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV")

    #     assert isinstance(min_date, datetime.datetime)
    #     assert isinstance(max_date, datetime.datetime)

    # def test_eleven(self):
    #     """
    #     delete_by_df: Testing for successful delete_by_df
    #     """

    #     df = self.cwms.retrieve_ts(
    #         "LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )
    #     if not self.cwms.retrieve_location("TST"):
    #         self.cwms.store_location("TST")
    #     p_cwms_ts_id = "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"
    #     p_units = "cms"
    #     values = list(df["value"])
    #     p_qualities = list(df["quality_code"])
    #     times = list(df["date_time"])

    #     self.cwms.store_ts(p_cwms_ts_id, p_units, times, values, p_qualities)
    #     df2 = self.cwms.retrieve_ts(
    #         "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )

    #     assert df.equals(df2)

    #     sample = df2.sample(frac=0.3)
    #     sample["ts_id"] = "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"
    #     self.cwms.delete_by_df(sample)

    #     df3 = self.cwms.retrieve_ts(
    #         "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )

    #     not_deleted = df2[
    #         False == df2["date_time"].isin(sample["date_time"])
    #     ].reset_index(drop=True)

    #     assert not_deleted.equals(df3)

    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS DATA")
    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS ID")
    #     self.cwms.delete_location("TST")
    #     try:
    #         df2 = self.cwms.retrieve_ts(
    #             "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #             "2019/1/1",
    #             "2019/9/1",
    #             "cms",
    #             return_df=True,
    #         )
    #     except ValueError as e:
    #         msg = 'TS_ID_NOT_FOUND: The timeseries identifier "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"'
    #         assert msg in e.__str__()

    # def test_eleven(self):
    #     """
    #     store_by_df: Testing for successful store_by_df
    #     """

    #     df = self.cwms.retrieve_ts(
    #         "LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )
    #     if not self.cwms.retrieve_location("TST"):
    #         self.cwms.store_location("TST")

    #     p_cwms_ts_id = "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"
    #     p_units = "cms"

    #     test_df = df.copy()
    #     test_df["ts_id"] = p_cwms_ts_id
    #     test_df["units"] = p_units

    #     self.cwms.store_by_df(test_df)
    #     df2 = self.cwms.retrieve_ts(
    #         "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )

    #     assert df.equals(df2)

    #     # test for protected
    #     test_df["quality_code"].iloc[0] = 2147483649
    #     test_df["value"].iloc[0] = 9999
    #     self.cwms.store_by_df(test_df)

    #     test_df["quality_code"].iloc[0] = 0
    #     test_df["value"].iloc[0] = -9999

    #     df3 = self.cwms.retrieve_ts(
    #         "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #         "2019/1/1",
    #         "2019/9/1",
    #         "cms",
    #         return_df=True,
    #     )

    #     assert df3["value"].iloc[0] == 9999

    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS DATA")
    #     self.cwms.delete_ts("TST.Flow-Out.Ave.~1Day.1Day.CBT-REV", "DELETE TS ID")
    #     self.cwms.delete_location("TST")
    #     try:
    #         df2 = self.cwms.retrieve_ts(
    #             "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV",
    #             "2019/1/1",
    #             "2019/9/1",
    #             "cms",
    #             return_df=True,
    #         )
    #     except ValueError as e:
    #         msg = 'TS_ID_NOT_FOUND: The timeseries identifier "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"'
    #         assert msg in e.__str__()

    # def test_final(self):
    #     """
    #     close: Testing good close from db for cleanup
    #     """

    #     c = self.cwms.close()

    #     assert c == True
