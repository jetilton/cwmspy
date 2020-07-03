# -*- coding: utf-8 -*-
"""
Facilities for working with time series
"""
import cx_Oracle
import datetime
import pandas as pd
from dateutil import tz
import pytz
import logging
from itertools import combinations
import numpy as np
import json
from json import JSONDecodeError

from .utils import log_decorator
from .utils import CwmsTsUtils

LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)

class CwmsTsDelete:

    @LD
    def delete_ts(
        self, p_cwms_ts_id, p_delete_action="DELETE TS ID", p_db_office_id=None
    ):
        """Deletes a time series from the database.

        Parameters
        ----------
        p_cwms_ts_id : str
            The identifier of the time series to delete.
        p_delete_action : type
            Specifies what to delete.
        p_db_office_id : type
            The office that owns the time series. If not specified or NULL,
            the session user's default office will be used..

        Returns
        -------
        Boolean
            True for success.

        Examples
        -------
        ```python
        >>> cwms.delete_ts("Some.Fully.Qualified.Cwms.Ts.Id",
                            "DELETE TS DATA")
            True
        ```
        """

        cur = self.conn.cursor()
        try:

            cur.callproc(
                "cwms_ts.delete_ts", [p_cwms_ts_id, p_delete_action, p_db_office_id]
            )
        except Exception as e:
            LOGGER.error("Error in delete_ts.")
            cur.close()
            raise ValueError(e.__str__())
        cur.close()
        return True

    @LD
    def delete_ts_window(
        self,
        p_cwms_ts_id,
        start_time,
        end_time,
        p_override_protection="F",
        p_version_date=None,
        p_db_office_code=26,
    ):
        """Delete ts with user specified start and end date.

        Parameters
        ----------
        p_cwms_ts_id : str
            The time series identifier.
        start_time : str "%Y/%m/%d"
            The start time of the time window.
        end_time : str "%Y/%m/%d"
            The end time of the time window.
        p_override_protection : str
            A flag ('T' or 'F') specifying whether to override the protection
            flag on any existing data value.
        p_version_date : datetime
            The version date of the data
        p_db_office_code : int
           The unique numeric code identifying the office owning the time
           series (the default is 26).

        Returns
        -------
        Boolean
            True for success.

        Examples
        -------
        ```python
        >>> import CWMS
        >>> cwms = CWMS()
        >>> cwms.connect()
        >>> start_time = '2018/1/1'
        >>> end_time = '2019/2/1'
        >>> p_cwms_ts_id = 'your.cwms.ts.id'
        >>> cwms.delete_ts_window(p_cwms_ts_id, start_time, end_time,
        >>>                      p_override_protection='F', p_version_date=None,
        >>>                      p_db_office_code=26)
            True
        ```
        """

        p_start_time = datetime.datetime.strptime(start_time, "%Y/%m/%d")
        p_end_time = datetime.datetime.strptime(end_time, "%Y/%m/%d")

        alter_session_sql = (
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
        )
        cur = self.conn.cursor()
        cur.execute(alter_session_sql)

        delete_sql = """
                    delete from cwms_20.at_tsv_{}
                    where ts_code = {}
                    and date_time between to_date('{}') and to_date('{}')
                    """
        if not p_override_protection:
            delete_sql += """
                    and  quality_code not in (select quality_code from
                    cwms_20.cwms_data_quality where validity_id = 'PROTECTED')
                    """

        ts_code = self.get_ts_code(
            p_cwms_ts_id=p_cwms_ts_id, p_db_office_code=p_db_office_code
        )

        try:
            for year in range(p_start_time.year, (p_end_time.year + 1)):
                table = str(year)
                LOGGER.info(f"Deleteing {p_cwms_ts_id} from table {table}")
                sql = delete_sql.format(table, ts_code, start_time, end_time)
                if p_version_date:
                    sql += f"and version_date = to_date('{p_version_date}')"
                cur.execute(sql)
        except Exception as e:
            LOGGER.error(e)
            cur.execute("rollback")
            cur.close()
            raise Exception(e.__str__())
        cur.execute("commit")
        cur.close()
        return True

    @LD
    def delete_by_df(
        self, df, p_override_protection="F", p_version_date=None, p_db_office_code=26
    ):
        """Deletes time series data with a pandas.Core.DataFrame.

        Parameters
        ----------
        df : pandas.core.DataFrame
            A pandas data frame with columns `ts_id` and `date_time`.
        p_override_protection : str
            A flag ('T' or 'F') specifying whether to override the protection
            flag on any existing data value.
        p_version_date : type
            The version date of the data.
        p_db_office_code : type
            The unique numeric code that identifies the office that owns the time series.

        Returns
        -------
        boolean
            True for success
        Examples
        -------
        >>> cwms = CWMS()
        >>> cwms.connect()
        >>> df = cwms.retrieve_ts("Some.Fully.Qualified.Cwms.pathname","2019/1/1","2019/9/1","cms",return_df=True)
        >>> df["ts_id"] = "Some.Fully.Qualified.Cwms.pathname"
        >>> cwms.delete_by_df(df)
            True
        """
        # A standard format between the database and script
        alter_session_sql = (
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
        )

        cur = self.conn.cursor()
        cur.execute(alter_session_sql)
        s = "to_date('{}')"
        df = df.copy()
        df["string_date_time"] = [
            s.format(x.strftime("%Y-%m-%d %H:%M:%S")) for x in df["date_time"]
        ]
        delete_sql = """
                    delete from cwms_20.at_tsv_{}
                    where ts_code = {}
                    and date_time in {}
                    """
        if not p_override_protection:
            delete_sql += """
                    and  quality_code not in (select quality_code from
                    cwms_20.cwms_data_quality where validity_id = 'PROTECTED')
                    """
        df.set_index("date_time", inplace=True)

        grouped = df.groupby("ts_id")

        # if any ts_id or year fails, all deletes will be rolled back
        try:

            for p_cwms_ts_id, value in grouped:
                grpd = value.groupby(pd.Grouper(freq="Y"))

                # Get the ts_code by id because the at_tsv_YEAR tbls do not have
                # ts_ids
                ts_code = self.get_ts_code(
                    p_cwms_ts_id=p_cwms_ts_id, p_db_office_code=p_db_office_code
                )

                # Group by year to go through all of the at_tsv_YEAR tbls
                for date, val in grpd:
                    value_len = str(val.shape[0])
                    year = str(date.year)
                    LOGGER.info(
                        f"Deleting {value_len} values from {p_cwms_ts_id} at table {year}"
                    )
                    times = "("
                    for time in list(val["string_date_time"].values)[:-1]:
                        times += time + (",")
                    times += list(val["string_date_time"].values)[-1] + ")"

                    sql = delete_sql.format(year, ts_code, times)

                    if p_version_date:
                        sql += "and version_date = to_date('{}')".format(p_version_date)

                    cur.execute(sql)

        except Exception as e:
            LOGGER.error(e)
            cur.execute("rollback")
            cur.close()
            raise Exception(e.__str__())
        cur.execute("commit")
        cur.close()
        return True
