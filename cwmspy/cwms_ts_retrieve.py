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

class CwmsTsRetrieve:

    @LD
    def retrieve_time_series(
        self,
        ts_ids,
        units=["EN"],
        p_datums=None,
        p_start=None,
        p_end=None,
        p_timezone="UTC",
        p_office_id=None,
        as_json=False,
    ):
        """Retreives time series in a number of formats for a combination 
        time window, timezone, formats, and vertical datums 

        Parameters
        ----------
        ts_ids : list
            The names (time series identifers) of the time series to retrieve.
            Can use sql wildcard for single time series identifier, 
            example: ts_ids =["Some.*.fully.?.path"]
        units : list

            The units to return the time series in. Valid units are:

            - <span style="color:#bf2419">`"EN"`</span>
            -- English Units
            - <span style="color:#bf2419">`"SI"`</span>
            -- SI units
            - <span>actual unit of parameter</span>
            -- (e.g. "ft", "cfs")

            If the P_Units variable has fewer positions than the p_name 
            variable, the last unit position is used for all remaning names. 
            If the units are unspecified or NULL, the NATIVE units will be 
            used for all time series.

        p_datums : str
            The vertical datums to return the units in. Valid datums are:

            - <span style="color:#bf2419">`"NATIVE"`</span>
            - <span style="color:#bf2419">`"NGVD29"`</span>
            - <span style="color:#bf2419">`"NAVD88"`</span>

        p_start : str
            The start of the time window to retrieve time series for. 
            No time series values earlier this time will be retrieved. 
            If unspecified or NULL, a value of 24 hours prior to the specified 
            or default end of the time window will be used. for the start of 
            the time window.

            FORMAT: %Y-%m-%d

        p_end : str
            The end of the time window to retrieve time series for. 
            No time series values later this time will be retrieved. 
            If unspecified or NULL, the current time will be used for the end 
            of the time window.

            FORMAT: %Y-%m-%d

        p_timezone : type
            The time zone to retrieve the time series in. 
            The P_Start and P_End parameters - if used - are also interpreted 
            according to this time zone. If unspecified or NULL, the UTC time 
            zone is used.
        p_office_id : type
            The office to retrieve time series for. 
            If unspecified or NULL, time series for all offices in the database 
            that match the other criteria will be retrieved.

        Returns
        -------
        pd.Core.DataFrame
            Pandas dataframe
        Examples
        -------
        ```python
        >>> import CWMS
        >>> import datetime
        >>> cwms = CWMS()
        >>> cwms.connect()
            True
        >>> now = datetme.datetime.utcnow()
        >>> start = (now - datetime.timedelta(10)).strftime('%Y-%m-%d')
        >>> end = now.strftime('%Y-%m-%d')
        >>> cwms.retrieve_time_series(ts_ids=['Some.Fully.Qualified.Cwms.Ts.ID', 
                                       'Another.Fully.Qualified.Cwms.Ts.ID'],
                                       p_start=start,
                                       p_end = end)


        ```
        """

        p_names = "|".join(ts_ids)
        p_units = "|".join(units)

        cur = self.conn.cursor()

        p_results = cur.var(cx_Oracle.CLOB)
        p_date_time = cur.var(cx_Oracle.DATETIME)
        p_query_time = cur.var(int)
        p_format_time = cur.var(int)
        p_ts_count = cur.var(int)
        p_value_count = cur.var(int)

        p_format = "JSON"

        try:

            clob = cur.callproc(
                "cwms_ts.retrieve_time_series",
                [
                    p_results,
                    p_date_time,
                    p_query_time,
                    p_format_time,
                    p_ts_count,
                    p_value_count,
                    p_names,
                    p_format,
                    p_units,
                    p_datums,
                    p_start,
                    p_end,
                    p_timezone,
                ],
            )

        except Exception as e:
            LOGGER.error("Error in retrieving time series")
            cur.close()
            raise ValueError(e.__str__())
        cur.close()
        try:
            result = json.loads(clob[0].read())
            if as_json:
                return result
        except JSONDecodeError as e:
            LOGGER.info("No data for the requested pathnames and dates.")
            return pd.DataFrame()

        try:
            ts = result["time-series"]["time-series"]
        except KeyError:
            LOGGER.warning("No data found")
            return pd.DataFrame()

        df_list = []
        for data in ts:
            ts_id = data["name"]

            riv = data.get("regular-interval-values")
            if riv:

                segments = riv["segments"]
                units = riv["unit"].split(" ")[0]
                df_l = []
                for segment in segments:
                    first_time = segment["first-time"]
                    last_time = segment["last-time"]
                    value_count = segment["value-count"]

                    values = segment["values"]

                    date_range = pd.date_range(first_time, last_time, value_count)
                    df = pd.DataFrame(values, columns=["value", "quality_code"])
                    df.insert(0, "date_time", date_range)
                    df_l.append(df)
                df = pd.concat(df_l)
                df["units"] = units

            else:
                iiv = data["irregular-interval-values"]
                units = iiv["unit"].split(" ")[0]
                values = np.array(iiv["values"])
                df = pd.DataFrame(values)
                df.columns = ["date_time", "value", "quality_code"]
                df["date_time"] = pd.to_datetime(df["date_time"])
                df["units"] = units

            df.insert(0, "ts_id", ts_id)
            df_list.append(df)
        try:
            df = pd.concat(df_list)
            df["time_zone"] = p_timezone
            df["value"] = df["value"].astype(float)
        except ValueError:
            df = pd.DataFrame()

        return df

    @LD
    def retrieve_ts(
        self,
        p_cwms_ts_id,
        start_time,
        end_time,
        p_units=None,
        p_timezone="UTC",
        p_trim="F",
        p_start_inclusive="T",
        p_end_inclusive="T",
        p_previous="T",
        p_next="F",
        version_date="1111/11/11",
        p_max_version="T",
        p_office_id=None,
        return_df=True,
        local_tz=False,
    ):
        """Retrieves time series data for a specified time series and
            time window.

        Parameters
        ----------
        p_cwms_ts_id : str
            The time series identifier to retrieve data for.
        start_time : str "%Y/%m/%d"
            The start time of the time window.
        end_time : str "%Y/%m/%d"
            The end time of the time window.
        p_units : str
            The unit to retrieve the data values in.
        p_timezone : str
            The time zone for the time window and retrieved times.
        p_trim : str
            A flag ('T' or 'F') that specifies whether to trim missing values
            from the beginning and end of the retrieved values.
        p_start_inclusive : str
            A flag ('T' or 'F') that specifies whether the time window begins
            on ('T') or after ('F') the start time.
        p_end_inclusive : str
            A flag ('T' or 'F') that specifies whether the time window ends on
            ('T') or before ('F') the end time.
        p_previous : str
            A flag ('T' or 'F') that specifies whether to retrieve the latest
            value before the start of the time window.
        p_next : str
            A flag ('T' or 'F') that specifies whether to retrieve the earliest
            value after the end of the time window.
        p_version_date : str
            The version date of the data to retrieve. If not specified or NULL,
            the version date is determined by P_Max_Version.
        p_max_version : str
            A flag ('T' or 'F') that specifies whether to retrieve the maximum
            ('T') or minimum ('F') version date if P_Version_Date is NULL.
        p_office_id : str
            The office that owns the time series.
        return_df : bool
            Return result as pandas df.
        local_tz : bool
            Return data in local timezone.

        Returns
        -------
        list or pandas df
            Time series data, date_time, value, quality_code.


        Examples
        -------
        ```python
        >>> from cwmspy import CWMS
        >>> cwms = CWMS()
        >>> cwms.connect()
        >>> cwms.connect()
            True
        >>> df = cwms.retrieve_ts(p_cwms_ts_id='Some.Fully.Qualified.Ts.Id',
                                start_time='2019/1/1', end_time='2019/9/1', return_df=True)
        >>> df.head()
                        date_time       value  quality_code
            0 2018-12-31 08:00:00  574.831986             0
            1 2019-01-01 08:00:00  668.277580             0
            2 2019-01-02 08:00:00  608.812202             0
            3 2019-01-03 08:00:00  597.485463             0
            4 2019-01-04 08:00:00  560.673563             0
        ```
        """

        p_start_time = datetime.datetime.strptime(start_time, "%Y/%m/%d")
        p_end_time = datetime.datetime.strptime(end_time, "%Y/%m/%d")

        p_version_date = datetime.datetime.strptime(version_date, "%Y/%m/%d")

        cur = self.conn.cursor()
        p_at_tsv_rc = self.conn.cursor().var(cx_Oracle.CURSOR)
        try:

            cur.callproc(
                "cwms_ts.retrieve_ts",
                [
                    p_at_tsv_rc,
                    p_cwms_ts_id,
                    p_units,
                    p_start_time,
                    p_end_time,
                    p_timezone,
                    p_trim,
                    p_start_inclusive,
                    p_end_inclusive,
                    p_previous,
                    p_next,
                    p_version_date,
                    p_max_version,
                    p_office_id,
                ],
            )

        except Exception as e:
            LOGGER.error("Error in retrieving time series.")
            cur.close()
            raise ValueError(e.__str__())
        cur.close()

        output = [r for r in p_at_tsv_rc.getvalue()]
        output_len = len(output)
        LOGGER.info(f"Found {output_len} records.")
        if local_tz:
            for i, v in enumerate(output):
                date = v[0]
                local = self._convert_to_local_time(date=date, timezone=p_timezone)

                output[i] = [local] + [x for x in v[1:]]

        if return_df:
            output = pd.DataFrame(
                output, columns=["date_time", "value", "quality_code"]
            )
            output["time_zone"] = p_timezone

        return output

    @LD
    def retrieve_multi_ts(
        self,
        p_cwms_ts_id_list,
        start_time=None,
        end_time=None,
        p_units_list=None,
        p_timezone="UTC",
        p_start_inclusive="T",
        p_end_inclusive="T",
        p_previous="T",
        p_next="F",
        version_date="1111/11/11",
        p_max_version="T",
        p_office_id=None,
        return_df=True,
        local_tz=False,
        por=False,
        pivot=False,
    ):
        """
        Retrieves time series data for a list of specified time series
            and time window or period of record.

        Parameters
        ----------
        p_cwms_ts_id_list : list
            List of time series identifiers.
        start_time : str
            The start of the time window in the specified or default time zone.
        end_time : str
            The end of the time window in the specified or default time zone.
        p_units_list : list
            Unit list to retrieve the data values in.
        p_timezone : str
            The time zone for the time window and retrieved times.
        p_start_inclusive : str
            A flag ('T' or 'F') that specifies whether the time window begins
            on ('T') or after ('F') the start time.
        p_end_inclusive : str
            A flag ('T' or 'F') that specifies whether the time window ends on
            ('T') or before ('F') the end time.
        p_previous : str
            A flag ('T' or 'F') that specifies whether to retrieve the latest
            value before the start of the time window.
        p_next : str
            A flag ('T' or 'F') that specifies whether to retrieve the earliest
            value after the end of the time window.
        version_date : str
            The version date of the data to retrieve. If not specified or NULL,
            the version date is determined by P_Max_Version.
        p_max_version : str
            A flag ('T' or 'F') that specifies whether to retrieve the maximum
            ('T') or minimum ('F') version date if P_Version_Date is NULL.
        p_office_id : str
            The office that owns the time series.
        return_df : bool
            Return result as pandas df.
        local_tz : bool
            Return data in local timezone.
        por : bool
            Return period of record.
        pivot : bool
            Pivot dataframe so cwms ts id's are columns.

        Returns
        -------
        list or pandas df
            Time series data, date_time, value, quality_code.

        Examples
        -------
        ```python
        >>> from cwmspy.core import CWMS
        >>> cwms = CWMS()
        >>> cwms.connect()
        >>> p_cwms_ts_id_list = ['Some.Fully.Qualified.Cwms.Ts.ID',
        >>>                     'Second.Fully.Qualified.Cwms.Ts.ID']
        >>> df = cwms.retrieve_multi_ts(p_cwms_ts_id_list, '2019/1/1', '2019/9/1')
        >>> df.head()

                        date_time                                ts_id       value  quality_code
            0 2018-12-31 08:00:00  Some.Fully.Qualified.Cwms.Ts.ID      574.831986             0
            1 2019-01-01 08:00:00  Some.Fully.Qualified.Cwms.Ts.ID      668.277580             0
            2 2019-01-02 08:00:00  Some.Fully.Qualified.Cwms.Ts.ID      608.812202             0
            3 2019-01-03 08:00:00  Some.Fully.Qualified.Cwms.Ts.ID      597.485463             0
            4 2019-01-04 08:00:00  Some.Fully.Qualified.Cwms.Ts.ID      560.673563             0
        ```
        ```python
        >>> df = cwms.retrieve_multi_ts(p_cwms_ts_id_list,
        >>>                            '2019/1/1',
        >>>                            '2019/9/1',
        >>>                            pivot=True)
        >>> df.head()

            ts_id                  'Some.Fully.Qualified.Cwms.Ts.ID'      'Second.Fully.Qualified.Cwms.Ts.ID'
            date_time
            2018-12-31 08:00:00                           574.831986                                     NaN
            2018-12-31 23:00:00                                  NaN                                     0.0
            2019-01-01 00:00:00                                  NaN                                     0.0
            2019-01-01 01:00:00                                  NaN                                     0.0
            2019-01-01 02:00:00                                  NaN                                     0.0
        ```
        """

        l = []
        for i, ts_id in enumerate(p_cwms_ts_id_list):
            if p_units_list:
                p_units = p_units_list[i]
            else:
                p_units = None

            arg = [
                p_units,
                p_timezone,
                "F",
                p_start_inclusive,
                p_end_inclusive,
                p_previous,
                p_next,
                version_date,
                p_max_version,
                p_office_id,
                return_df,
                local_tz,
            ]

            if por:
                args0 = [ts_id]
                args = args0 + arg
                rslt = self.get_por(*args)
            else:
                args0 = [ts_id, start_time, end_time]
                args = args0 + arg
                rslt = self.retrieve_ts(*args)

            if return_df:
                rslt["ts_id"] = ts_id

            l.append(rslt)

        if return_df:
            l = pd.concat(l, ignore_index=True)
            l = l[["date_time", "ts_id", "value", "quality_code"]]
            if pivot:
                l = l.pivot(index="date_time", columns="ts_id", values="value")

        return l