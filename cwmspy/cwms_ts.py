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


LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)


class CwmsTsMixin:
    @staticmethod
    @LD
    def _convert_to_local_time(date, timezone="UTC"):
        # reference: https://stackoverflow.com/a/4771733/4296857
        if date == None:
            return None
        from_zone = tz.gettz(timezone)
        utc = date.replace(tzinfo=from_zone)
        LOCAL_TIMEZONE = (
            datetime.datetime.now(datetime.timezone(datetime.timedelta(0)))
            .astimezone()
            .tzinfo
        )
        local_string = utc.astimezone(LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
        local = datetime.datetime.strptime(local_string, "%Y-%m-%d %H:%M:%S")
        LOGGER.debug(f"Date converted to {local_string}")
        return local

    @LD
    def get_ts_code(self, p_cwms_ts_id, p_db_office_code=None):
        """Get the CWMS TS Code of a given pathname.

        Parameters
        ----------
        p_cwms_ts_id : str
            CWMS time series identifier.
        p_db_office_code : int
            The unique numeric code identifying the office
                               owning the time series

        Returns
        -------
        str
            the unique numeric code value for the specified
                time series if successful, False otherwise.

        Examples
        -------
        ```python
        >>> from cwmspy import CWMS
        >>> cwms = CWMS()
        >>> cwms.connect()
            True
        >>> cwms.get_ts_code("Some.fully.qualified.ts.id")
            "04319021"

        ```
        """

        cur = self.conn.cursor()
        try:

            ts_code = cur.callfunc(
                "cwms_ts.get_ts_code",
                cx_Oracle.STRING,
                [p_cwms_ts_id, p_db_office_code],
            )
        except Exception as e:
            LOGGER.error("Error retrieving ts_code")
            cur.close()
            raise ValueError(e.__str__())
        LOGGER.info(f"get_ts_code returned {ts_code}")
        cur.close()

        return ts_code

    @LD
    def get_ts_max_date(
        self,
        p_cwms_ts_id,
        p_time_zone="UTC",
        version_date="1111/11/11",
        p_office_id=None,
    ):
        """Retrieves the latest non-null time series data date in the
            database for a time series

        Parameters
        ----------
        p_cwms_ts_id : str
            The time series identifier.
        p_time_zone : str
            The time zone in which to retrieve the latest time
            (the default is 'UTC').
        version_date : str
            The version date of the time series in the specified time zone
            (the default is '1111/11/11' which represents non-versioned).
        p_office_id : type
            Description of parameter `p_office_id` (the default is None).

        Returns
        -------
        datetime.datetime
            The latest non-null date in the time series

        Examples
        -------
        ```python
        >>> import CWMS
        >>> cwms = CWMS()
        >>> cwms.connect()
        >>> cwms.get_ts_max_date('Some.Fully.Qualified.Cwms.Ts.ID')
            datetime.datetime(2019, 8, 16, 7, 0)
        ```
        """
        p_version_date = datetime.datetime.strptime(version_date, "%Y/%m/%d")
        cur = self.conn.cursor()
        try:

            max_date = cur.callfunc(
                "cwms_ts.get_ts_max_date",
                cx_Oracle.DATETIME,
                [p_cwms_ts_id, p_time_zone, p_version_date, p_office_id],
            )
        except Exception as e:
            cur.close()
            LOGGER.error("Error retrieving get_ts_max_date")
            raise ValueError(e.__str__())
        LOGGER.info(f"max_date returned {max_date}")
        cur.close()

        return max_date

    @LD
    def get_ts_min_date(
        self,
        p_cwms_ts_id,
        p_time_zone="UTC",
        version_date="1111/11/11",
        p_office_id=None,
    ):
        """Retrieves the earliest non-null time series data date in the
            database for a time series

        Parameters
        ----------
        p_cwms_ts_id : str
            The time series identifier.
        p_time_zone : str
            The time zone in which to retrieve the latest time
            (the default is 'UTC').
        version_date : str
            The version date of the time series in the specified time zone
            (the default is '1111/11/11' which represents non-versioned).
        p_office_id : type
            Description of parameter `p_office_id` (the default is None).

        Returns
        -------
        datetime.datetime
            The earliest non-null date in the time series

        Examples
        -------
        ```python
        >>> import CWMS

        >>> cwms = CWMS()
        >>> cwms.connect()
            True
        >>> cwms.get_ts_min_date('Some.Fully.Qualified.Cwms.Ts.ID')

            datetime.datetime(1975, 2, 18, 8, 0)
        ```
        """

        p_version_date = datetime.datetime.strptime(version_date, "%Y/%m/%d")
        cur = self.conn.cursor()
        try:

            min_date = cur.callfunc(
                "cwms_ts.get_ts_min_date",
                cx_Oracle.DATETIME,
                [p_cwms_ts_id, p_time_zone, p_version_date, p_office_id],
            )
        except Exception as e:
            LOGGER.error("Error in retrieving get_ts_min_date")
            cur.close()
            raise ValueError(e.__str__())
        LOGGER.info(f"get_ts_min_date returned {min_date}")
        cur.close()

        return min_date

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
        p_end : str
            The end of the time window to retrieve time series for. 
            No time series values later this time will be retrieved. 
            If unspecified or NULL, the current time will be used for the end 
            of the time window.
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

        >>> cwms = CWMS()
        >>> cwms.connect()
            True
        >>> cwms.retrieve_time_series(['Some.Fully.Qualified.Cwms.Ts.ID', 
                                       'Another.Fully.Qualified.Cwms.Ts.ID'])


        ```
        """

        p_names = " | ".join(ts_ids)
        p_units = " | ".join(units)

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
        except JSONDecodeError as e:
            LOGGER.info("No data for the requested pathnames and dates.")
            return False
        df_list = []
        for data in result["time-series"]["time-series"]:
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

                    values = segment['values']

                    date_range = pd.date_range(first_time, last_time, value_count)
                    df = pd.DataFrame(values, columns=["value", "quality_code"])
                    df.insert(0,"date_time", date_range)
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

        return output

    @LD
    def store_ts(
        self,
        p_cwms_ts_id,
        p_units,
        times,
        values,
        qualities=None,
        format=None,
        p_store_rule="REPLACE ALL",
        p_override_prot="F",
        version_date=None,
        p_office_id=None,
    ):
        """Stores time series data to the database using parameter types
            compatible with cx_Oracle Pyton package.

        Parameters
        ----------
        p_cwms_ts_id : str
            The time series identifier.
        p_units : str
            The unit of the data values.
        times : list
            The UTC times of the data values.  Can be string or type datetime
        values : list
            The data values.
        p_qualities : list
            The data quality codes for the data values.
        format : str
            strftime to parse time, eg “%d/%m/%Y”, note that “%f” will
            parse all the way up to nanoseconds. See strftime documentation
            for more information on choices:
                https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
            store_ts will try to infer format if None.
        p_store_rule : type
            The store rule to use.
        p_override_prot : str
            A flag ('T' or 'F') specifying whether to override the protection
            flag on any existing data value.
        p_version_date : datetime
            Description of parameter `p_office_id`.
        p_office_id : type
            The office owning the time series. If not specified or NULL, the
            session user's default office is used.

        Returns
        -------
        Boolean
            True for success.

        Examples
        -------
        ```python
        >>> from cwmspy import CWMS
        >>> import datetime
        >>> cwms = CWMS()
        >>> cwms.connect()
            True
        >>> p_cwms_ts_id = 'Some.Fully.Qualified.Cwms.Ts.ID'
        >>> p_units = "cms"
        >>> values = [1,2,3]
        >>> p_qualities = [0,0,0]
        >>> times = ['2019/1/1','2019/1/2','2019/1/3']
        >>> times = [datetime.datetime.strptime(x, "%Y/%m/%d") for x in times]
        >>> cwms.store_ts(p_cwms_ts_id, p_units, times, values, p_qualities)
            True
        ```
        """

        cur = self.conn.cursor()

        p_values = cur.arrayvar(cx_Oracle.NATIVE_FLOAT, values)

        t = pd.to_datetime(times, utc=True, infer_datetime_format=True, format=format)
        # Get the UTC times of the data values in Java milliseconds
        # this is what actually goes into Store_Ts
        zero = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
        p_times = [((time - zero).total_seconds() * 1000) for time in t]

        if not version_date:
            p_version_date = datetime.datetime(1111, 11, 11)
        else:
            p_version_date = version_date
        if not qualities:
            p_qualities = [0 for x in p_times]
        else:
            p_qualities = qualities

        try:
            data_len = len(values)
            LOGGER.info(f"Loading {data_len} values for {p_cwms_ts_id}")
            cur.callproc(
                "cwms_ts.store_ts",
                [
                    p_cwms_ts_id,
                    p_units,
                    p_times,
                    p_values,
                    p_qualities,
                    p_store_rule,
                    p_override_prot,
                    p_version_date,
                    p_office_id,
                ],
            )
        except Exception as e:
            LOGGER.error("Error in store_ts.")
            cur.close()
            raise ValueError(e.__str__())
        cur.close()
        return True

    @LD
    def store_by_df(
        self,
        df,
        p_store_rule="REPLACE ALL",
        p_override_prot="F",
        version_date=None,
        p_office_id=None,
    ):
        """Stores time series data to the database with pandas.core.dataframe as input.

        Parameters
        ----------
        df : pandas.core.DataFrame
            Pandas DataFrame that requires `ts_id`, `date_time`, `units`,
            and `value` columns.  If optional column `quality_code` does not exist,
            all quality codes are assumed equal to 0.
        p_store_rule : type
            The store rule to use.
        p_override_prot : str
            A flag ('T' or 'F') specifying whether to override the protection
            flag on any existing data value.
        p_version_date : datetime
            Description of parameter `p_office_id`.
        p_office_id : type
            The office owning the time series. If not specified or NULL, the
            session user's default office is used.

        Returns
        -------
        Boolean
            `True` for success

        Examples
        -------
        ```python
        >>> from cwmspy import CWMS
        >>> import datetime
        >>> cwms = CWMS()
        >>> cwms.connect()
            True
        >>> p_cwms_ts_id = 'Some.Fully.Qualified.Pathname'
        >>> p_units = 'cms'
        >>> start_time = '2019/1/1'
        >>> end_time = '2019/8/1'
        >>> df = cwms.retrieve_ts(p_cwms_ts_id=p_cwms_ts_id,
        >>>                     start_time=start_time,
        >>>                     end_time=end_time,
        >>>                     p_units=p_units
        >>>                     )
        >>> df['units'] = p_units
        >>> df['ts_id'] = p_cwms_ts_id
        >>> df['value'] = df['value'] / 1.1
        >>> cwms.store_by_df(df)
            True
        ```

        """

        if "quality_code" not in df.columns:
            df["quality_code"] = 0
        df["date_time"] = df.apply(
            lambda row: row["date_time"].replace(tzinfo=None), axis=1
        )
        grouped = df.groupby("ts_id")

        for p_cwms_ts_id, value in grouped:

            grpd = value.groupby("units")

            for p_units, val in grpd:
                # Only want to write new data to disk
                # Get current data, merge it for comparison

                # Add a little overlap to get current data
                min_date = (
                    val["date_time"].min() - datetime.timedelta(days=1)
                ).strftime("%Y/%m/%d")
                max_date = (
                    val["date_time"].max() + datetime.timedelta(days=1)
                ).strftime("%Y/%m/%d")

                # Will throw an error if time series identifier does not exist
                try:
                    LOGGER.info("Get existing data if it does exist for comparison")
                    current_data = self.retrieve_ts(
                        p_cwms_ts_id=p_cwms_ts_id,
                        start_time=min_date,
                        end_time=max_date,
                        p_units=p_units,
                    )
                    LOGGER.info("Merging with existing data to only write new values")
                    merged = val.merge(
                        current_data,
                        on=["date_time", "value"],
                        how="outer",
                        suffixes=["", "_"],
                        indicator=True,
                    )

                    # The data to store after comparing to current data
                    new_data = merged[merged["_merge"] == "left_only"]
                    if new_data.empty:
                        LOGGER.info(f"No new data to load for {p_cwms_ts_id}")
                        continue
                    else:
                        new_data_len = new_data.shape[0]
                        LOGGER.info(f"Loading {new_data_len} new values")
                except ValueError:
                    new_data = val.copy()

                self.store_ts(
                    p_cwms_ts_id=p_cwms_ts_id,
                    p_units=p_units,
                    times=list(new_data["date_time"]),
                    values=list(new_data["value"]),
                    qualities=list(new_data["quality_code"]),
                    format=None,
                    p_store_rule=p_store_rule,
                    p_override_prot=p_override_prot,
                    version_date=version_date,
                    p_office_id=p_office_id,
                )
        return True

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
    def rename_ts(
        self,
        p_cwms_ts_id_old,
        p_cwms_ts_id_new,
        p_utc_offset_new=None,
        p_office_id=None,
    ):
        """Renames a time series in the database, optionally setting a new
            regular interval offset.

            Restrictions on changing include:
            - New time series identifier must agree with new/existing data
                interval and offset (regular/irregular)
            - Cannot change time utc offset if from one regular offset to
                another if time series data exists

        Parameters
        ----------
        p_cwms_ts_id_old : str
            The existing time series identifier.
        p_cwms_ts_id_new : str
            The new time series identifier.
        p_utc_offset_new : int
            The new offset into the utc data interval in minutes.
        p_office_id : str
            The office that owns the time series. If not specified or NULL,
                the session user's default office is used.

        Returns
        -------
        Boolean
            True for success.

        Examples
        -------
        ```python
        >>> p_cwms_ts_id_old = "Some.Fully.Qualified.Ts.ID"
        >>> p_cwms_ts_id_new = "New.Fully.Qualified.Ts.ID"
        >>> cwms.rename_ts(
            p_cwms_ts_id_old,
            p_cwms_ts_id_new,
            p_utc_offset_new=None,
            p_office_id=None
            )
        ```
        """

        cur = self.conn.cursor()
        try:

            cur.callproc(
                "cwms_ts.rename_ts",
                [p_cwms_ts_id_old, p_cwms_ts_id_new, p_utc_offset_new, p_office_id],
            )
        except Exception as e:
            LOGGER.error("Error in rename_ts")
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

    @LD
    def get_extents(
        self,
        p_cwms_ts_id,
        p_time_zone="UTC",
        version_date="1111/11/11",
        p_office_id=None,
    ):
        """Retrieves the earliest and latest non-null time series data date in
            the database for a time series

        Parameters
        ----------
        p_cwms_ts_id : str
            The time series identifier.
        p_time_zone : str
            The time zone in which to retrieve the latest time
            (the default is 'UTC').
        version_date : str
            The version date of the time series in the specified time zone
            (the default is '1111/11/11' which represents non-versioned).
        p_office_id : int
            Description of parameter `p_office_id` (the default is None).

        Returns
        -------
        datetime.datetime
            The earliest and latest non-null dates in the time series

        Examples
        -------
        ```python
        >>> import CWMS
        >>> cwms = CWMS()
        >>> cwms.connect()
            True
        >>> cwms.get_extents('Some.Fully.Qualified.Cwms.Ts.ID')
            (datetime.datetime(1975, 2, 18, 8, 0), datetime.datetime(2019, 8, 16, 7, 0))
        ```
        """

        min_date = self.get_ts_min_date(
            p_cwms_ts_id,
            p_time_zone=p_time_zone,
            version_date=version_date,
            p_office_id=p_office_id,
        )

        max_date = self.get_ts_max_date(
            p_cwms_ts_id,
            p_time_zone=p_time_zone,
            version_date=version_date,
            p_office_id=p_office_id,
        )

        return min_date, max_date

    @LD
    def get_por(
        self,
        p_cwms_ts_id,
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
        """Short summary.

        Parameters
        ----------
        p_cwms_ts_id : str
            The time series identifier.
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
        pd.core.frame.DataFrame or list
            The period of record for given time series identifier

        Examples
        -------
        ```Python
        >>> from cwmspy.core import CWMS
        >>> cwms = CWMS()
        >>> cwms.connect()
        >>> df = cwms.get_por('Some.Fully.Qualified.Cwms.Ts.ID')
        >>> df.head()
                        date_time        value  quality_code
            0 1975-02-18 08:00:00   750.396435             3
            1 1975-02-19 08:00:00   750.396435             3
            2 1975-02-20 08:00:00  1403.666086             3
            3 1975-02-21 08:00:00  1613.210750             0
            4 1975-02-22 08:00:00  1765.272217             0
        ```

        """

        mn, mx = self.get_extents(
            p_cwms_ts_id=p_cwms_ts_id,
            p_time_zone=p_timezone,
            version_date=version_date,
            p_office_id=p_office_id,
        )

        # To get a little overlap
        mn = mn - datetime.timedelta(days=1)
        mx = mx + datetime.timedelta(days=1)

        start_time = mn.strftime("%Y/%m/%d")
        end_time = mx.strftime("%Y/%m/%d")

        por = self.retrieve_ts(
            p_cwms_ts_id,
            start_time,
            end_time,
            p_units=p_units,
            p_timezone=p_timezone,
            p_trim="F",
            p_start_inclusive=p_start_inclusive,
            p_end_inclusive=p_end_inclusive,
            p_previous=p_previous,
            p_next=p_next,
            version_date=version_date,
            p_max_version=p_max_version,
            p_office_id=p_office_id,
            return_df=return_df,
            local_tz=local_tz,
        )

        return por

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

    def compare_ts(
        self,
        p_cwms_ts_id_list,
        p_units_list=None,
        p_timezone="UTC",
        p_trim="F",
        p_start_inclusive="T",
        p_end_inclusive="T",
        p_previous="T",
        p_next="F",
        version_date="1111/11/11",
        p_max_version="T",
        p_office_id=None,
        local_tz=False,
        only_diffs=True,
    ):
        """
        Compares values across list of time series identifiers.

        Parameters
        ----------
        p_cwms_ts_id_list : list
            List of time series identifiers.
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
        only_diffs : bool
            Return only differences in timestamp values (the default is True).

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
        >>> p_cwms_ts_id_list = ['Some.Fully.Qualified.Cwms.Ts.ID-RAW',
        >>>                     'Some.Fully.Qualified.Cwms.Ts.ID-REV']
        >>> df = cwms.compare_ts(p_cwms_ts_id_list)
        >>> df.head()

                            Some.Fully.Qualified.Cwms.Ts.ID-RAW	Some.Fully.Qualified.Cwms.Ts.ID-REV
                            value	quality_code	value	quality_code
            date_time
            1961-06-07 23:00:00	13130.521765	0.0	12852.387405	3.0
            1961-06-08 23:00:00	13521.294248	0.0	12831.936349	3.0
            1961-06-09 23:00:00	13980.027162	0.0	12811.485293	3.0
            1961-06-10 23:00:00	14181.076773	0.0	12791.034237	3.0
            1961-06-11 23:00:00	14056.482648	0.0	12770.583181	3.0
        ```
        """
        df_list = []
        for idx, p_cwms_ts_id in enumerate(p_cwms_ts_id_list):
            if p_units_list:
                p_units = p_units_list[idx]
            else:
                p_units = None
            df = self.get_por(
                p_cwms_ts_id,
                p_units=p_units,
                p_timezone=p_timezone,
                p_trim=p_trim,
                p_start_inclusive=p_start_inclusive,
                p_end_inclusive=p_end_inclusive,
                p_previous=p_previous,
                p_next=p_next,
                version_date=version_date,
                p_max_version=p_max_version,
                p_office_id=p_office_id,
                return_df=True,
                local_tz=local_tz,
            )
            df.set_index("date_time", inplace=True)
            df_list.append(df)

        # reference: https://stackoverflow.com/a/47112033/4296857
        comp = pd.concat(df_list, axis="columns", keys=p_cwms_ts_id_list)
        if only_diffs:
            df_list = []
            # np.isclose only accepts 2 arrays, getting a combination of all
            # possible arrays to compare
            comb = combinations(p_cwms_ts_id_list, 2)
            for i in comb:
                a, b = i
                bol = pd.DataFrame(
                    np.isclose(comp[a]["value"].values, comp[b]["value"].values)
                    == False
                )
                df_list.append(bol)
            bol = pd.concat(df_list, axis=1).apply(
                lambda row: True in row.values, axis=1
            )
            comp = comp[bol.values]
        return comp

    @LD
    def update_ts_id(
        self,
        p_cwms_ts_id,
        p_interval_utc_offset=None,
        p_snap_forward_minutes=None,
        p_snap_backward_minutes=None,
        p_local_reg_time_zone_id=None,
        p_ts_active_flag=None,
        p_db_officeid=None,
    ):

        cur = self.conn.cursor()
        try:

            cur.callproc(
                "cwms_ts.update_ts_id",
                [
                    p_cwms_ts_id,
                    p_interval_utc_offset,
                    p_snap_forward_minutes,
                    p_snap_backward_minutes,
                    p_local_reg_time_zone_id,
                    p_ts_active_flag,
                    p_db_officeid,
                ],
            )
        except Exception as e:
            LOGGER.error("Error in update_ts_id.")
            cur.close()
            raise ValueError(e)
        cur.close()
        return True
