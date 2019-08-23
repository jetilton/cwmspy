# -*- coding: utf-8 -*-
"""
Facilities for working with time series
"""
import cx_Oracle
from cx_Oracle import DatabaseError
import datetime
import pandas as pd
from dateutil import tz
import pytz


class CwmsTsMixin:
    @staticmethod
    def _convert_to_local_time(date, timezone="UTC"):
        # reference: https://stackoverflow.com/a/4771733/4296857
        if date == None:
            return None
        from_zone = tz.gettz(timezone)
        utc = date.replace(tzinfo=from_zone)
        local = utc.astimezone(tz.tzlocal()).strftime("%Y-%m-%d %H:%M:%S")
        local = datetime.datetime.strptime(local, "%Y-%m-%d %H:%M:%S")
        return local

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
        except DatabaseError as e:
            cur.close()
            raise ValueError(e.__str__())

        cur.close()
        return ts_code

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
        except DatabaseError as e:
            cur.close()
            raise ValueError(e.__str__())

        cur.close()
        return max_date

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
        except DatabaseError as e:
            cur.close()
            raise ValueError(e.__str__())

        cur.close()
        return min_date

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
        df=True,
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
        df : bool
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
                                start_time='2019/1/1', end_time='2019/9/1', df=True)
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

        except DatabaseError as e:
            cur.close()
            raise ValueError(e.__str__())
        cur.close()

        output = [r for r in p_at_tsv_rc.getvalue()]

        if local_tz:
            for i, v in enumerate(output):
                date = v[0]
                local = self._convert_to_local_time(date=date, timezone=p_timezone)

                output[i] = [local] + [x for x in v[1:]]

        if df:
            output = pd.DataFrame(
                output, columns=["date_time", "value", "quality_code"]
            )

        return output

    def store_ts(
        self,
        p_cwms_ts_id,
        p_units,
        times,
        values,
        p_qualities,
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
        p_times : list
            The UTC times of the data values.
        values : list
            The data values.
        p_qualities : list
            The data quality codes for the data values.
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

        #t = [x.tz_localize("UTC") for x in times]
        zero = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
        p_times = [((time - zero).total_seconds() * 1000) for time in t]

        if not version_date:
            p_version_date = datetime.datetime(1111, 11, 11)
        else:
            p_version_date = version_date

        try:
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
        except DatabaseError as e:
            cur.close()
            raise ValueError(e.__str__())
        cur.close()
        return True

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
        except DatabaseError as e:
            cur.close()
            raise DatabaseError(e.__str__())
        cur.close()
        return True

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
        except DatabaseError as e:
            cur.close()
            raise DatabaseError(e.__str__())
        cur.close()
        return True

    def delete_ts_window(
        self,
        p_cwms_ts_id,
        start_time,
        end_time,
        p_override_protection="F",
        p_version_date=None,
        p_db_office_code=26,
    ):
        """Short summary.

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
            Description of parameter `p_office_id`.
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
                sql = delete_sql.format(table, ts_code, start_time, end_time)
                if p_version_date:
                    sql += "and version_date = to_date('{}')".format(p_version_date)

                cur.execute(sql)
                cur.execute("commit")
        except Exception as e:
            cur.execute("rollback")
            cur.close()
            raise Exception(e.__str__())
        cur.close()
        return True

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
        df=True,
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
        df : bool
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

        return self.retrieve_ts(
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
            df=df,
            local_tz=local_tz,
        )

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
        df=True,
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
        df : bool
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
                df,
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

            if df:
                rslt["ts_id"] = ts_id

            l.append(rslt)

        if df:
            l = pd.concat(l, ignore_index=True)
            l = l[["date_time", "ts_id", "value", "quality_code"]]
            if pivot:
                l = l.pivot(index="date_time", columns="ts_id", values="value")
        return l
