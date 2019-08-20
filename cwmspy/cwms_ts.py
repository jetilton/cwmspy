# -*- coding: utf-8 -*-
import cx_Oracle
from cx_Oracle import DatabaseError
import datetime
import pandas as pd
from dateutil import tz
import pytz
from .connect import Connect

class CWMS_TS(Connect):
    def __init__(self,conn):
        self.conn = conn
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


        Args:
            p_cwms_ts_id: CWMS time series identifier
            p_db_office_code: The unique numeric code identifying the office
                               owning the time series

        Returns:
            ts_code: the unique numeric code value for the specified
                        time series if successful, False otherwise


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
        import CWMS

        cwms = CWMS()
        cwms.connect()

        cwms.get_ts_max_date('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV')
        >>>datetime.datetime(2019, 8, 16, 7, 0)

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
        import CWMS

        cwms = CWMS()
        cwms.connect()

        cwms.get_ts_min_date('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV')
        >>>datetime.datetime(1975, 2, 18, 8, 0)

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

        """

        cur = self.conn.cursor()

        p_values = cur.arrayvar(cx_Oracle.NATIVE_FLOAT, values)

        t = [x.tz_localize("UTC") for x in times]
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
    
    
