import functools
from functools import wraps
import logging


def log_decorator(logger):
    def real_decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            name = function.__name__
            logger.debug(f"Start {name}")
            out = function(*args, **kwargs)
            logger.debug(f"End {name}")
            return out

        return wrapper

    return real_decorator


class CwmsTsUtils():
    LOGGER = logging.getLogger(__name__)
    LD = log_decorator(LOGGER)

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