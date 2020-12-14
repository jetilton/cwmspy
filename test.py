============================= test session starts =============================
platform win32 -- Python 3.7.7, pytest-6.0.1, py-1.9.0, pluggy-0.13.1
rootdir: D:\gitClones\cwmspy
collected 30 items / 27 deselected / 3 selected
run-last-failure: rerun previous 3 failures

test\test_cwms_ts.py FF.                                                 [100%]

================================== FAILURES ===================================
______________ TestClass.test_retrieve_time_series[pt7-cms-UTC] _______________

self = <cwmspy.core.CWMS object at 0x000002269CDC8088>
ts_ids = ['CWMSPY.Flow.Inst.0.0.REV'], units = ['cms'], p_datums = None
p_start = '2015-12-01', p_end = '2020-01-03', p_timezone = 'UTC'
p_office_id = None, as_json = False

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
        if p_start:
            p_start = pd.to_datetime(p_start).strftime("%Y-%m-%d")
        if p_end:
            # add one day to make it inclusive to 24:00
            p_end = (pd.to_datetime(p_end) + datetime.timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
    
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
>                   p_office_id,
                ],
            )
E           cx_Oracle.DatabaseError: ORA-24247: network access denied by access control list (ACL)
E           ORA-06512: at "SYS.UTL_INADDR", line 4
E           ORA-06512: at "SYS.UTL_INADDR", line 35
E           ORA-06512: at "CWMS_20.CWMS_TS", line 13434
E           ORA-06512: at line 1

cwmspy\cwms_ts.py:485: DatabaseError

During handling of the above exception, another exception occurred:

self = <test.test_cwms_ts.TestClass object at 0x000002269CDBAF08>, name = 'pt7'
units = 'cms', tz = 'UTC'
cwms_data = (<cwmspy.core.CWMS object at 0x000002269CDC8088>, ['2016/12/31', '2017/01/01', '2017/01/02', '2017/01/03', '2017/01/04...68256817, 0.1411200080598672, -0.7568024953079282, -0.9589242746631385, ...], 'CWMSPY.Flow.Inst.0.0.REV', 'cms', 'UTC')

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
>           as_json=False,
        )

test\test_cwms_ts.py:147: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
cwmspy\utils.py:11: in wrapper
    out = function(*args, **kwargs)
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

self = <cwmspy.core.CWMS object at 0x000002269CDC8088>
ts_ids = ['CWMSPY.Flow.Inst.0.0.REV'], units = ['cms'], p_datums = None
p_start = '2015-12-01', p_end = '2020-01-03', p_timezone = 'UTC'
p_office_id = None, as_json = False

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
        if p_start:
            p_start = pd.to_datetime(p_start).strftime("%Y-%m-%d")
        if p_end:
            # add one day to make it inclusive to 24:00
            p_end = (pd.to_datetime(p_end) + datetime.timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
    
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
                    p_office_id,
                ],
            )
    
        except Exception as e:
            LOGGER.error("Error in retrieving time series")
            cur.close()
>           raise ValueError(e.__str__())
E           ValueError: ORA-24247: network access denied by access control list (ACL)
E           ORA-06512: at "SYS.UTL_INADDR", line 4
E           ORA-06512: at "SYS.UTL_INADDR", line 35
E           ORA-06512: at "CWMS_20.CWMS_TS", line 13434
E           ORA-06512: at line 1

cwmspy\cwms_ts.py:492: ValueError
----------------------------- Captured log setup ------------------------------
ERROR    cwmspy.cwms_loc:cwms_loc.py:193 ORA-20025: LOCATION_ID_NOT_FOUND: The Location: "CWMSPY" does not exist.
ORA-06512: at "CWMS_20.CWMS_ERR", line 59
ORA-06512: at "CWMS_20.CWMS_LOC", line 2523
ORA-01403: no data found
ORA-06512: at "CWMS_20.CWMS_LOC", line 2515
ORA-06512: at line 1
------------------------------ Captured log call ------------------------------
ERROR    cwmspy.cwms_ts:cwms_ts.py:490 Error in retrieving time series
___________ TestClass.test_retrieve_time_series[pt7-cfs-US/Pacific] ___________

self = <cwmspy.core.CWMS object at 0x000002269CF24288>
ts_ids = ['CWMSPY.Flow.Inst.0.0.REV'], units = ['cfs'], p_datums = None
p_start = '2015-12-01', p_end = '2020-01-03', p_timezone = 'US/Pacific'
p_office_id = None, as_json = False

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
        if p_start:
            p_start = pd.to_datetime(p_start).strftime("%Y-%m-%d")
        if p_end:
            # add one day to make it inclusive to 24:00
            p_end = (pd.to_datetime(p_end) + datetime.timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
    
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
>                   p_office_id,
                ],
            )
E           cx_Oracle.DatabaseError: ORA-24247: network access denied by access control list (ACL)
E           ORA-06512: at "SYS.UTL_INADDR", line 4
E           ORA-06512: at "SYS.UTL_INADDR", line 35
E           ORA-06512: at "CWMS_20.CWMS_TS", line 13434
E           ORA-06512: at line 1

cwmspy\cwms_ts.py:485: DatabaseError

During handling of the above exception, another exception occurred:

self = <test.test_cwms_ts.TestClass object at 0x000002269CF22548>, name = 'pt7'
units = 'cfs', tz = 'US/Pacific'
cwms_data = (<cwmspy.core.CWMS object at 0x000002269CF24288>, ['2016/12/31', '2017/01/01', '2017/01/02', '2017/01/03', '2017/01/04...7, 0.1411200080598672, -0.7568024953079282, -0.9589242746631385, ...], 'CWMSPY.Flow.Inst.0.0.REV', 'cfs', 'US/Pacific')

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
>           as_json=False,
        )

test\test_cwms_ts.py:147: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
cwmspy\utils.py:11: in wrapper
    out = function(*args, **kwargs)
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

self = <cwmspy.core.CWMS object at 0x000002269CF24288>
ts_ids = ['CWMSPY.Flow.Inst.0.0.REV'], units = ['cfs'], p_datums = None
p_start = '2015-12-01', p_end = '2020-01-03', p_timezone = 'US/Pacific'
p_office_id = None, as_json = False

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
        if p_start:
            p_start = pd.to_datetime(p_start).strftime("%Y-%m-%d")
        if p_end:
            # add one day to make it inclusive to 24:00
            p_end = (pd.to_datetime(p_end) + datetime.timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
    
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
                    p_office_id,
                ],
            )
    
        except Exception as e:
            LOGGER.error("Error in retrieving time series")
            cur.close()
>           raise ValueError(e.__str__())
E           ValueError: ORA-24247: network access denied by access control list (ACL)
E           ORA-06512: at "SYS.UTL_INADDR", line 4
E           ORA-06512: at "SYS.UTL_INADDR", line 35
E           ORA-06512: at "CWMS_20.CWMS_TS", line 13434
E           ORA-06512: at line 1

cwmspy\cwms_ts.py:492: ValueError
----------------------------- Captured log setup ------------------------------
ERROR    cwmspy.cwms_loc:cwms_loc.py:193 ORA-20025: LOCATION_ID_NOT_FOUND: The Location: "CWMSPY" does not exist.
ORA-06512: at "CWMS_20.CWMS_ERR", line 59
ORA-06512: at "CWMS_20.CWMS_LOC", line 2523
ORA-01403: no data found
ORA-06512: at "CWMS_20.CWMS_LOC", line 2515
ORA-06512: at line 1
------------------------------ Captured log call ------------------------------
ERROR    cwmspy.cwms_ts:cwms_ts.py:490 Error in retrieving time series
=========================== short test summary info ===========================
FAILED test/test_cwms_ts.py::TestClass::test_retrieve_time_series[pt7-cms-UTC]
FAILED test/test_cwms_ts.py::TestClass::test_retrieve_time_series[pt7-cfs-US/Pacific]
================= 2 failed, 1 passed, 27 deselected in 11.69s =================
