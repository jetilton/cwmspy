# -*- coding: utf-8 -*-
import datetime

class Extra:
    def delete_ts_window(self, p_cwms_ts_id, start_time, end_time,
                  p_override_protection='F', p_version_date=None,
                  p_db_office_code=26):
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
        import CWMS
        
        cwms = CWMS()
        cwms.connect()
        start_time = '2018/1/1'
        end_time = '2019/2/1'
        p_cwms_ts_id = 'your.cwms.ts.id'
        
        cwms.delete_ts_window(p_cwms_ts_id, start_time, end_time,
                              p_override_protection='F', p_version_date=None,
                              p_db_office_code=26)
        >>>True

        """

        p_start_time = datetime.datetime.strptime(start_time,
                                                           "%Y/%m/%d")
        p_end_time = datetime.datetime.strptime(end_time,
                                                           "%Y/%m/%d")

        alter_session_sql = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
        cur = self.conn.cursor()
        cur.execute(alter_session_sql)

        delete_sql ="""
                    delete from cwms_20.at_tsv_{}
                    where ts_code = {}
                    and date_time between to_date('{}') and to_date('{}')
                    """
        if not p_override_protection:
            delete_sql += """
                    and  quality_code not in (select quality_code from
                    cwms_20.cwms_data_quality where validity_id = 'PROTECTED')
                    """
        

        ts_code = self.get_ts_code(p_cwms_ts_id=p_cwms_ts_id,
                                   p_db_office_code=p_db_office_code)


        try:
            for year in range(p_start_time.year,(p_end_time.year+1)):
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

    def get_extents(self, p_cwms_ts_id, p_time_zone='UTC',
                       version_date= '1111/11/11', p_office_id=None):
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
        import CWMS
        
        cwms = CWMS()
        cwms.connect()
        
        cwms.get_extents('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV')
        >>>(datetime.datetime(1975, 2, 18, 8, 0), datetime.datetime(2019, 8, 16, 7, 0))

        """

        min_date = self.get_ts_min_date(p_cwms_ts_id, p_time_zone=p_time_zone,
                       version_date=version_date, p_office_id=p_office_id)

        max_date = self.get_ts_max_date(p_cwms_ts_id, p_time_zone=p_time_zone,
                       version_date=version_date, p_office_id=p_office_id)

        return min_date, max_date


    def get_por(self, p_cwms_ts_id, p_units=None, p_timezone='UTC', p_trim='F',
                    p_start_inclusive='T', p_end_inclusive='T',
                    p_previous='T', p_next='F', version_date='1111/11/11',
                    p_max_version='T', p_office_id=None, df=True,
                    local_tz=False):
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
        from cwmspy.core import CWMS 
        cwms = CWMS()
        cwms.connect()
        df = cwms.get_por('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV')
        df.head()
        >>>            date_time        value  quality_code
           0 1975-02-18 08:00:00   750.396435             3
           1 1975-02-19 08:00:00   750.396435             3
           2 1975-02-20 08:00:00  1403.666086             3
           3 1975-02-21 08:00:00  1613.210750             0
           4 1975-02-22 08:00:00  1765.272217             0

        """

        p_version_date = datetime.datetime.strptime(version_date,
                                                       "%Y/%m/%d")
        mn, mx =  self.get_extents(p_cwms_ts_id=p_cwms_ts_id,
                                                     p_time_zone=p_timezone,
                                                     version_date=version_date,
                                                     p_office_id=p_office_id)

        #To get a little overlap
        mn = mn - datetime.timedelta(days=1)
        mx = mx + datetime.timedelta(days=1)

        start_time = mn.strftime("%Y/%m/%d")
        end_time = mx.strftime("%Y/%m/%d")

        return self.retrieve_ts(p_cwms_ts_id, start_time,
                        end_time, p_units=p_units, p_timezone=p_timezone,
                        p_trim='F', p_start_inclusive=p_start_inclusive,
                        p_end_inclusive=p_end_inclusive, p_previous=p_previous,
                        p_next=p_next, p_version_date=p_version_date,
                        p_max_version=p_max_version, p_office_id=p_office_id,
                        df=df, local_tz=local_tz)
