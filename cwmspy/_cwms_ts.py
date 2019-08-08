# -*- coding: utf-8 -*-
import cx_Oracle
from cx_Oracle import DatabaseError
import datetime
import pandas as pd
from dateutil import tz


class CWMS_TS:
    
    def _convert_to_local_time(self, date, timezone='UTC'):
        # reference: https://stackoverflow.com/a/4771733/4296857
        if date == None:
            return None
        from_zone = tz.gettz(timezone)
        utc = date.replace(tzinfo=from_zone)
        local = utc.astimezone(tz.tzlocal()).strftime('%Y-%m-%d %H:%M:%S')
        local = datetime.datetime.strptime(local, '%Y-%m-%d %H:%M:%S')
        return local

    def get_ts_code(self, p_cwms_ts_id, p_db_office_code=26):
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
            ts_code = cur.callfunc('cwms_ts.get_ts_code', cx_Oracle.STRING,
                                   [p_cwms_ts_id,p_db_office_code])
        except DatabaseError as e:
            cur.close()
            raise ValueError(e.__str__())

        cur.close()
        return ts_code

    def retrieve_ts(self, p_units, p_cwms_ts_id, start_time,
                    end_time, p_timezone='UTC', p_trim='F',
                    p_start_inclusive='F', p_end_inclusive='F',
                    p_previous='T', p_next='F', p_version_date=None,
                    p_max_version='T', p_office_id=None, df=True,
                    local_tz=False):
        """Retrieves time series data for a specified time series and
            time window.

        Parameters
        ----------
        p_units : str
            The unit to retrieve the data values in.
        p_cwms_ts_id : str
            The time series identifier to retrieve data for.
        p_start_time : str "%Y/%m/%d"
            The start time of the time window.
        p_end_time : str "%Y/%m/%d"
            The end time of the time window.
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

        p_start_time = datetime.datetime.strptime(start_time,
                                                       "%Y/%m/%d")
        p_end_time = datetime.datetime.strptime(end_time,
                                                       "%Y/%m/%d")

        cur = self.conn.cursor()
        p_at_tsv_rc = self.conn.cursor().var(cx_Oracle.CURSOR)
        try:
            cur.callproc('cwms_ts.retrieve_ts', [p_at_tsv_rc,
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
                                                 p_office_id])

        except DatabaseError as e:
            cur.close()
            raise ValueError(e.__str__())
        cur.close()
        
        output = [r for r in p_at_tsv_rc.getvalue()]
        
        if local_tz:
            for i,v in enumerate(output):
                date = v[0]
                local = self._convert_to_local_time(date=date, 
                                                    timezone=p_timezone)
                
                output[i] = [local] + [x for x in v[1:]] 

        if df:
            output = pd.DataFrame(output, columns=['date_time',
                                                   'value',
                                                   'quality_code'])

        return output
    
    
    def store_ts(self, p_cwms_ts_id, p_units, p_times, values, p_qualities, 
                 p_store_rule='REPLACE ALL', p_override_prot='F', 
                 p_office_id=None):
        
        cur = self.conn.cursor()
        
        p_values = cur.arrayvar(cx_Oracle.NATIVE_FLOAT, values)
        try:
            cur.callproc('cwms_ts.store_ts', [p_cwms_ts_id, 
                                              p_units, 
                                              p_times, 
                                              p_values, 
                                              p_qualities, 
                                              p_store_rule, 
                                              p_override_prot, 
                                              #p_versiondate, 
                                              p_office_id
                                              ])
        except DatabaseError as e:
            cur.close()
            raise ValueError(e.__str__())
        cur.close()
        return True
    
    def delete_ts_window(self, p_cwms_ts_id, start_time, end_time, 
                  p_start_time_inclusive='T', p_end_time_inclusive='T', 
                  p_override_protection='F', p_version_date=None, 
                  p_time_zone=None, p_date_times=None, p_max_version='T',
                  p_ts_item_mask=-1, p_db_office_id=None):
        
        p_start_time = datetime.datetime.strptime(start_time,
                                                       "%Y/%m/%d")
        p_end_time = datetime.datetime.strptime(end_time,
                                                       "%Y/%m/%d")
        
        cur = self.conn.cursor()
        try:
            cur.callproc('cwms_ts.delete_ts', [p_cwms_ts_id,
                                               p_override_protection,
                                               p_start_time,
                                               p_end_time,
                                               p_start_time_inclusive,
                                               p_end_time_inclusive,
                                               p_version_date,
                                               p_time_zone,
                                               #p_date_times,
                                               #p_max_version,
                                               #p_ts_item_mask,
                                               #p_db_office_id
                                              ])   
        except DatabaseError as e:
            cur.close()
            raise DatabaseError(e.__str__())
        cur.close()
        return True
    
    def delete_ts(self, p_cwms_ts_id, p_delete_action='DELETE TS ID',  
                  p_db_office_id=None):
    
        
        cur = self.conn.cursor()
        try:
            cur.callproc('cwms_ts.delete_ts', [p_cwms_ts_id,
                                               p_delete_action,
                                               p_db_office_id
                                              ])   
        except DatabaseError as e:
            cur.close()
            raise DatabaseError(e.__str__())
        cur.close()
        return True
        
                
        

        
