# -*- coding: utf-8 -*-
import datetime

class Extra:
    def delete_ts_window(self, p_cwms_ts_id, start_time, end_time,
                  p_override_protection='F', p_version_date=None,
                  p_db_office_code=26):

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
                    and  QUALITY_CODE not in (select quality_code from
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
            cur.close()
            raise Exception(e.__str__())
        cur.close()
        return True
    
    def get_extents(self, p_cwms_ts_id, p_time_zone='UTC',
                       version_date= '1111/11/11', p_office_id=None):
        
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
    
    
    
    