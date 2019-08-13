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