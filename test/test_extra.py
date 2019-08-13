# -*- coding: utf-8 -*-
import os
from cwmspy import CWMS 
import pandas as pd

class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))
    cwms = CWMS()
    cwms.connect()
    def test_one(self):
        """
        get_ts_code: Testing successful delete_window
        """
        df = self.cwms.retrieve_ts('cms','LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                  '2019/1/1', '2019/9/1', df=True)
        if not self.cwms.retrieve_location('TST'):
            self.cwms.store_location("TST")
        p_cwms_ts_id='TST.Flow-Out.Ave.~1Day.1Day.CBT-REV'
        p_units='cms'
        values = list(df['value'])
        p_qualities = list(df['quality_code'])
        times = list(df['date_time'])
    
        self.cwms.store_ts(p_cwms_ts_id, p_units, times, values, p_qualities)
        df2 = self.cwms.retrieve_ts('cms','TST.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                    '2018/1/1', '2019/9/1', df=True)
    
        start_time = '2018/1/1'
        end_time = '2019/2/1'
        self.cwms.delete_ts_window(p_cwms_ts_id, start_time, end_time,
                  p_override_protection='F', p_version_date=None,
                  p_db_office_code=26)
        
        df3 = self.cwms.retrieve_ts('cms','TST.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                    '2018/1/1', '2019/9/1', df=True)
        assert df3.set_index('date_time')\
                .equals(df2.set_index('date_time').loc['2019-02-01':])
        
        
        self.cwms.delete_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV', 'DELETE TS DATA')
        self.cwms.delete_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV', 'DELETE TS ID')
        self.cwms.delete_location("TST")
        
            
        
                  
    def test_final(self):
        """
        close: Testing good close from db for cleanup
        """
        
        c = self.cwms.close()

        assert c == True

        