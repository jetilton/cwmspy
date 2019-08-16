# -*- coding: utf-8 -*-
import os
from cwmspy import CWMS 
import pandas as pd
import numpy as np

class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))
    cwms = CWMS()
    cwms.connect()
    def test_one(self):
        """
        get_ts_code: Testing successful delete_window
        """
        
        #adding fake location and data to test with
        df = self.cwms.retrieve_ts('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                  '2019/1/1', '2019/9/1', 'cms',df=True)
        if not self.cwms.retrieve_location('TST'):
            self.cwms.store_location("TST")
        p_cwms_ts_id='TST.Flow-Out.Ave.~1Day.1Day.CBT-REV'
        p_units='cms'
        values = list(df['value'])
        p_qualities = list(df['quality_code'])
        times = list(df['date_time'])
    
        self.cwms.store_ts(p_cwms_ts_id, p_units, times, values, p_qualities)
        df2 = self.cwms.retrieve_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                    '2018/1/1', '2019/9/1', 'cms',df=True)
    
        start_time = '2018/1/1'
        end_time = '2019/2/1'
        self.cwms.delete_ts_window(p_cwms_ts_id, start_time, end_time,
                  p_override_protection='F', p_version_date=None,
                  p_db_office_code=26)
        
        df3 = self.cwms.retrieve_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                    '2018/1/1', '2019/9/1', 'cms',df=True)
        assert df3.set_index('date_time')\
                .equals(df2.set_index('date_time').loc['2019-02-01':])
        
        
        self.cwms.delete_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV', 'DELETE TS DATA')
        self.cwms.delete_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV', 'DELETE TS ID')
        self.cwms.delete_location("TST")
        
    def test_two(self):
        """
        get_extents: Testing successful get_extents
        """
        
        min_date = self.cwms.get_ts_min_date('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV')
        max_date = self.cwms.get_ts_max_date('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV')
        mn,mx = self.cwms.get_extents('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV')
        assert mn == min_date
        assert mx == max_date
        
        
    def test_three(self):
        """
        get_por: Testing successful get_por 
        """
        
        l = self.cwms.get_por('ALF.Elev-Forebay.Ave.~1Day.1Day.CBT-RAW',df=False)
        assert isinstance(l, list)
        assert np.floor(l[0][1]+1) == 626
        
        df = self.cwms.get_por('ALF.Elev-Forebay.Ave.~1Day.1Day.CBT-RAW',df=True)
        assert isinstance(df, pd.core.frame.DataFrame)
        assert np.floor(df['value'].values[0]+1) == 626
        assert df.shape[0] >20000
            
        
                  
    def test_final(self):
        """
        close: Testing good close from db for cleanup
        """
        
        c = self.cwms.close()

        assert c == True

        