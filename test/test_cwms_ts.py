# -*- coding: utf-8 -*-
import os
from cwmspy import CWMS 
import datetime
import pytz
import numpy as np
import pandas as pd

class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))
    cwms = CWMS()
    cwms.connect()
    def test_one(self):
        """
        get_ts_code: Testing successful ts code
        """
        ts_code = self.cwms.get_ts_code("ALF.Elev-Forebay.Ave.~1Day.1Day.CBT-RAW")
        
        assert ts_code == '23318028'
        
    def test_three(self):
        """
        get_ts_code: Testing unsuccessful ts code bc of bad p_cwms_ts_id
        """
        try:
            self.cwms.get_ts_code("Not a code")
        except ValueError as e:
            msg = 'ORA-20001: TS_ID_NOT_FOUND: The timeseries identifier "Not a code"'
            assert msg in e.__repr__()
            
    def test_four(self):
        """
        get_ts_code: Testing unsuccessful ts code bc of bad p_db_office_code
        """
        
        try:
            self.cwms.get_ts_code("ALF.Elev-Forebay.Ave.~1Day.1Day.CBT-RAW", 35)
        except ValueError as e:
            msg = 'ORA-20001: TS_ID_NOT_FOUND: The timeseries identifier "ALF.Elev-Forebay.Ave.~1Day.1Day.CBT-RAW" was not found for office "POH"'
            assert msg in e.__repr__()
            
    def test_five(self):
        """
        retrieve_ts: Testing successful retrieve_ts 
        """
        
        l = self.cwms.retrieve_ts('TDA.Flow-Spill.Ave.1Hour.1Hour.CBT-RAW',  '2019/1/1', '2019/9/1', df=False)
        assert isinstance(l, list)
        assert np.floor(l[1600][1]+1) == 40
        
        df = self.cwms.retrieve_ts('TDA.Flow-Spill.Ave.1Hour.1Hour.CBT-RAW',  '2019/1/1', '2019/9/1')
        assert isinstance(df, pd.core.frame.DataFrame)
        assert np.floor(df['value'].values[1600]+1) == 40
        
    def test_six(self):
        """
        retrieve_ts: Testing unsuccessful retrieve_ts 
        """
        
        try:
            self.cwms.retrieve_ts('this is not valid',  '2019/1/1', '2019/9/1', 'cms',df=False)
        except Exception as e:
            msg = 'ORA-06502: PL/SQL: numeric or value error\nORA-06512: at "CWMS_20.CWMS_TS"'
            assert msg in e.__str__()
            
    def test_seven(self):
        """
        retrieve_ts: Testing convert to local_tz
        the LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV pathname inputs data at 12:00
        local time, so it should only have 1 hour when on local, but 2 hours
        when utc
        """
        
        
        df = self.cwms.retrieve_ts('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                  '2019/1/1', '2019/9/1','cms', df=True, local_tz=True)
        
        assert len(set([x.hour for x in df['date_time']])) == 1
        
        df = self.cwms.retrieve_ts('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                  '2019/1/1', '2019/9/1', 'cms', df=True, local_tz=False)
        
        assert len(set([x.hour for x in df['date_time']])) == 2

    def test_eight(self):
        """
        store_ts: Testing store_ts and delete_ts
        """
        
        
        df = self.cwms.retrieve_ts('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                  '2019/1/1', '2019/9/1', 'cms', df=True)
        if not self.cwms.retrieve_location('TST'):
            self.cwms.store_location("TST")
        p_cwms_ts_id='TST.Flow-Out.Ave.~1Day.1Day.CBT-REV'
        p_units='cms'
        values = list(df['value'])
        p_qualities = list(df['quality_code'])
        times = list(df['date_time'])
    
        self.cwms.store_ts(p_cwms_ts_id, p_units, times, values, p_qualities)
        df2 = self.cwms.retrieve_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                    '2019/1/1', '2019/9/1', 'cms', df=True)
    
        
        assert df.equals(df2)
        
        self.cwms.delete_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV', 'DELETE TS DATA')
        self.cwms.delete_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV', 'DELETE TS ID')
        self.cwms.delete_location("TST")
        try:
            df2 = self.cwms.retrieve_ts('TST.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                        '2019/1/1', '2019/9/1', 'cms', df=True)
        except ValueError as e:
            msg = 'TS_ID_NOT_FOUND: The timeseries identifier "TST.Flow-Out.Ave.~1Day.1Day.CBT-REV"'
            assert msg in e.__str__()
            
    def test_nine(self):
        """
        store_ts: Testing rename_ts
        """
        
        
        df = self.cwms.retrieve_ts('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                  '2019/1/1', '2019/9/1', 'cms', df=True)
        if not self.cwms.retrieve_location('TST'):
            self.cwms.store_location("TST")
        p_cwms_ts_id_old = 'TST.Flow-Out.Ave.~1Day.1Day.CBT-REV'
        p_cwms_ts_id_new = 'TST.Flow-In.Ave.~1Day.1Day.CBT-REV'
        p_units='cms'
        values = list(df['value'])
        p_qualities = list(df['quality_code'])
        times = list(df['date_time'])
    
        self.cwms.store_ts(p_cwms_ts_id_old, p_units, times, values, p_qualities)
        df2 = self.cwms.retrieve_ts(p_cwms_ts_id_old,  
                                    '2019/1/1', '2019/9/1', 'cms',df=True)
    
        
        assert df.equals(df2)
        self.cwms.rename_ts(p_cwms_ts_id_old, p_cwms_ts_id_new,
                  p_utc_offset_new=None, p_office_id=None)
        
        df3 = self.cwms.retrieve_ts(p_cwms_ts_id_new,  
                                    '2019/1/1', '2019/9/1', 'cms', df=True)
        
        assert df2.equals(df3)
        
        self.cwms.delete_ts(p_cwms_ts_id_new, 'DELETE TS DATA')
        self.cwms.delete_ts(p_cwms_ts_id_new, 'DELETE TS ID')
        self.cwms.delete_location("TST")
        
    def test_ten(self):
        """
        get_max_date/get_min_date: Testing for successful min/max dates
        """
        
        min_date = self.cwms.get_ts_min_date('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV')
        max_date = self.cwms.get_ts_max_date('LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV')
        
        assert isinstance(min_date, datetime.datetime)
        assert isinstance(max_date, datetime.datetime)
        
        
            
    def test_final(self):
        """
        close: Testing good close from db for cleanup
        """
        
        c = self.cwms.close()

        assert c == True

        