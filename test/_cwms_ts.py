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
        
        l = self.cwms.retrieve_ts('cms','TDA.Flow-Spill.Ave.1Hour.1Hour.CBT-RAW',  '2019/1/1', '2019/9/1', df=False)
        assert isinstance(l, list)
        assert np.floor(l[1600][1]+1) == 40
        
        df = self.cwms.retrieve_ts('cms','TDA.Flow-Spill.Ave.1Hour.1Hour.CBT-RAW',  '2019/1/1', '2019/9/1')
        assert isinstance(df, pd.core.frame.DataFrame)
        assert np.floor(df['value'].values[1600]+1) == 40
        
    def test_six(self):
        """
        retrieve_ts: Testing unsuccessful retrieve_ts 
        """
        
        try:
            self.cwms.retrieve_ts('cms','this is not valid',  '2019/1/1', '2019/9/1', df=False)
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
        
        
        df = self.cwms.retrieve_ts('cms','LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                  '2019/1/1', '2019/9/1', df=True, local_tz=True)
        
        assert len(set([x.hour for x in df['date_time']])) == 1
        
        df = self.cwms.retrieve_ts('cms','LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                  '2019/1/1', '2019/9/1', df=True, local_tz=False)
        
        assert len(set([x.hour for x in df['date_time']])) == 2

    def test_eight(self):
        """
        store_ts: Testing store_ts 
        """
        
        
        df = self.cwms.retrieve_ts('cms','LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                  '2019/1/1', '2019/9/1', df=True)
        if not self.cwms.retrieve_location('TST'):
            self.cwms.store_location("TST")
        p_cwms_ts_id='TST.Flow-Out.Ave.~1Day.1Day.CBT-REV'
        p_units='cms'
        values = list(df['value'])
        p_qualities = list(df['quality_code'])
        times = [x.tz_localize("UTC") for x in list(df['date_time'])]
        zero = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
        p_times = [((time - zero).total_seconds() * 1000) for time in times]
        self.cwms.store_ts(p_cwms_ts_id, p_units, p_times, values, p_qualities)
        df2 = self.cwms.retrieve_ts('cms','TST.Flow-Out.Ave.~1Day.1Day.CBT-REV',  
                                    '2019/1/1', '2019/9/1', df=True)
        
        assert df.equals(df2)
        
        
        
    def test_final(self):
        """
        close: Testing good close from db for cleanup
        """
        
        c = self.cwms.close()

        assert c == True

        