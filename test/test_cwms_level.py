import os
from cwmspy import CWMS
import datetime
import numpy as np
import pandas as pd


class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))
    cwms = CWMS()
    cwms.connect()

    def test_one(self):
        """
        
        """
        level = self.cwms.retrieve_location_level_values(
            "BCLO.Temp-Water.Inst.0.Fisheries Max Target",
            "01/01/2000",
            "05/01/2000",
            "F",
            df=False,
        )

        print(level)
        assert level

    def test_final(self):
        """
        close: Testing good close from db for cleanup
        """

        c = self.cwms.close()

        assert c == True

