# -*- coding: utf-8 -*-

import os
from cwmspy import CWMS
import pytest


@pytest.fixture(params=["pm3", "pt7"])
def cwms(request):
    cwms = CWMS(verbose=True)
    name = request.param
    cwms.connect(name=name)
    yield cwms
    try:
        cwms.delete_location("CWMSPY", "DELETE TS DATA")
        cwms.delete_location("CWMSPY", "DELETE TS ID")
        cwms.delete_location("CWMSPY")
    except:
        pass
    cwms.close()


class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))

    def test_one(self, cwms):
        """
        store_location delete_location: Testing successful store_location
        """
        cwms.store_location("CWMSPY")

        cur = cwms.conn.cursor()

        loc = cwms.retrieve_location("CWMSPY")
        cur.close()
        if loc["value"][0].upper() == "CWMSPY":
            assert cwms.delete_location("CWMSPY")
        else:
            print(loc)
            raise ValueError()

        cur = cwms.conn.cursor()
        sql = """
                select base_location_id from cwms_20.at_base_location
                where base_location_id = 'CWMSPY'
            """
        loc = cur.execute(sql).fetchall()
        cur.close()
        if loc:
            print(loc)
            raise ValueError()

