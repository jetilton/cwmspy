# -*- coding: utf-8 -*-

import os
from cwmspy import CWMS


class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))
    cwms = CWMS()
    cwms.connect()

    def test_one(self):
        """
        store_location delete_location: Testing successful store_location
        """
        self.cwms.store_location("TST")

        cur = self.cwms.conn.cursor()

        loc = self.cwms.retrieve_location("TST")[0][2]
        cur.close()
        if "TST" == loc:
            assert self.cwms.delete_location("TST")
        else:
            print(loc)
            raise ValueError()

        cur = self.cwms.conn.cursor()
        sql = """
                select base_location_id from cwms_20.at_base_location
                where base_location_id = 'TST'
            """
        loc = cur.execute(sql).fetchall()
        cur.close()
        if loc:
            raise ValueError()

    def test_final(self):
        """
        close: Testing good close from db for cleanup
        """

        c = self.cwms.close()

        assert c == True
