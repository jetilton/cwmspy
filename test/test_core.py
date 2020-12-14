# -*- coding: utf-8 -*-

import sys
import pytest
import os
import random
from cwmspy import CWMS
from dotenv import load_dotenv


@pytest.fixture()
def cwms():
    cwms = CWMS(verbose=True)
    yield cwms
    c = cwms.close()


class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))

    load_dotenv()
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    service_name = os.getenv("SERVICE_NAME")

    def test_one(self, cwms):
        """
        connect: Testing bad connection to db
        """

        c = cwms.connect(
            host="BAD_HOST",
            service_name="BAD_SERVICE_NAME",
            port=1521,
            user=self.user,
            password=self.password,
        )

        assert c == False

    def test_two(self, cwms):
        """
        connect: Testing good connection to db
        """

        c = cwms.connect(
            host=self.host,
            service_name=self.service_name,
            port=1521,
            user=self.user,
            password=self.password,
        )

        assert c == True

    def test_connect_using_name(self, cwms):
        """
        connect: Testing good connection to db
        """

        c = cwms.connect(name="pt7")

        assert c == True

