# -*- coding: utf-8 -*-

import sys
import pytest
import os
import random
from cwmspy import CWMS 
from dotenv import load_dotenv

class TestClass(object):
    cwd = os.path.dirname(os.path.realpath(__file__))
    cwms = CWMS()
    
    load_dotenv()
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    service_name = os.getenv("SERVICE_NAME")
    
    def test_one(self):
        """
        connect: Testing bad connection to db
        """
        
        
        c = self.cwms.connect(host="BAD_HOST", service_name="BAD_SERVICE_NAME", 
                              port=1521, user=self.user, 
                              password=self.password)

        assert c == False
        
    def test_two(self):
        """
        connect: Testing good connection to db
        """
        
        
        c = self.cwms.connect(host=self.host, service_name=self.service_name, 
                              port=1521, user=self.user, 
                              password=self.password)

        assert c == True
        
        
    def test_final(self):
        """
        close: Testing good close from db
        """
        
        c = self.cwms.close()

        assert c == True

