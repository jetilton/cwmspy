# -*- coding: utf-8 -*-
"""
Facilities for working with users in the CWMS database
"""
import argparse
import sys
import os
import getpass
import re
import subprocess
import logging
import pandas as pd
from .utils import log_decorator


LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)


class CwmsSecMixin:
    @LD 
    def cat_locked_users_tab(self, return_df=True):
        """Retrieves list or a pandas dataframe of locked users

        Parameters
        ----------
        return_df : bool
            Return result as pandas df vs the default str
        Returns
        -------
        list or pandas df
            USERNAME', 'ACCOUNT_STATUS', 'LOCK_DATE', 'EXPIRY_DATE'
        """
        cur = self.conn.cursor()
        sql = "select * from table(cwms_sec.cat_locked_users_tab)"

        try:
            locked_users = cur.execute(sql).fetchall()
        except ValueError as e:
            LOGGER.error("Error in retrieve_locked_users.")
            cur.close()
            raise ValueError(e)
        cur.close()
        if (return_df):
            df = pd.DataFrame(locked_users)
            df.columns = ['USERNAME', 'ACCOUNT_STATUS', 'LOCK_DATE', 'EXPIRY_DATE']
            return df
        return locked_users


    @LD 
    def unlock_db_account(self, p_username):
        """Unlocks a users account
        
        Parameters
        ----------
        p_username : str
            A user id of account to be unlocked

        p_db_office_id : str
            Office id
        Returns
        -------
        Returns True if operation was successful
        """
        cur = self.conn.cursor()
        try:
            cur.callproc('cwms_sec.unlock_db_account', [p_username])
            LOGGER.info(f'Account {p_username} is unlocked successfully')
        except ValueError as e:
            LOGGER.error("Error in unlocking users.")
            cur.close()
            raise ValueError(e)
        cur.close()
        return True


    @LD 
    def lock_db_account(self, p_username):
        """lock a user accounts by id
        
        Parameters
        ----------
        p_username : str
            A user id of account to be locked

        p_db_office_id : str
            Office id
        Returns
        -------
        Returns True if operation was successful
        """
        cur = self.conn.cursor()
        try:
            cur.callproc('cwms_sec.lock_db_account', [p_username])
            LOGGER.info(f'Account {p_username} is locked successfully')
        except ValueError as e:
            LOGGER.error("Error in locking users.")
            cur.close()
            raise ValueError(e)
        cur.close()
        return True
