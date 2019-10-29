# -*- coding: utf-8 -*-
"""
Facilities for working with location levels.
"""
import datetime
from datetime import timedelta
import cx_Oracle
from cx_Oracle import DatabaseError
import inspect
import pandas as pd
import logging
import sys

from .utils import LogDecorator

logger = logging.getLogger(__name__)
ld = LogDecorator(logger)


class CwmsLevelMixin:
    @ld
    def retrieve_location_level_values(
        self,
        p_location_level_id,
        p_start_time,
        p_end_time,
        p_level_units,
        p_timezone_id="GMT",
        p_office_id="NWDP",
        df=True,
    ):
        """
        
        Retrieves a time series of location level values for a specified location level and a time window.

        Parameters
        ----------
        p_location_level_id : str
            The location level identifier. Format is location.parameter.parameter_type.duration.specified_level
        p_start_time : str 
            The start of the time window.  Format : 'dd-mm-yyyy hh24mi'.
        p_end_time : str
            The end of the time window.  Format : 'dd-mm-yyyy hh24mi'.
        p_level_units : str
            The value unit to retrieve the level values in
        p_timezone_id : str
            The time zone of the time window. Retrieved dates are also in this time zone (the default is "GMT").
        p_office_id : str
            The office that owns the location level. If not specified or NULL, the session user's default office is used` (the default is "NWDP").
        df : boolean
            Return a pandas.Core.DataFrame (the default is True).

        Returns
        -------
        type: either list or pandas.Core.DataFrame
        The location level values. The time series contains values at the spcified start and end times of the time window and may contain values at intermediate times
            * If the level is constant, the time series will be of length 2 and the quality_codes of both elements will be zero
            * If the level varies in a recurring pattern, the time series will include values at any pattern breakpoints in the time window. The quality_codes of all elements will be zero
            * If the level varies irregularly, the time series will include values of at any times of the representing time series that are in the time window. The quality codes of times within the time window will be the quality codes of the representing time series. The quality codes of the elements at the beginning and end of the time window may be zero
        The quality code of each returned value will be one of the following
            * 0: The value for all times between the previous value time and this one is the same as the previous value
            * 1: The value for all times between the previous value time and this one is interpolated between the previous value and this one
                
        
        Examples
        -------
        ```python
        >>> from cwmspy import CWMS
        >>> cwms = CWMS()
        >>> cwms.connect()
            True
        >>> p_cwms_ts_id = 'Some.Fully.Qualified.Pathname'
        >>> p_level_units = 'cms'
        >>> p_start_time = '01/01/2000'
        >>> p_end_time = '05/01/2000'
        >>> df = cwms.retrieve_location_level_values(p_cwms_ts_id=p_cwms_ts_id,
        >>>                     p_start_time=p_start_time,
        >>>                     p_end_time=p_end_time,
        >>>                     p_level_units=p_level_units
        >>>                     )
        ```

        """

        try:
            cur = self.conn.cursor()

            bind_vars = {
                "p_location_level_id": p_location_level_id,
                "p_level_units": p_level_units,
                "p_start_time": p_start_time,
                "p_end_time": p_end_time,
                "p_timezone_id": p_timezone_id,
                "p_office_id": p_office_id,
            }

            logger.info("Start retrieve_location_level_values.")
            cur.execute(
                """
                select * from table( cwms_level.retrieve_location_level_values(
                p_location_level_id =>:p_location_level_id,
                p_level_units       =>:p_level_units,
                p_start_time        =>to_date( :p_start_time, 'dd-mm-yyyy hh24mi' ),
                p_end_time          =>to_date( :p_end_time, 'dd-mm-yyyy hh24mi' ),
                p_timezone_id       =>:p_timezone_id,
                p_office_id         =>:p_office_id ) )""",
                bind_vars,
            )
            records = cur.fetchall()
            cur.close()
        except Exception as e:
            logger.error(e)
            cur.close()
            # print bind_vars
            raise DatabaseError(e.__str__())
        result = []
        # The following code deals with the hacky location level API call that HEC
        # Implemented. The quality flag is an interpolation flag, meaning 0 is not
        # to be interopolated.
        lastVal = None
        for row in records:
            if row[2] == 0 and lastVal != None:
                result.append([row[0] - timedelta(minutes=1), lastVal, row[2]])
            result.append(row)
            lastVal = row[1]
        records = str(len(result))
        logger.info(f"Found {records} records.")
        if df:
            result = pd.DataFrame(result)
            result.columns = ["date", "value", "quality_code"]
        logger.info("End retrieve_location_level_values.")
        return result
