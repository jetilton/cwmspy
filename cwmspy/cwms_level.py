# -*- coding: utf-8 -*-
"""
Facilities for working with location levels.
"""
import datetime
from datetime import timedelta
import json
from json import JSONDecodeError

import cx_Oracle
import pandas as pd
import logging


from .utils import log_decorator


LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)


class CwmsLevelMixin:
    @LD
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
        >>> p_location_level_id = 'Some.Fully.Qualified.Pathname'
        >>> p_level_units = 'cms'
        >>> p_start_time = '01/01/2000'
        >>> p_end_time = '05/01/2000'
        >>> df = cwms.retrieve_location_level_values(p_location_level_id=p_location_level_id,
        >>>                     p_start_time=p_start_time,
        >>>                     p_end_time=p_end_time,
        >>>                     p_level_units=p_level_units
        >>>                     )
        ```

        """
        p_start_time = pd.to_datetime(p_start_time).to_pydatetime().strftime("%Y-%m-%d")
        p_end_time = pd.to_datetime(p_end_time).to_pydatetime().strftime("%Y-%m-%d")

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

            LOGGER.info("Start retrieve_location_level_values.")
            cur.execute(
                """
                select * from table( cwms_level.retrieve_location_level_values(
                p_location_level_id =>:p_location_level_id,
                p_level_units       =>:p_level_units,
                p_start_time        =>to_date( :p_start_time, 'yyyy-mm-dd' ),
                p_end_time          =>to_date( :p_end_time, 'yyyy-mm-dd' ),
                p_timezone_id       =>:p_timezone_id,
                p_office_id         =>:p_office_id ) )""",
                bind_vars,
            )
            records = cur.fetchall()
            cur.close()
        except Exception as e:
            LOGGER.error("Error in retrieve_location_level_values.")
            cur.close()
            # print bind_vars
            raise ValueError(e.__str__())
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
        LOGGER.info(f"Found {records} records.")
        if df:
            result = pd.DataFrame(result)
            result.columns = ["date", "value", "quality_code"]
            result["location_level_id"] = p_location_level_id
            if p_level_units:
                result["units"] = p_level_units
        LOGGER.info("End retrieve_location_level_values.")
        return result

    @LD
    def retrieve_location_levels(
        self,
        p_names=None,
        p_format=None,
        p_units=None,
        p_datums=None,
        p_start=None,
        p_end=None,
        p_timezone="UTC",
        p_office_id=None,
        as_json=False,
    ):

        p_names = "|".join(p_names)
        if p_units:
            p_units = "|".join(p_units)

        cur = self.conn.cursor()

        p_results = cur.var(cx_Oracle.CLOB)
        p_date_time = cur.var(cx_Oracle.DATETIME)
        p_query_time = cur.var(int)
        p_format_time = cur.var(int)
        p_count = cur.var(int)

        if p_start:
            p_start = pd.to_datetime(p_start).strftime("%Y-%m-%d")
        if p_end:
            # add one day to make it inclusive to 24:00
            p_end = (pd.to_datetime(p_end) + datetime.timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )

        try:

            clob = cur.callproc(
                "cwms_level.retrieve_location_levels",
                [
                    p_results,
                    p_date_time,
                    p_query_time,
                    p_format_time,
                    p_count,
                    p_names,
                    p_format,
                    p_units,
                    p_datums,
                    p_start,
                    p_end,
                    p_timezone,
                    p_office_id,
                ],
            )

        except Exception as e:
            LOGGER.error("Error in retrieving time series")
            cur.close()
            raise ValueError(e)
        cur.close()
        try:
            result = json.loads(clob[0].read())
            if as_json:
                return result
        except JSONDecodeError as e:
            LOGGER.info("No data for the requested pathnames and dates.")
            raise e

        try:
            levels = result["location-levels"]["location-levels"]
        except KeyError:
            LOGGER.warning("No data found")
            return pd.DataFrame()

        df_list = []
        for data in levels:
            location_level_id = data["name"]
            parameter = data["values"]["parameter"]
            segments = data["values"]["segments"]
            df_l = []
            for segment in segments:
                interpolate = eval(segment["interpolate"].capitalize())

                values = segment["values"]

                df = pd.DataFrame(values, columns=["date_time", "value"])
                df["parameter"] = parameter
                df["interpolate"] = interpolate
                df.insert(0, "location_level_id", location_level_id)
                df_l.append(df)
            df = pd.concat(df_l)
            df_list.append(df)

        df = pd.concat(df_list)

        return df
