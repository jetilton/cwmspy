# -*- coding: utf-8 -*-
"""
Facilities for working with time series
"""
import cx_Oracle
import datetime
import pandas as pd
from dateutil import tz
import pytz
import logging
from itertools import combinations
import numpy as np
import json
from json import JSONDecodeError

from .utils import log_decorator
from .utils import CwmsTsUtils


LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)

class CwmsTsStore(CwmsTsUtils):

    @LD
    def store_by_df(
        self,
        df,
        timezone=None,
        p_store_rule="REPLACE ALL",
        p_override_prot="F",
        version_date=None,
        p_office_id=None,
        only_add_different=True,
    ):
        """Stores time series data to the database with pandas.core.dataframe as input.

        Parameters
        ----------
        df : pandas.core.DataFrame
            Pandas DataFrame that requires `ts_id`, `date_time`, `units`,
            and `value` columns.  If optional column `quality_code` does not exist,
            all quality codes are assumed equal to 0.
        p_store_rule : type
            The store rule to use.
        p_override_prot : str
            A flag ('T' or 'F') specifying whether to override the protection
            flag on any existing data value.
        p_version_date : datetime
            Description of parameter `p_office_id`.
        p_office_id : type
            The office owning the time series. If not specified or NULL, the
            session user's default office is used.
        only_add_different : boolean
            Check what is currently in database and only commit changes

        Returns
        -------
        Boolean
            `True` for success

        Examples
        -------
        ```python
        >>> from cwmspy import CWMS
        >>> import datetime
        >>> cwms = CWMS()
        >>> cwms.connect()
            True
        >>> p_cwms_ts_id = 'Some.Fully.Qualified.Pathname'
        >>> p_units = 'cms'
        >>> start_time = '2019/1/1'
        >>> end_time = '2019/8/1'
        >>> df = cwms.retrieve_ts(p_cwms_ts_id=p_cwms_ts_id,
        >>>                     start_time=start_time,
        >>>                     end_time=end_time,
        >>>                     p_units=p_units
        >>>                     )
        >>> df['units'] = p_units
        >>> df['ts_id'] = p_cwms_ts_id
        >>> df['value'] = df['value'] / 1.1
        >>> cwms.store_by_df(df)
            True
        ```

        """

        if "time_zone" not in df.columns and timezone:
            df["time_zone"] = timezone

        if "quality_code" in df.columns:
            df["quality_code"] = df["quality_code"].astype(int)
        else:
            df["quality_code"] = 0

        # I want to make sure everything is the right type for cx_Oracle
        df["date_time"] = pd.to_datetime(df["date_time"])
        df["date_time"] = df["date_time"].dt.tz_localize(None)
        df["value"] = df["value"].astype(float)

        grouped = df.groupby(["ts_id", "units", "time_zone"])
        for g, v in grouped:
            p_cwms_ts_id, p_units, timezone = g

            # Add a little overlap to get current data
            min_date = (v["date_time"].min() - datetime.timedelta(days=1)).strftime(
                "%Y/%m/%d"
            )
            max_date = (v["date_time"].max() + datetime.timedelta(days=1)).strftime(
                "%Y/%m/%d"
            )
            if only_add_different:
                # Only want to write new data to disk
                # Get current data, merge it for comparison
                # Will throw an error if time series identifier does not exist
                new_data = v.copy()
                try:
                    current_data = self.retrieve_ts(
                        p_cwms_ts_id=p_cwms_ts_id,
                        start_time=min_date,
                        end_time=max_date,
                        p_units=p_units,
                    )
                except Exception as e:
                    LOGGER.error(f"Error retrieveing {p_cwms_ts_id} for comparison.")
                    current_data = pd.DataFrame()

                if not current_data.empty:
                    try:
                        merged = v.merge(
                            current_data,
                            on=["date_time", "value"],
                            how="outer",
                            suffixes=["", "_"],
                            indicator=True,
                        )
                        # The data to store after comparing to current data
                        new_data = merged[merged["_merge"] == "left_only"]
                    except:
                        LOGGER.error(
                            f"Failed to merge {p_cwms_ts_id} with existing data."
                        )
                if new_data.empty:
                    LOGGER.info(f"No new data to load for {p_cwms_ts_id}")
                    # Do not want to try and load empty data so continue
                    continue
            else:
                new_data = v.copy()

            new_data_len = new_data.shape[0]
            LOGGER.info(f"Loading {new_data_len} new values")

            try:
                self.store_ts(
                    p_cwms_ts_id=p_cwms_ts_id,
                    p_units=p_units,
                    timezone=timezone,
                    times=list(new_data["date_time"]),
                    values=list(new_data["value"].astype(float)),
                    qualities=list(new_data["quality_code"]),
                    format=None,
                    p_store_rule=p_store_rule,
                    p_override_prot=p_override_prot,
                    version_date=version_date,
                    p_office_id=p_office_id,
                )
            except Exception as e:
                LOGGER.error(f"Error in store_ts for {p_cwms_ts_id}")
                LOGGER.error(e)
                continue
        return True


    @LD
    def update_ts_id(
        self,
        p_cwms_ts_id,
        p_interval_utc_offset=None,
        p_snap_forward_minutes=None,
        p_snap_backward_minutes=None,
        p_local_reg_time_zone_id=None,
        p_ts_active_flag=None,
        p_db_officeid=None,
    ):

        cur = self.conn.cursor()
        try:

            cur.callproc(
                "cwms_ts.update_ts_id",
                [
                    p_cwms_ts_id,
                    p_interval_utc_offset,
                    p_snap_forward_minutes,
                    p_snap_backward_minutes,
                    p_local_reg_time_zone_id,
                    p_ts_active_flag,
                    p_db_officeid,
                ],
            )
        except Exception as e:
            LOGGER.error("Error in update_ts_id.")
            cur.close()
            raise ValueError(e)
        cur.close()
        return True

    @LD
    def create_ts(
        self,
        p_cwms_ts_id,
        p_utc_offset=None,
        p_interval_forward=None,
        p_interval_backward=None,
        p_versioned="F",
        p_active_flag="T",
        p_office_id=None,
    ):

        cur = self.conn.cursor()
        try:

            cur.callproc(
                "cwms_ts.create_ts",
                [
                    p_cwms_ts_id,
                    p_utc_offset,
                    p_interval_forward,
                    p_interval_backward,
                    p_versioned,
                    p_active_flag,
                    p_office_id,
                ],
            )
        except Exception as e:
            LOGGER.error("Error in create_ts.")
            cur.close()
            raise ValueError(e.__str__())
        cur.close()
        return True
