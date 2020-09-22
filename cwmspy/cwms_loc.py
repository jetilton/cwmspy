# -*- coding: utf-8 -*-
"""
Facilities for working with locations in the CWMS database
"""
import logging
from .utils import log_decorator
import cx_Oracle
import pandas as pd

LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)


class CwmsLocMixin:
    @LD
    def store_location(
        self,
        p_location_id,
        p_location_type=None,
        p_elevation=None,
        p_elev_unit_id=None,
        p_vertical_datum=None,
        p_latitude=None,
        p_longitude=None,
        p_horizontal_datum=None,
        p_public_name=None,
        p_long_name=None,
        p_description=None,
        p_time_zone_id=None,
        p_country_name=None,
        p_state_initial=None,
        p_active=None,
        p_ignorenulls="T",
        p_db_office_id=None,
    ):
        """Adds a location to the database.

        Parameters
        ----------

        p_location_id : str
            The location identifier.
        p_location_type : str
            A user-defined type for the location.
        p_elevation : float
            The elevation of the location.
        p_elev_unit_id : str
            The elevation unit.
        p_vertical_datum : str
            The datum of the elevation.
        p_latitude : float
            The actual latitude of the location.
        p_longitude : float
            The actual longitude of the location.
        p_horizontal_datum : str
            The datum for the latitude and longitude.
        p_public_name : str
            The public name for the location.
        p_long_name : str
            The long name for the location.
        p_description : str
            A description of the location.
        p_time_zone_id : str
            The time zone name for the location.
        p_country_name : str
            The name of the county that the location is in.
        p_state_initial : str
            The two letter abbreviation of the state that the location is in.
        p_active : str
             flag ('T' or 'F') that specifies whether the location is marked
             as active.
        p_ignorenulls : str
            A flag ('T' or 'F') that specifies whether to ignore NULL
            parameters. If 'F', existing data will be updated with NULL
            parameter values.
        p_db_office_id : str
            The office that owns the location. If not specified or NULL, the
            session user's default office will be used.

        Returns
        -------
        Boolean
            True for success.

        Examples
        -------
        ```python
        cwms.retrieve_location("TST")
        []
        cwms.store_location("TST")
        cwms.retrieve_location("TST")
        [(76140126, 26, 'TST', 'T')]

        ```

        """
        cur = self.conn.cursor()
        LOGGER.info("Start store_location.")
        try:
            cur.callproc(
                "cwms_loc.store_location",
                [
                    p_location_id,
                    p_location_type,
                    p_elevation,
                    p_elev_unit_id,
                    p_vertical_datum,
                    p_latitude,
                    p_longitude,
                    p_horizontal_datum,
                    p_public_name,
                    p_long_name,
                    p_description,
                    p_time_zone_id,
                    p_country_name,
                    p_state_initial,
                    p_active,
                    p_ignorenulls,
                    p_db_office_id,
                ],
            )
        except Exception as e:
            LOGGER.error("Error in store location.")
            cur.close()
            raise ValueError(e)
        cur.close()
        return True

    @LD
    def delete_location(
        self, p_location_id, p_delete_action="DELETE LOC", p_db_office_id=None
    ):
        """Deletes a location from the database.

        Parameters
        ----------
        p_location_id : str
            The location identifier.
        p_delete_action : str

            Specifies what to delete. Actions are as follows:

            - <span style="color:#bf2419">`"DELETE LOC"`</span>
            -- deletes only this location, and then only if it has no associated dependent data
            - <span style="color:#bf2419">`"'DELETE KEY"`</span>
            -- deletes only this location, and then only if it has no associated dependent data
            - <span style="color:#bf2419">`"DELETE TS ID"`</span>
            -- deletes time series identifiers associated with this location, and then only if they have no time series data
            - <span style="color:#bf2419">`"DELETE DATA"`</span>
            -- deletes only dependent data of this location, if any
            - <span style="color:#bf2419">`"DELETE TS DATA"`</span>
            -- deletes time series data of all time series identifiers associated with this location, but not the time series identifiers themselves
            - <span style="color:#bf2419">`"DELETE TS CASCADE"`</span>
            -- deletes time series identifiers associated with this location, and all of their time series data, if any
            - <span style="color:#bf2419">`"DELETE LOC CASCADE"`</span>
            -- deletes this location and all dependent data, if any
            - <span style="color:#bf2419">`"DELETE ALL"`</span>
            -- deletes this location and all dependent data, if any

        p_db_office_id : str
            The office that owns the location. If not specified or NULL, the
            session user's default office will be used.


        Returns
        -------
        Boolean
            True for success.

        Examples
        -------
        ```python
        from cwmspy.core import CWMS
        cwms = CWMS()
        cwms.connect()

        cwms.retrieve_location("TST")
        [(76140126, 26, 'TST', 'T')]
        cwms.delete_location("TST")
        cwms.retrieve_location("TST")
        []
        ```

        """
        LOGGER.info("Start delete_location")
        cur = self.conn.cursor()
        try:
            cur.callproc(
                "cwms_loc.delete_location",
                [p_location_id, p_delete_action, p_db_office_id],
            )
        except Exception as e:
            LOGGER.error(e)
            cur.close()
        cur.close()
        LOGGER.info("End delete_location")
        return True

    @LD
    def retrieve_location(
        self, p_location_id, p_elev_unit_id="m", p_db_office_id=None, return_df=True
    ):
        """Retreives location information from the database.

        Parameters
        ----------
        p_location_id : str
            The location identifier.
        p_elev_unit_id : str
            The unit to retrieve the elevation in (the default is "m").
        p_db_office_id : str
            The office that owns the location. If not specified or None, the session user's default office will be used (the default is None).
        return_df : bool
            Return a pandas dataframe (the default is True).

        Returns
        -------
        type
            Pandas dataframe or list of dictionaries

        Examples
        -------
        ```python
        from cwmspy.core import CWMS
        cwms = CWMS()
        cwms.connect()

        df = cwms.retrieve_location("TST")
        ```
        """

        LOGGER.info("Start retrieve_location")

        cur = self.conn.cursor()
        # The below are out parameters.  You need to pass in out parameters to the
        # procedure if they are listed of the correct type.
        p_location_type = cur.var(cx_Oracle.STRING)
        p_elevation = cur.var(cx_Oracle.NUMBER)
        p_vertical_datum = cur.var(cx_Oracle.STRING)
        p_latitude = cur.var(cx_Oracle.NUMBER)
        p_longitude = cur.var(cx_Oracle.NUMBER)
        p_horizontal_datum = cur.var(cx_Oracle.STRING)
        p_public_name = cur.var(cx_Oracle.STRING)
        p_long_name = cur.var(cx_Oracle.STRING)
        p_description = cur.var(cx_Oracle.STRING)
        p_time_zone_id = cur.var(cx_Oracle.STRING)
        p_county_name = cur.var(cx_Oracle.STRING)
        p_state_initial = cur.var(cx_Oracle.STRING)
        p_active = cur.var(cx_Oracle.STRING)
        p_alias_cursor = cur.var(cx_Oracle.CURSOR)

        # These are all of the out parameters that will be returned
        out_list = [
            p_location_id,
            p_location_type,
            p_elevation,
            p_vertical_datum,
            p_latitude,
            p_longitude,
            p_horizontal_datum,
            p_public_name,
            p_long_name,
            p_description,
            p_time_zone_id,
            p_county_name,
            p_state_initial,
            p_active,
            p_alias_cursor,
        ]

        try:
            in_list = out_list.copy()
            in_list.insert(1, p_elev_unit_id)
            in_list += [p_db_office_id]
            cur.callproc(
                "cwms_loc.retrieve_location", in_list,
            )
        except ValueError as e:
            LOGGER.error("Error in retrieve_location.")
            cur.close()
            raise ValueError(e)
        cur.close()
        LOGGER.info("End retrieve_location")
        alias = [r for r in p_alias_cursor.getvalue()]

        out_dict = [
            {
                "name": "location_id",
                "value": "",
                "description": "The location identifier",
            },
            {
                "name": "location_type",
                "value": "",
                "description": "A user-defined type for the location",
            },
            {
                "name": "elevation",
                "value": "",
                "description": "The elevation of the location",
            },
            {
                "name": "vertical_datum",
                "value": "",
                "description": "The datum of the elevation",
            },
            {
                "name": "lat",
                "value": "",
                "description": "The actual latitude of the location",
            },
            {
                "name": "long",
                "value": "",
                "description": "The actual longitude of the location",
            },
            {
                "name": "horizontal_datum",
                "value": "",
                "description": "The datum for the latitude and longitude",
            },
            {
                "name": "public_name",
                "value": "",
                "description": "The public name for the location",
            },
            {
                "name": "long_name",
                "value": "",
                "description": "The long name for the location",
            },
            {
                "name": "description",
                "value": "",
                "description": "A description of the location",
            },
            {
                "name": "timezone",
                "value": "",
                "description": "The time zone name for the location",
            },
            {
                "name": "county_name",
                "value": "",
                "description": "The name of the county that the location is in",
            },
            {
                "name": "state_initial",
                "value": "",
                "description": "The two letter abbreviation of the state that the location is in",
            },
            {
                "name": "location_id",
                "value": "",
                "description": "The location identifier",
            },
            {"name": "alias", "value": "", "description": "Aliases for the location",},
        ]

        for i, v in enumerate(out_dict):
            if i == 0:
                v["value"] = out_list[i]
            else:
                v["value"] = out_list[i].getvalue()
        out_dict[-1]["value"] = alias
        if return_df:
            return pd.DataFrame(out_dict).set_index("name")
        return out_dict
