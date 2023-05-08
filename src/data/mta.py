import re
import sqlite3
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
import util
from bs4 import BeautifulSoup


class MtaTurnstiles(util.Source):
    base_url = "http://web.mta.info/developers/"
    catalog_url = base_url + "turnstile.html"

    def __init__(self, raw_dir, gpkg, start_date, end_date):
        super().__init__("mta_turnstile", "MTA Turnstile Counts", epsg=4326)

        assert raw_dir.exists(), "directory does not exist"
        self.raw_dir = Path(raw_dir)

        # parse list of turnstile text file urls
        cat_resp = requests.get(self.catalog_url)
        cat_resp.raise_for_status()
        self.cat_soup = BeautifulSoup(cat_resp.text, features="html.parser")

        self.gpkg = Path(gpkg)
        self.start_date = start_date
        self.end_date = end_date

    def download_raw(self, redownload=False):
        """Download raw text files from mta developer site"""
        date_re = re.compile(r"\d{6}")
        for link in self.cat_soup.find("div", "last").find_all("a"):
            data_url = self.base_url + link.attrs["href"]
            date_str = date_re.search(data_url.split("/")[-1]).group()
            dt = datetime.strptime(date_str, "%y%m%d").date()

            # filter by date
            if (self.start_date is not None) and dt < self.start_date:
                continue

            if (self.end_date is not None) and dt > self.end_date:
                continue

            out_file = self.raw_dir.joinpath(f"turnstile_{date_str}.txt")

            if not out_file.exists() or redownload is True:
                util.download_file(data_url, out_file)

    def setup_gpkg(
        self, remote_complex_lookup_csv, stations_csv, replace=False, crs=2263
    ):
        """Create geopackage with necessary base tables for turnstile data"""

        if replace:
            if self.gpkg.exists():
                self.gpkg.unlink()

        # MTA Stations
        stations = pd.read_csv(stations_csv, dtype={"complex id": pd.StringDtype()})
        station_renames = {
            n: n.lower().strip().replace(" ", "_") for n in stations.columns
        }
        stations.rename(columns=station_renames, inplace=True)
        stations = gpd.GeoDataFrame(
            stations,
            geometry=gpd.points_from_xy(
                x=stations["gtfs_longitude"], y=stations["gtfs_latitude"]
            ),
            crs=4326,
        )
        stations.to_crs(crs, inplace=True)
        stations.to_file(self.gpkg, layer="stations", mode="w")

        # Remote -> Complex lookup table
        remote_lookup = pd.read_csv(
            remote_complex_lookup_csv, dtype={"complex_id": pd.StringDtype()}
        )
        with sqlite3.connect(self.gpkg) as con:
            remote_lookup.to_sql(
                "remote_complex_lookup", con, if_exists="replace", index=False
            )

            ts_table_create = (
                "CREATE TABLE turnstile_observations ( "
                "id varchar NOT NULL, "
                "unit_id varchar NOT NULL, "
                "controlarea varchar NOT NULL, "
                "remoteunit varchar NOT NULL, "
                "subunit_channel_position varchar NOT NULL, "
                "station varchar NOT NULL, "
                "linenames varchar NOT NULL, "
                "division varchar NOT NULL, "
                "date varchar NOT NULL, "
                "time varchar NOT NULL, "
                "observed_at timestamp NOT NULL, "
                "description varchar NOT NULL, "
                "entries bigint NOT NULL, "
                "exits bigint NOT NULL, "
                "net_entries bigint, "
                "net_exits bigint, "
                "hours_difference float,"
                "filename varchar NOT NULL "
                ")"
            )

            con.execute(ts_table_create)

    def _update_net_values(self, con):
        """Update net_entires/net_exists as difference between observations using window function"""
        with sqlite3.connect(self.gpkg) as con:
            sql = """
                    with net_observations as(
                    SELECT
                    id,
                    entries - lag(entries, 1) OVER w AS calculated_net_entries,
                    exits - lag(exits, 1) OVER w AS calculated_net_exits,
                    (JulianDay(observed_at) - JulianDay(lag(observed_at, 1) OVER w)) * 24.0 as hours_difference
                    FROM turnstile_observations
                    WINDOW w AS (PARTITION BY unit_id ORDER BY observed_at)
                    ),
                    net_sum AS (
                    SELECT
                    id,
                    hours_difference,
                    CASE WHEN abs(calculated_net_entries) < 10000 AND hours_difference <= 24
                        THEN abs(calculated_net_entries) ELSE NULL END as net_entries,
                    CASE WHEN abs(calculated_net_exits) < 10000 AND hours_difference <= 24
                        THEN abs(calculated_net_exits) ELSE NULL END as net_exits
                    FROM net_observations
                    )
                    UPDATE turnstile_observations
                    SET
                    hours_difference = (SELECT hours_difference FROM net_sum WHERE net_sum.id = turnstile_observations.id),
                    net_entries = (SELECT CAST(net_entries as Integer) FROM net_sum WHERE net_sum.id = turnstile_observations.id),
                    net_exits = (SELECT CAST(net_exits as Integer) FROM net_sum WHERE net_sum.id = turnstile_observations.id)
                    WHERE id in (SELECT id from net_sum)
                """
            con.execute(sql)
            con.commit()

    def _update_daily_subunit(self, con):
        """Create daily summary table per unit"""

        # create initial table
        con.execute("DROP TABLE IF EXISTS daily_subunit")
        create_sql = """
                CREATE TABLE daily_subunit AS
                    SELECT
                    unit_id,
                    date(datetime(observed_at, '-2 hour', 'start of day')) as date,
                    SUM(net_entries) AS entries,
                    SUM(net_exits) AS exits,
                    NULL as remoteunit
                    FROM turnstile_observations
                    GROUP BY unit_id, datetime(observed_at, '-2 hour', 'start of day')
            """
        con.execute(create_sql)
        con.commit()

        # fill in remoteunit
        update_sql = """
        WITH ru as(
            SELECT DISTINCT unit_id, remoteunit FROM turnstile_observations
        )
        UPDATE daily_subunit
        SET remoteunit = (SELECT remoteunit FROM ru WHERE ru.unit_id = daily_subunit.unit_id)
        """
        con.execute(update_sql)
        con.commit()

    def _update_daily_period_subunit(self, con, period):
        """Create daily summary table per unit for specified peak/off-peak period"""

        assert period in ("morning_peak", "evening_peak", "peak", "offpeak")

        match period:
            case "morning_peak":
                where = "WHERE starts.period_start_hour IN (4,5,6,7) AND starts.weekday IN (1,2,3,4,5)"
                table = "daily_morning_peak_subunit"
            case "evening_peak":
                where = "WHERE starts.period_start_hour IN (14,15,16,17) AND starts.weekday IN (1,2,3,4,5)"
                table = "daily_evening_peak_subunit"
            case "peak":
                where = "WHERE starts.period_start_hour IN (4,5,6,7,14,15,16,17) AND starts.weekday IN (1,2,3,4,5)"
                table = "daily_peak_subunit"
            case "offpeak":
                where = "WHERE starts.period_start_hour NOT IN (4,5,6,7,14,15,16,17) OR starts.weekday IN (0,6)"
                table = "daily_offpeak_subunit"

        # create initial table
        con.execute(f"DROP TABLE IF EXISTS {table}")
        create_sql = f"""
                CREATE TABLE {table} AS
                    WITH starts AS (
                        SELECT
                        id,
                        unit_id,
                        observed_at,
                        hours_difference,
                        net_entries,
                        net_exits,
                        CAST(strftime('%H', DATETIME(observed_at, '-4 hour')) AS bigint) as period_start_hour,
                        CAST(strftime('%w', observed_at) AS bigint) as weekday
                        FROM turnstile_observations
                        WHERE hours_difference < 5 AND hours_difference > 3
                    )
                    SELECT
                    unit_id,
                    date(datetime(observed_at, '-2 hour', 'start of day')) as date,
                    SUM(net_entries) AS entries,
                    SUM(net_exits) AS exits,
                    NULL as remoteunit
                    FROM starts
                    {where}
                    GROUP BY unit_id, datetime(observed_at, '-2 hour', 'start of day')
            """
        con.execute(create_sql)
        con.commit()

        # fill in remoteunit
        update_sql = f"""
        WITH ru as(
            SELECT DISTINCT unit_id, remoteunit FROM turnstile_observations
        )
        UPDATE {table}
        SET remoteunit = (SELECT remoteunit FROM ru WHERE ru.unit_id = {table}.unit_id)
        """
        con.execute(update_sql)
        con.commit()

    def _update_daily_complex(self, con):
        """Create daily counts per complex"""

        con.execute("DROP TABLE IF EXISTS daily_complex")
        create_sql = """
            CREATE TABLE daily_complex AS
            WITH unique_remotes AS(
                SELECT DISTINCT complex_id, remote FROM remote_complex_lookup
            ),
            daily_sum AS(
                SELECT
                    CASE
                    WHEN unique_remotes.complex_id IS NULL
                    THEN remoteunit
                    ELSE unique_remotes.complex_id
                    END as complex_id,
                    remoteunit,
                    date,
                    entries,
                    exits
                FROM daily_subunit
                LEFT JOIN unique_remotes
                ON daily_subunit.remoteunit = unique_remotes.remote
            )
            SELECT
            complex_id,
            date,
            sum(entries) as entries,
            sum(exits) as exits
            FROM daily_sum
            GROUP BY complex_id, date
            """
        con.execute(create_sql)
        con.commit()

    def _update_daily_period_complex(self, con, period):
        """Create daily counts per complex"""

        assert period in ("morning_peak", "evening_peak", "peak", "offpeak")

        match period:
            case "morning_peak":
                subunit_table = "daily_morning_peak_subunit"
                table = "daily_morning_peak_complex"
            case "evening_peak":
                subunit_table = "daily_evening_peak_subunit"
                table = "daily_evening_peak_complex"
            case "peak":
                subunit_table = "daily_peak_subunit"
                table = "daily_peak_complex"
            case "offpeak":
                subunit_table = "daily_offpeak_subunit"
                table = "daily_offpeak_complex"

        con.execute(f"DROP TABLE IF EXISTS {table}")

        create_sql = f"""
            CREATE TABLE {table} AS
            WITH unique_remotes AS(
                SELECT DISTINCT complex_id, remote FROM remote_complex_lookup
            ),
            daily_sum AS(
                SELECT
                    CASE
                    WHEN unique_remotes.complex_id IS NULL
                    THEN remoteunit
                    ELSE unique_remotes.complex_id
                    END as complex_id,
                    remoteunit,
                    date,
                    entries,
                    exits
                FROM {subunit_table}
                LEFT JOIN unique_remotes
                ON {subunit_table}.remoteunit = unique_remotes.remote
            )
            SELECT
            complex_id,
            date,
            sum(entries) as entries,
            sum(exits) as exits
            FROM daily_sum
            GROUP BY complex_id, date
            """
        con.execute(create_sql)
        con.commit()

    def _update_monthly_yearly_complex(self, con, period=None):
        assert (
            period in ("morning_peak", "evening_peak", "peak", "offpeak")
            or period is None
        )

        match period:
            case None:
                table = "complex"
            case "morning_peak":
                table = "morning_peak_complex"
            case "evening_peak":
                table = "evening_peak_complex"
            case "peak":
                table = "peak_complex"
            case "offpeak":
                table = "offpeak_complex"

        con.execute(f"DROP TABLE IF EXISTS monthly_{table}")
        con.execute(f"DROP TABLE IF EXISTS annual_{table}")

        monthly_sql = f"""
            CREATE TABLE monthly_{table} AS
            SELECT
            complex_id,
            CAST(strftime("%Y", date) as bigint) as year,
            CAST(strftime("%m", date) as bigint) as month,
            CAST(AVG(entries) as bigint) as mean_daily_entries,
            SUM(entries) as total_entries,
            CAST(AVG(exits) as bigint) as mean_daily_exits,
            SUM(exits) as total_exits
            FROM
            daily_{table}
            GROUP BY complex_id, strftime("%m-%Y", date)
            HAVING COUNT(*) > 10
            ORDER BY complex_id, year, month
        """

        annual_sql = f"""
            CREATE TABLE annual_{table} AS
            SELECT
            complex_id,
            CAST(strftime("%Y", date) as bigint) as year,
            CAST(AVG(entries) as bigint) as mean_daily_entries,
            SUM(entries) as total_entries,
            CAST(AVG(exits) as bigint) as mean_daily_exits,
            SUM(exits) as total_exits
            FROM
            daily_{table}
            GROUP BY complex_id, strftime("%Y", date)
            ORDER BY complex_id, year
        """

        con.execute(monthly_sql)
        con.execute(annual_sql)
        con.commit()

    def raw_to_gpkg(
        self,
    ):
        """Convert raw text files to geopackage with daily summaries"""
        with sqlite3.connect(self.gpkg) as con:
            for rawfile in sorted(self.raw_dir.glob("turnstile_*.txt")):
                ts = pd.read_csv(rawfile)

                # column renames
                ts.rename(columns={k: k.strip() for k in ts.columns}, inplace=True)
                ts.rename(
                    columns={
                        "C/A": "controlarea",
                        "UNIT": "remoteunit",
                        "SCP": "subunit_channel_position",
                        "STATION": "station",
                        "LINENAME": "linenames",
                        "DIVISION": "division",
                        "DESC": "description",
                        "ENTRIES": "entries",
                        "EXITS": "exits",
                        "DATE": "date",
                        "TIME": "time",
                    },
                    inplace=True,
                )

                ts["observed_at"] = pd.to_datetime(
                    ts.date + " " + ts.time, format="%m/%d/%Y %H:%M:%S"
                )

                # unique id for individual turnstile
                ts["unit_id"] = (
                    ts.controlarea + ts.remoteunit + ts.subunit_channel_position
                )

                # unique identifier
                ts["id"] = ts.unit_id + ts.observed_at.dt.strftime("%Y%m%d%H%M%S")

                ts["filename"] = str(rawfile)

                ts.sort_values(["unit_id", "observed_at"], inplace=True)

                temp_table = f"ts_{rawfile.stem}"
                ts.to_sql(temp_table, con, if_exists="replace", index=False)

                con.execute(
                    f"""
                        INSERT OR IGNORE INTO turnstile_observations
                        SELECT * FROM (
                            SELECT
                            id, unit_id, controlarea, remoteunit, subunit_channel_position,
                            station, linenames, division, date, time, observed_at, description, entries, exits,
                            CAST(NULL as bigint) as net_entries,
                            CAST(NULL as bigint) as net_exits,
                            CAST(NULL as float) as hours_difference,
                            filename
                            FROM {temp_table}
                        )
                        ORDER BY unit_id, observed_at
                    """
                )

                con.execute(f"DROP TABLE {temp_table}")
                con.commit()

            self._update_net_values(con)
            self._update_daily_subunit(con)
            self._update_daily_complex(con)
            self._update_monthly_yearly_complex(con)

            for period in ("morning_peak", "evening_peak", "peak", "offpeak"):
                self._update_daily_period_subunit(con, period)
                self._update_daily_period_complex(con, period)
                self._update_monthly_yearly_complex(con, period)
