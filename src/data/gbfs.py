import gzip
import json
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path

import click
import geopandas as gpd
import pandas as pd
import util


class Stations:
    def __init__(
        self,
        url="https://gbfs.citibikenyc.com/gbfs/en/station_information.json",
        local_file=None,
    ):
        self.url = url
        self.raw_file = local_file

    def _download_raw(self, output_file=None):
        """download gzip compressed json to output_file"""
        self.raw_file = util.download_file(self.url, output_file, compress=True)
        return self.raw_file

    def process(self, output_file, mode="w", mask=None, replace=False):
        """process into geodataframe/geopackage"""

        used_tempfile = False

        # save json to tempfile
        if self.raw_file is None:
            raw_file = tempfile.NamedTemporaryFile(suffix=".json.gz")
            self._download_raw(output_file=raw_file.name)
            used_tempfile = True

        if output_file.exists() and replace:
            output_file.unlink()

        # convert json dicts to geodataframe
        with gzip.open(self.raw_file, "rt") as f:
            si = json.loads(f.read())
            features = []
            for i, s in enumerate(si["data"]["stations"]):
                feat = {
                    "objectid": i,
                    "station_id": s["station_id"],
                    "legacy_id": s["legacy_id"],
                    "external_id": s["external_id"],
                    "lon": s["lon"],
                    "lat": s["lat"],
                    "name": s["name"],
                    "short_name": s["short_name"],
                    "station_type": s["station_type"],
                    "capacity": s["capacity"],
                    "eightd_has_key_dispenser": s["eightd_has_key_dispenser"],
                    "rental_method_key": True
                    if "KEY" in s["rental_methods"]
                    else False,
                    "rental_method_credit": True
                    if "CREDITCARD" in s["rental_methods"]
                    else False,
                    "has_kiosk": s["has_kiosk"],
                    "electric_bike_surcharge_waiver": s[
                        "electric_bike_surcharge_waiver"
                    ],
                    "region_id": s["region_id"] if "region_id" in s else None,
                }
                features.append(feat)
        df = pd.DataFrame.from_records(features)
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(x=df.lon, y=df.lat, crs=4326)
        )
        # to long island state plane
        gdf.to_crs("EPSG:2263", inplace=True)

        if mask is not None:
            gdf = gpd.clip(gdf, mask)

        # save to file
        gdf.to_file(output_file, layer="station", mode=mode, crs="EPSG:2263")
        self.processed_file = output_file

        # clean up raw json file if used
        if used_tempfile:
            raw_file.close()

        return self.processed_file


class StationStatus:
    def __init__(self, output_file, raw_dir):
        self.output_file = Path(output_file).resolve()
        self.raw_dir = Path(raw_dir).resolve()

        observation_files = sorted(self.raw_dir.glob("*.json.gz"))
        self.observations = [
            (datetime.strptime(f.name[0:19], "%Y-%m-%d_%H:%M:%S"), f)
            for f in observation_files
        ]

    def _read_statusfile(self, status):
        obs_dt, status_file = status
        with gzip.open(status_file, "rt") as f:
            raw = json.loads(f.read())
            statuses = []
            for station in raw["data"]["stations"]:
                # capture = time of API call
                station["capture_datetime"] = obs_dt.strftime("%Y-%m-%d_%H:%M:%S")
                station["capture_date"] = obs_dt.strftime("%Y-%m-%d")
                station["capture_year"] = obs_dt.year
                station["capture_month"] = obs_dt.month
                station["capture_day"] = obs_dt.day
                station["capture_time"] = obs_dt.strftime("%H:%M:%S")
                station["capture_hour"] = obs_dt.hour
                station["capture_minute"] = obs_dt.minute
                station["capture_second"] = obs_dt.second
                station["capture_weekday"] = obs_dt.weekday()

                # reported = time of last GBFS status update
                last_rep = datetime.fromtimestamp(station.pop("last_reported"))
                station["reported_datetime"] = last_rep.strftime("%Y-%m-%d_%H:%M:%S")
                station["reported_date"] = last_rep.strftime("%Y-%m-%d")
                station["reported_year"] = last_rep.year
                station["reported_month"] = last_rep.month
                station["reported_day"] = last_rep.day
                station["reported_time"] = last_rep.strftime("%H:%M:%S")
                station["reported_hour"] = last_rep.hour
                station["reported_minute"] = last_rep.minute
                station["reported_second"] = last_rep.second
                station["reported_weekday"] = last_rep.weekday()

                # valet not always present in status
                if "valet" in station.keys():
                    station["valet_revision"] = station["valet"]["valet_revision"]
                    station["valet_active"] = station["valet"]["active"]
                    station["valet_off_dock_count"] = station["valet"]["off_dock_count"]

                    station["valet_off_dock_capacity"] = station["valet"][
                        "off_dock_capacity"
                    ]
                    station["valet_dock_blocked_count"] = station["valet"][
                        "dock_blocked_count"
                    ]
                    station.pop("valet")

                if "eightd_active_station_services" in station.keys():
                    # I don't think we need this
                    station.pop("eightd_active_station_services")

                statuses.append(station)

            return statuses

    def _create_table(self, con):
        create = (
            "CREATE TABLE IF NOT EXISTS status ( "
            "objectid INTEGER PRIMARY KEY AUTOINCREMENT, "
            "station_id TEXT NOT NULL, "
            "legacy_id TEXT NOT NULL, "
            "is_installed BOOLEAN NOT NULL CHECK (is_installed IN (0, 1)), "
            "num_docks_disabled INTEGER, "
            "num_docks_available INTEGER, "
            "eightd_has_available_keys BOOLEAN NOT NULL CHECK (eightd_has_available_keys IN (0, 1)), "
            "eightd_active_station_services TEXT, "
            "station_status TEXT, "
            "num_bikes_available INTEGER, "
            "num_bikes_disabled INTEGER, "
            "num_ebikes_available INTEGER, "
            "num_scooters_available INTEGER, "
            "num_scooters_unavailable INTEGER, "
            "is_returning BOOLEAN NOT NULL CHECK (is_returning IN (0,1)), "
            "is_renting BOOLEAN NOT NULL CHECK (is_renting IN (0,1)), "
            "valet_revision TEXT, "
            "valet_active INTEGER, "
            "valet_off_dock_count INTEGER, "
            "valet_off_dock_capacity INTEGER, "
            "valet_dock_blocked_count INTEGER, "
            "capture_datetime DATETIME NOT NULL, "
            "capture_date TEXT, "
            "capture_year INTEGER, "
            "capture_month INTEGER, "
            "capture_day INTEGER, "
            "capture_time TEXT, "
            "capture_hour INTEGER, "
            "capture_minute INTEGER, "
            "capture_second INTEGER, "
            "capture_weekday INTEGER, "
            "reported_datetime DATETIME NOT NULL, "
            "reported_date TEXT, "
            "reported_year INTEGER, "
            "reported_month INTEGER, "
            "reported_day INTEGER, "
            "reported_time TEXT, "
            "reported_hour INTEGER, "
            "reported_minute INTEGER, "
            "reported_second INTEGER, "
            "reported_weekday INTEGER, "
            "stale BOOLEAN CHECK (stale IN (0,1)),"
            "FOREIGN KEY(station_id) REFERENCES station(station_id)"
            ")"
        )

        con.execute(create)
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_status_station_id ON status (station_id)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_status_reported_datetime ON status (reported_datetime)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_status_reported_year ON status (reported_year)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_status_reported_month ON status (reported_month)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_status_reported_hour ON status (reported_hour)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_status_reported_weekday ON status (reported_weekday)"
        )
        con.commit()

    def _last_captured(self, con):
        last = con.execute(
            "SELECT capture_datetime FROM status ORDER BY capture_datetime DESC LIMIT 1;"
        ).fetchall()
        if len(last) == 0:
            return False
        else:
            return datetime.strptime(last[0][0], "%Y-%m-%d %H:%M:%S")

    def _filter_obs(self, after):
        self.observations = list(filter(lambda obs: obs[0] > after, self.observations))

    def _set_stale(self, con):
        """Set stale boolean (non-updates) attribute using rolling window"""

        sql = """
            with hours_dif AS (
                SELECT
                objectid,
                station_id,
                reported_datetime,
                (JulianDay(reported_datetime) - JulianDay(lag(reported_datetime, 1) OVER w)) * 24.0 as hours_difference
                FROM status
                WINDOW w AS (Partition BY station_id ORDER BY reported_datetime)
            ),
            staleness AS (
                SELECT
                objectid,
                CASE WHEN hours_difference = 0 THEN 1 ELSE 0 END as stale
                FROM hours_dif
            )
            UPDATE status
            SET
            stale = (SELECT stale FROM staleness WHERE staleness.objectid = status.objectid)
        """
        con.execute(sql)
        con.commit()

    def process(self):
        assert self.output_file.exists(), "gpkg does not exist"

        count = 0
        with sqlite3.connect(self.output_file) as con:
            self._create_table(con)

            # filter out observations already in geopackage
            last_cap = self._last_captured(con)
            if last_cap:
                self._filter_obs(last_cap)

            for obs in self.observations:
                status = self._read_statusfile(obs)
                df = pd.DataFrame.from_records(status)

                df["capture_datetime"] = pd.to_datetime(
                    df.capture_datetime, format="%Y-%m-%d_%H:%M:%S"
                )
                df["reported_datetime"] = pd.to_datetime(
                    df.reported_datetime, format="%Y-%m-%d_%H:%M:%S"
                )
                df.to_sql("status", con, if_exists="append", index=False)
                count += 1
            self._set_stale(con)
            return count

    def _peak_summary(self, con, period):
        assert period in ("all", "morning_peak", "evening_peak", "peak", "offpeak")

        match period:
            case "all":
                where = "WHERE station.station_id in (SELECT station_id FROM not_stale)"
                table = "status_summary"
            case "morning_peak":
                where = "WHERE (reported_weekday BETWEEN 0 AND 4) AND reported_hour IN (6,7,8,9) AND stale = 0 AND station.station_id in (SELECT station_id FROM not_stale)"
                table = "status_morning_peak_summary"
            case "evening_peak":
                where = "WHERE (reported_weekday BETWEEN 0 AND 4) AND reported_hour IN (16,17,18,19) AND stale = 0 AND station.station_id in (SELECT station_id FROM not_stale)"
                table = "status_evening_peak_summary"
            case "peak":
                where = "WHERE (reported_weekday BETWEEN 0 AND 4) AND reported_hour IN (6,7,8,9,16,17,18,19) AND stale = 0 AND station.station_id in (SELECT station_id FROM not_stale)"
                table = "status_peak_summary"
            case "offpeak":
                where = "WHERE (reported_hour NOT IN (6,7,8,9,16,17,18,19) OR reported_weekday BETWEEN 5 AND 6) AND stale = 0 AND station.station_id in (SELECT station_id FROM not_stale)"
                table = "status_offpeak_summary"

        sql = f"""
            CREATE TABLE IF NOT EXISTS {table} AS
            WITH not_stale AS(
                SELECT
                station_id,
                COUNT(CASE WHEN stale = 1 THEN 1 ELSE NULL END) *1.0 / COUNT(*)*1.0 as stale_percent
                FROM status
                GROUP BY station_id
                HAVING stale_percent < 0.70
                ORDER BY stale_percent DESC
            ),
            rush AS (
                SELECT
                    station.fid as station_fid,
                    station.station_id as station_id,
                    num_bikes_available,
                    num_docks_available,
                    num_bikes_available*1.0 / (station.capacity*1.0) AS perc_capacity_available,
                    num_bikes_available*1.0 / ((num_docks_available + num_bikes_available)*1.0) AS perc_enabled_available,
                    num_ebikes_available,
                    reported_datetime,
                    reported_year,
                    reported_month,
                    reported_day,
                    reported_hour,
                    reported_minute,
                    reported_weekday
                FROM status JOIN station on station.station_id = status.station_id
                {where}
            ),
            totals AS (
                SELECT station_fid, station_id,
                COUNT(*) as total,
                SUM(CASE WHEN num_bikes_available = 0 THEN 1 else 0 end) as count_eq0_bikes_available,
                SUM(CASE WHEN num_bikes_available = 1 THEN 1 else 0 end) as count_eq1_bikes_available,
                SUM(CASE WHEN num_bikes_available <= 3 THEN 1 else 0 end) as count_lte3_bikes_available,
                SUM(CASE WHEN num_bikes_available <= 5 THEN 1 else 0 end) as count_lte5_bikes_available,
                SUM(CASE WHEN num_docks_available = 0 THEN 1 else 0 end) as count_eq0_docks_available,
                SUM(CASE WHEN num_docks_available = 1 THEN 1 else 0 end) as count_eq1_docks_available,
                SUM(CASE WHEN num_docks_available <= 3 THEN 1 else 0 end) as count_lte3_docks_available,
                SUM(CASE WHEN num_docks_available <= 5 THEN 1 else 0 end) as count_lte5_docks_available,
                round(AVG(perc_capacity_available), 3) as avg_perc_capacity_available,
                round(AVG(perc_enabled_available), 3) as avg_perc_enabled_available,
                SUM(CASE WHEN perc_capacity_available = 0.0 THEN 1 else 0 end) as count_0,
                SUM(CASE WHEN perc_capacity_available <= 0.10 THEN 1 else 0 end) as count_10,
                SUM(CASE WHEN perc_capacity_available <= 0.25 THEN 1 else 0 end) as count_25,
                SUM(CASE WHEN perc_capacity_available <= 0.50 THEN 1 else 0 end) as count_lt50,
                SUM(CASE WHEN perc_capacity_available >= 0.50 THEN 1 else 0 end) as count_gt50,
                SUM(CASE WHEN perc_capacity_available >= 0.75 THEN 1 else 0 end) as count_gt75,
                SUM(CASE WHEN perc_capacity_available >= 0.90 THEN 1 else 0 end) as count_gt90,
                SUM(CASE WHEN perc_capacity_available = 100 THEN 1 else 0 end) as count_100,
                SUM(CASE WHEN perc_enabled_available = 0.0 THEN 1 else 0 end) as counte_0,
                SUM(CASE WHEN perc_enabled_available <= 0.10 THEN 1 else 0 end) as counte_10,
                SUM(CASE WHEN perc_enabled_available <= 0.25 THEN 1 else 0 end) as counte_25,
                SUM(CASE WHEN perc_enabled_available <= 0.50 THEN 1 else 0 end) as counte_lt50,
                SUM(CASE WHEN perc_enabled_available >= 0.50 THEN 1 else 0 end) as counte_gt50,
                SUM(CASE WHEN perc_enabled_available >= 0.75 THEN 1 else 0 end) as counte_gt75,
                SUM(CASE WHEN perc_enabled_available >= 0.90 THEN 1 else 0 end) as counte_gt90,
                SUM(CASE WHEN perc_enabled_available = 100 THEN 1 else 0 end) as counte_100
                FROM rush
                GROUP BY station_fid, station_id
            )
            SELECT
                station_fid, station_id,
                round(count_eq0_bikes_available*1.0 / total*1.0, 3) AS bikes_available_eq0,
                round(count_eq1_bikes_available*1.0 / total*1.0, 3) AS bikes_available_eq1,
                round(count_lte3_bikes_available*1.0 / total*1.0, 3) AS bikes_available_lte3,
                round(count_lte5_bikes_available*1.0 / total*1.0, 3) AS bikes_available_lte5,
                round(count_eq0_docks_available*1.0 / total*1.0, 3) AS docks_available_eq0,
                round(count_eq1_docks_available*1.0 / total*1.0, 3) AS docks_available_eq1,
                round(count_lte3_docks_available*1.0 / total*1.0, 3) AS docks_available_lte3,
                round(count_lte5_docks_available*1.0 / total*1.0, 3) AS docks_available_lte5,
                avg_perc_capacity_available,
                avg_perc_enabled_available,
                round(count_0*1.0 / total*1.0, 3) AS capacity_available_eq_0percent,
                round(count_10*1.0 / total*1.0, 3) AS capacity_available_lt_10percent,
                round(count_25*1.0 / total*1.0, 3) AS capacity_available_lt_25percent,
                round(count_lt50*1.0 / total*1.0, 3) AS capacity_available_lt_50percent,
                round(count_gt50*1.0 / total*1.0, 3) AS capacity_available_gt_50percent,
                round(count_gt75*1.0 / total*1.0, 3) AS capacity_available_gt_75percent,
                round(count_gt90*1.0 / total*1.0, 3) AS capacity_available_gt_90percent,
                round(count_100*1.0 / total*1.0, 3) AS capacity_available_eq_100percent,
                round(counte_0*1.0 / total*1.0, 3) AS enabled_available_eq_0percent,
                round(counte_10*1.0 / total*1.0, 3) AS enabled_available_lt_10percent,
                round(counte_25*1.0 / total*1.0, 3) AS enabled_available_lt_25percent,
                round(counte_lt50*1.0 / total*1.0, 3) AS enabled_available_lt_50percent,
                round(counte_gt50*1.0 / total*1.0, 3) AS enabled_available_gt_50percent,
                round(counte_gt75*1.0 / total*1.0, 3) AS enabled_available_gt_75percent,
                round(counte_gt90*1.0 / total*1.0, 3) AS enabled_available_gt_90percent,
                round(counte_100*1.0 / total*1.0, 3) AS enabled_available_eq_100percent
            FROM totals"""
        con.execute(sql)
        con.commit()

    def create_summaries(self):
        with sqlite3.connect(self.output_file) as con:
            self._peak_summary(con, period="all")
            self._peak_summary(con, period="morning_peak")
            self._peak_summary(con, period="evening_peak")
            self._peak_summary(con, period="peak")
            self._peak_summary(con, period="offpeak")


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)


@cli.command(help="Download station information")
@click.pass_context
@click.argument("output_file", nargs=1, type=click.Path())
def getstations(ctx, output_file):
    stations = Stations()
    stations.process(output_file=output_file)


@cli.command(help="Process station status into geopackage")
@click.pass_context
@click.argument("raw_dir", nargs=1, type=click.Path())
@click.argument("output_file", nargs=1, type=click.Path())
def cleanstatus(ctx, raw_dir, output_file):
    if not Path(output_file).exists():
        stations = Stations()
        stations.process(output_file=output_file)

    status = StationStatus(output_file, raw_dir)
    status.process()


if __name__ == "__main__":
    cli(obj={})
