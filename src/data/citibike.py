"""Gather citibike trip data"""

import sqlite3
import warnings
import zipfile
from io import TextIOWrapper
from pathlib import Path

import geopandas as gpd
import pandas as pd
import util
from requests import HTTPError


class TripData(util.Source):
    base_url = "https://s3.amazonaws.com/tripdata/"

    def __init__(self, raw_dir, gpkg, start_year, start_month, end_year, end_month):
        self.raw_dir = Path(raw_dir)
        self.gpkg = Path(gpkg)
        self.start_year = start_year
        self.start_month = start_month
        self.end_year = end_year
        self.end_month = end_month

    def download_raw(
        self,
        redownload=False,
    ):
        assert self.start_year >= 2017, "Years before 2017 not supported"
        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                if year == self.start_year:
                    if month < self.start_month:
                        continue

                if year == self.end_year:
                    if month > self.end_month:
                        break

                filename = f"{year}{str(month).zfill(2)}-citibike-tripdata.csv.zip"
                url = self.base_url + filename
                local_file = self.raw_dir.joinpath(filename)

                if not local_file.exists() or redownload is True:
                    try:
                        util.download_file(url, local_file)
                    except HTTPError:
                        try:
                            # some files have this typo in name
                            alt_url = url.replace("citi", "cit")
                            util.download_file(alt_url, local_file)
                        except HTTPError:
                            warn_text = f"{url} not found, skipping"
                            warnings.warn(warn_text)

    def _make_df_consistent(self, df):
        """Deal with inconsistencies in column names"""
        to_keep = [
            "start_station_id",
            "start_station_name",
            "end_station_id",
            "end_station_name",
            "start_lng",
            "start_lat",
            "end_lng",
            "end_lat",
            "rideable_type",
            "started_at",
            "duration_seconds",
            "ended_at",
            "member_casual",
        ]

        if "started_at" in df.columns:
            df.started_at = pd.to_datetime(df.started_at, format="%Y-%m-%d %H:%M:%S")
        if "ended_at" in df.columns:
            df.ended_at = pd.to_datetime(df.ended_at, format="%Y-%m-%d %H:%M:%S")

        if "starttime" in df.columns:
            df["started_at"] = pd.to_datetime(
                df.starttime.str[:-5], format="%Y-%m-%d %H:%M:%S"
            )
        if "stoptime" in df.columns:
            df["ended_at"] = pd.to_datetime(
                df.stoptime.str[:-5], format="%Y-%m-%d %H:%M:%S"
            )

        df.rename(
            columns={
                "start station latitude": "start_lat",
                "start station longitude": "start_lng",
                "end station latitude": "end_lat",
                "end station longitude": "end_lng",
                "start station name": "start_station_name",
                "end station name": "end_station_name",
                "start station id": "start_station_id",
                "end station id": "end_station_id",
            },
            inplace=True,
        )

        df["duration_seconds"] = (
            (df.ended_at - df.started_at).dt.total_seconds().astype(pd.Int64Dtype())
        )

        if "rideable_type" not in df.columns:
            df["rideable_type"] = None

        if "member_casual" not in df.columns:
            df["member_casual"] = None

        to_drop = [c for c in df.columns if c not in to_keep]
        df.drop(to_drop, axis=1, inplace=True)
        return df

    def _extract_df(self, csv_zip):
        """Extract dataframe from zipped csv"""
        with zipfile.ZipFile(csv_zip, mode="r") as archive:
            # should be biggest file
            filename = max(archive.filelist, key=lambda x: x.file_size).filename
            with archive.open(filename, mode="r") as cb:
                csvio = TextIOWrapper(cb)
                df = pd.read_csv(
                    csvio,
                    dtype={
                        "start station id": pd.StringDtype(),
                        "end station id": pd.StringDtype(),
                        "start_station_id": pd.StringDtype(),
                        "end_station_id": pd.StringDtype(),
                    },
                )
                df = self._make_df_consistent(df)
                return df

    def _divide_df(self, df):
        """Divide into separate stations & trips tables"""
        station_cols = [
            "start_station_id",
            "start_station_name",
            "end_station_id",
            "end_station_name",
            "start_lng",
            "start_lat",
            "end_lng",
            "end_lat",
        ]

        # get unique list of stations
        start_stations = (
            df[station_cols]
            .groupby("start_station_id")
            .first()
            .reset_index()[
                ["start_station_id", "start_station_name", "start_lng", "start_lat"]
            ]
        )
        start_stations.rename(columns=lambda x: x.replace("start_", ""), inplace=True)

        end_stations = (
            df[station_cols]
            .groupby("end_station_id")
            .first()
            .reset_index()[["end_station_id", "end_station_name", "end_lng", "end_lat"]]
        )
        end_stations.rename(columns=lambda x: x.replace("end_", ""), inplace=True)

        stations = pd.concat([start_stations, end_stations]).drop_duplicates(
            "station_id"
        )
        stations = gpd.GeoDataFrame(
            stations,
            geometry=gpd.points_from_xy(x=stations.lng, y=stations.lat),
            crs=4326,
        )

        # filter trips
        trip_cols = [
            "rideable_type",
            "started_at",
            "duration_seconds",
            "ended_at",
            "member_casual",
            "start_station_id",
            "end_station_id",
        ]
        trips = df[trip_cols]
        trips = trips[~trips.start_station_id.isna() & ~trips.end_station_id.isna()]

        return stations, trips

    def _create_trips_table(self, con):
        con.execute("DROP TABLE IF EXISTS trips")
        sql = """
            CREATE TABLE IF NOT EXISTS trips (
                trip_id INTEGER PRIMARY KEY AUTOINCREMENT,
                rideable_type TEXT,
                started_at DATETIME NOT NULL,
                ended_at DATETIME NOT NULL,
                duration_seconds INTEGER NOT NULL,
                member_casual TEXT,
                start_station_id TEXT NOT NULL,
                end_station_id TEXT NOT NULL,
                FOREIGN KEY(start_station_id) REFERENCES stations(station_id),
                FOREIGN KEY(end_station_id) REFERENCES stations(station_id)
            )
        """
        con.execute(sql)
        con.commit()

    def _setup_gpkg(self, stations_df, crs=2263):
        if stations_df.crs.to_epsg() != crs:
            stations_df.to_crs(crs, inplace=True)

        if self.gpkg.exists():
            # append any new stations to stations table
            exist_stations = gpd.read_file(self.gpkg, layer="stations")
            new_stations = stations_df[
                ~stations_df.station_id.isin(exist_stations.station_id)
            ]
            if len(new_stations) > 0:
                new_stations.to_file(self.gpkg, layer="stations", mode="a")
        else:
            # Create stations table with unique constraint
            stations_df.to_file(self.gpkg, layer="stations")
            with sqlite3.connect(self.gpkg) as con:
                con.execute(
                    "CREATE UNIQUE INDEX station_id_unq_idx ON stations(station_id)"
                )
                # Create trip table manually
                self._create_trips_table(con)

    def to_gpkg(self, replace=False, crs=2263):
        if self.gpkg.exists() and replace:
            self.gpkg.unlink()

        for csv_zip in sorted(self.raw_dir.glob("*.csv.zip")):
            df = self._extract_df(csv_zip)
            stations, trips = self._divide_df(df)

            # update/create stations table
            self._setup_gpkg(stations, crs=crs)
            with sqlite3.connect(self.gpkg) as con:
                trips.to_sql("trips", con, if_exists="append", index=False)

            del df
            del stations
            del trips
