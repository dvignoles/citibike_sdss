import util
import click
import gzip
import json
import tempfile
import pandas as pd
import geopandas as gpd


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

    def process(self, output_file, mode="w"):
        """process into geodataframe/geopackage"""

        used_tempfile = False

        # save json to tempfile
        if self.raw_file is None:
            raw_file = tempfile.NamedTemporaryFile(suffix='.json.gz')
            self._download_raw(output_file=raw_file.name)
            used_tempfile = True

        # convert json dicts to geodataframe
        with gzip.open(self.raw_file, "rt") as f:
            si = json.loads(f.read())
            features = []
            for s in si["data"]["stations"]:
                feat = {
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

        # save to file
        gdf.to_file(output_file, layer="stations", mode=mode)
        self.processed_file = output_file

        # clean up raw json file if used
        if used_tempfile:
            raw_file.close()

        return self.processed_file


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


if __name__ == "__main__":
    cli(obj={})
