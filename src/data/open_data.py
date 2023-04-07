import util
import geopandas as gp
import pandas as pd
import os

def open_data_sources():
    """Constructs SourceDict of open data sources for Citi Bike SDSS."""
    sources = util.SourceDict(
        [
            # Census
            util.Source(
                name="census",
                data_url="https://data.cityofnewyork.us/resource/i69b-3rdj.geojson",
                info_url="https://data.cityofnewyork.us/City-Government/2010-Census-Tracts/fxpq-c8ku",
                description="2010 Census Tracts from the US Census for NYC.",
            ),
            # Bike routes
            util.Source(
                name="bike_routes",
                data_url="https://data.cityofnewyork.us/resource/s5uu-3ajy.geojson",
                info_url="https://data.cityofnewyork.us/Transportation/New-York-City-Bike-Routes/7vsa-caz7",
                description="Locations of bike lanes and routes throughout NYC.",
            ),
            # Streets
            util.Source(
                name="streets",
                data_url="https://data.cityofnewyork.us/resource/8rma-cm9c.geojson",
                info_url="https://data.cityofnewyork.us/City-Government/NYC-Street-Centerline-CSCL-/exjm-f27b",
                description="Road-bed representation of New York City streets.",
            ),
            # Borough boundaries
            util.Source(
                name="boroughs",
                data_url="https://data.cityofnewyork.us/resource/7t3b-ywvw.geojson",
                info_url="https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm",
                description="Boundaries of NYC boroughs, water areas excluded.",
            ),
            # Motor vehicle crashes and collisions
            util.Source(
                name="motor_vehicle_crashes",
                data_url="https://data.cityofnewyork.us/resource/h9gi-nx95.geojson",
                info_url="https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95",
                description="Motor vehicle crashes in NYC from 2012 to the present.",
            ),

        ]
    )

    return sources


def get_open_data(sources, limit=1000000):
    """Downloads open data and returns a dict of GeoDataFrames.

    Arguments:
    sources: a SourceDict containing the Sources for the open data
    limit: the maximum size of the JSON request for each Source

    Returns:
    gdf_dict: a dict of GeoDataFrames of the form {"name": GDF}
    """
    gdf_dict = util.gdf_dict(sources.url_dict(), limit)
    return gdf_dict


def clean_open_data(gdf_dict: dict):
    """Cleans open data for Citi Bike SDSS.

    Arguments:
    gdf_dict: A dict of GeoDataFrames to be cleaned; data is cleaned in place."""

    print("Cleaning open data...")

    # Motor vehicle layer processing
    print("Filtering motor vehicles layer...")
    mv = "motor_vehicle_crashes"

    # Keep only useful columns
    keep = [
        "geometry",
        "zip_code",
        "crash_date",
        "number_of_cyclist_killed",
        "number_of_cyclist_injured",
        "latitude",
        "longitude",
        "borough",
    ]
    gdf_dict[mv] = gdf_dict[mv][keep]

    # Change numeric columns to numeric types
    to_num = [
        "number_of_cyclist_killed",
        "number_of_cyclist_injured",
        "latitude",
        "longitude",
    ]
    for col in to_num:
        gdf_dict[mv][col] = pd.to_numeric(gdf_dict[mv][col])

    # Filter motor vehicle crashes to only include crashes on Jan 1, 2019 or later
    gdf_dict[mv] = gdf_dict[mv][gdf_dict[mv].crash_date >= "2019-01-01T00:00:00.000"]

    # Only include crashes in which at least one cyclist was killed or injured
    gdf_dict[mv] = gdf_dict[mv][
        (gdf_dict[mv].number_of_cyclist_killed > 0)
        | (gdf_dict[mv].number_of_cyclist_injured > 0)
    ]

    # Clip filtered GeoDataFrame by the borough boundaries
    print("Clipping motor vehicles layer...")
    gdf_dict[mv] = gp.clip(gdf_dict[mv], gdf_dict["boroughs"])

    print("Data cleaning complete.\n")


def project_open_data(gdf_dict: dict, epsg: int, preserve=False):
    """Transforms GeoDataFrame dict to selected coordinate reference system.
    Arguments:
    gdf_dict: Dict of GeoDataFrames to be transformed; GDFs are transformed in place.
    epsg: EPSG code of target coordinate reference system.
    preserve: If true, original geometry column is preserved in gdf.orig_geometry.
    """
    print("Projecting open data...")
    for gdf in gdf_dict:
        # If no CRS, set existing CRS before transformation
        if gdf_dict[gdf].crs is None:
            gdf_dict[gdf].set_crs(epsg=4263, inplace=True)
        # If preserve==True, store original geometry in new column
        if preserve:
            gdf["orig_geometry"] = gdf.geometry
        # Transform to CRS of choice
        print(f"Projecting {gdf} layer...")
        gdf_dict[gdf].to_crs(epsg=epsg, inplace=True)

    print("Projection complete.\n")


def write_open_data(gdf_dict: dict, path: str):
    """Writes open data from gdf_dict to a GeoPackage."""
    util.gdf_dict_to_gpkg(gdf_dict, path)

# To add to source list
acs = util.Source(
    name="nyc_acs",
    data_url="http://api.census.gov/data/2021/acs/acs1",
    info_url="https://api.census.gov/data/2021/acs/acs1.html",
    description="General API info for American Community Survey data.",
    api_key="bc7a87bd95f8f3135fe9f3de03202bd6d427a1b8",
    api_key_date="2023-04-03",
)

sources = open_data_sources()
open_gdfs = get_open_data(sources, limit=2000000)

clean_open_data(open_gdfs)
project_open_data(open_gdfs, epsg=2263)
write_open_data(open_gdfs, "open_data.gpkg")


# Write a GeoPackage to path with the data from specified URLs as layers
# and store the resulting dict of GeoDataFrames

# bike_gdfs is a dict of GeoDataFrames; keys are the same as my_urls above
# this command also writes to a GeoPackage at the given path


# @click.command()
# @click.option(
#     "--url", "-u", "urls", type=(str, str), multiple=True, default=open_data_urls
# )
# @click.option("--limit", "-l", "limit", type=int, default=1000000)
# @click.option("--path", "-p", "path", type=str, default="citibike_data.gpkg")
# def main(urls, path, limit):
#     open_data_to_gpkg(urls, path, limit)


# if __name__ == "__main__":
# main()

# bike_gdfs = util.gdf_dict(my_urls, 100000)
# mask = bike_gdfs[borough_boundaries.geometry]
