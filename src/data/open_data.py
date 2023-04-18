import util
import geopandas as gp
import pandas as pd
import os
import importlib


# Define sources (doing so separately from open_data_sources() allows
# flexibility in which sources to include)
census_tracts_geom = util.OpenDataSource(
    name="census_geom",
    data_url="https://data.cityofnewyork.us/resource/i69b-3rdj.geojson",
    info_url="https://data.cityofnewyork.us/City-Government/2010-Census-Tracts/fxpq-c8ku",
    description="2010 Census Tracts from the US Census for NYC.",
    epsg=4326,
)

census_acs_pop = util.CensusSource(
    name="acs_population",
    description="Total population five-year estimates from ACS 2019.",
    place="New York, NY",
    variables=["B01003_001E"],
    epsg=3857,
)

subways = util.OpenDataSource(
    name="subway_stations",
    description="Point layer of all subway stations in NYC.",
    data_url="https://data.cityofnewyork.us/resource/kk4q-3rt2.geojson",
    info_url="https://data.cityofnewyork.us/Transportation/Subway-Stations/arq3-7z49",
    epsg=4326,
)

census_nta = util.OpenDataSource(
    name="census_nta",
    description="Aggregated population for NYC Neighborhood Tabulation Areas.",
    data_url="https://data.cityofnewyork.us/resource/rnsn-acs2.geojson",
    info_url="https://data.cityofnewyork.us/City-Government/Census-Demographics-at-the-Neighborhood-Tabulation/rnsn-acs2",
    epsg=4326,
)

# Bike routes
bike_routes = util.OpenDataSource(
    name="bike_routes",
    data_url="https://data.cityofnewyork.us/resource/s5uu-3ajy.geojson",
    info_url="https://data.cityofnewyork.us/Transportation/New-York-City-Bike-Routes/7vsa-caz7",
    description="Locations of bike lanes and routes throughout NYC.",
    epsg=4326,
)

# Streets
streets = util.OpenDataSource(
    name="streets",
    data_url="https://data.cityofnewyork.us/resource/8rma-cm9c.geojson",
    info_url="https://data.cityofnewyork.us/City-Government/NYC-Street-Centerline-CSCL-/exjm-f27b",
    description="Road-bed representation of New York City streets.",
    epsg=4326,
)

# Borough boundaries
boroughs = util.OpenDataSource(
    name="boroughs",
    data_url="https://data.cityofnewyork.us/resource/7t3b-ywvw.geojson",
    info_url="https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm",
    description="Boundaries of NYC boroughs, water areas excluded.",
    epsg=4326,
)

# Motor vehicle crashes and collisions
motor_vehicle_crashes = util.OpenDataSource(
    name="motor_vehicle_crashes",
    data_url="https://data.cityofnewyork.us/resource/h9gi-nx95.geojson",
    info_url="https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95",
    description="Motor vehicle crashes in NYC from 2012 to the present.",
    size=2000000,
    epsg=4326,
)


def open_data_sources():
    """Constructs SourceDict of open data sources for Citi Bike SDSS."""
    sources = util.SourceDict(
        [
            census_acs_pop,
            bike_routes,
            streets,
            boroughs,
            motor_vehicle_crashes,
            subways,
            census_nta,
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


def clean_open_data(data_dict):
    """Cleans open data for Citi Bike SDSS.

    Arguments:
    gdf_dict: A DataDict of GeoDataFrames to be cleaned; data is cleaned in place."""

    gdf_dict = data_dict.data

    print("Cleaning open data...")

    # Filter and clip motor vehicles layer
    gdf_dict["motor_vehicle_crashes"] = clean_motor_vehicles(
        gdf=gdf_dict["motor_vehicle_crashes"], mask=gdf_dict["boroughs"]
    )

    # Rename ACS population column
    gdf_dict["acs_population"].rename(
        columns={"B01003_001E": "population"}, inplace=True
    )

    # Clip census population layer
    print("Clipping census population layer...")
    gdf_dict["acs_population"] = gp.clip(gdf_dict["acs_population"], gdf_dict["boroughs"])

    print("Data cleaning complete.\n")


def clean_motor_vehicles(gdf, mask):

    # Motor vehicle layer processing
    print("Filtering motor vehicles layer...")

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

    gdf = gdf[keep]

    # Change numeric columns to numeric types
    to_num = [
        "number_of_cyclist_killed",
        "number_of_cyclist_injured",
        "latitude",
        "longitude",
    ]

    for col in to_num:
        gdf[col] = pd.to_numeric(gdf[col])

    # Filter motor vehicle crashes to only include crashes on Jan 1, 2019 or later
    gdf = gdf[gdf.crash_date >= "2019-01-01T00:00:00.000"]

    # Only include crashes in which at least one cyclist was killed or injured
    gdf = gdf[(gdf.number_of_cyclist_killed > 0) | (gdf.number_of_cyclist_injured > 0)]

    # Clip filtered motor vehicles GeoDataFrame by the borough boundaries
    gdf = gp.clip(gdf, mask)

    return gdf
