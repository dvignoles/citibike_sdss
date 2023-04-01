import click
import util

# Define list of open data sources
open_data_sources = util.SourceDict(
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


def open_data_to_gdfs(
    urls=open_data_sources.url_tuples(), limit=1000000
):

    # Create dict of URLs to pass to util.gdf_dict()
    urls_dict = {}

    # If urls is a tuple of tuples, iterate through the tuples and construct the dict
    if type(urls[0]) == tuple:
        for url_tuple in urls:
            urls_dict[url_tuple[0]] = url_tuple[1]
    # Else if urls is a tuple of strings (if just one URL is given), construct dict from tuple
    elif type(urls[0]) == str:
        urls_dict[urls[0]] = urls[1]
    else:
        raise TypeError

    # Generate dict of GeoDataFrames from the dict of URLs
    bike_gdfs = util.gdf_dict(urls_dict, limit)

    return bike_gdfs

    # Write a GeoPackage to path with the data from specified URLs as layers
    # and store the resulting dict of GeoDataFrames

    # bike_gdfs is a dict of GeoDataFrames; keys are the same as my_urls above
    # this command also writes to a GeoPackage at the given path
    return bike_gdfs


@click.command()
@click.option(
    "--url", "-u", "urls", type=(str, str), multiple=True, default=open_data_urls
)
@click.option("--limit", "-l", "limit", type=int, default=1000000)
@click.option("--path", "-p", "path", type=str, default="citibike_data.gpkg")
def main(urls, path, limit):
    open_data_to_gpkg(urls, path, limit)


# if __name__ == "__main__":
# main()

# bike_gdfs = util.gdf_dict(my_urls, 100000)
# mask = bike_gdfs[borough_boundaries.geometry]
