import click
from GPKG_pipeline import GPKG_pipeline as gpkg_p

# Define default URLs to download if none are specified in command line
my_urls = (
    ("census", "https://data.cityofnewyork.us/resource/63ge-mke6.geojson"),
    ("bike_routes", "https://data.cityofnewyork.us/resource/s5uu-3ajy.geojson"),
    ("street_centerline", "https://data.cityofnewyork.us/resource/8rma-cm9c.geojson"),
    ("vision_zero", "https://data.cityofnewyork.us/resource/h9gi-nx95.geojson"),
)


@click.command()
@click.option("--url", "-u", "urls", type=(str, str), multiple=True, default=my_urls)
@click.option("--limit", "-l", "limit", type=int, default=1000000)
@click.option("--path", "-p", "path", type=str, default="citibike_data.gpkg")
def main(urls, path, limit):
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

    # Write a GeoPackage to path with the data from specified URLs as layers
    # and store the resulting dict of GeoDataFrames
    bike_gdfs = gpkg_p.urls_to_gpkg(urls_dict, path, limit)


if __name__ == "__main__":
    main()

# bike_gdfs is a dict of GeoDataFrames; keys are the same as my_urls above
# this command also writes to a GeoPackage at the given path
