from GPKG_pipeline import GPKG_pipeline as gpkg_p

my_urls = {
    "census": "https://data.cityofnewyork.us/resource/63ge-mke6.geojson",
    # "bike_routes": "https://data.cityofnewyork.us/resource/s5uu-3ajy.geojson",
    # "street_centerline": "https://data.cityofnewyork.us/resource/8rma-cm9c.geojson",
    # "vision_zero": "https://data.cityofnewyork.us/resource/h9gi-nx95.geojson",
}


# bike_gdfs is a dict of GeoDataFrames; keys are the same as my_urls above
# this command also writes to a GeoPackage at the given path
bike_gdfs = gpkg_p.urls_to_gpkg(my_urls, "my_bike_map.gpkg", 10)
