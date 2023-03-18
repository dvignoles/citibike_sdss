import geopandas as gp
import requests

# Get GeoJSON from URL
def json_response(url, limit=500000):
    # Limit will determine how many rows are read
    my_params = {"$limit": limit}
    with requests.get(url, params=my_params) as response:
        return response.json()


# Create GeoDataFrame from URL
def gdf_from_url(url, limit=500000):
    response = json_response(url, limit)
    gdf = gp.GeoDataFrame.from_features(response)
    return gdf


# Create a dict of gdfs matching a dict of URLS
def gdf_dict(url_dict, max_size=500000):
    ans = {}
    for key in url_dict:
        ans[key] = gdf_from_url(url_dict[key], max_size)
    return ans


# Export dict of GeoDataFrames to a GeoPackage
def gdf_dict_to_gpkg(gdf_dict, path):
    for key in gdf_dict:
        gdf_dict[key].to_file(path, layer=key, driver="GPKG")


# Produce a GeoPackage from a dict of URLs
def urls_to_gpkg(url_dict, path, max_size=500000):
    my_gdf_dict = gdf_dict(url_dict, max_size)
    return gdf_dict_to_gpkg(my_gdf_dict, path)


my_urls = {
    "census": "https://data.cityofnewyork.us/resource/63ge-mke6.geojson",
    "bike_routes": "https://data.cityofnewyork.us/resource/s5uu-3ajy.geojson",
    "street_centerline": "https://data.cityofnewyork.us/resource/8rma-cm9c.geojson",
    "vision_zero": "https://data.cityofnewyork.us/resource/h9gi-nx95.geojson",
}

urls_to_gpkg(my_urls, "my_bike_map.gpkg")
