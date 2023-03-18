import geopandas as gp
import requests
import warnings

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
def gdf_dict(url_dict, limit=500000):
    ans = {}
    for key in url_dict:
        ans[key] = gdf_from_url(url_dict[key], limit)

        # Warn the user if the GeoDataFrame reached the size limit, as
        # this probably means that the full dataset was not
        # downloaded.
        if len(ans[key]) == limit:
            limit_warning = f"JSON response limit size reached for {key} GeoDataFrame. Increase the limit to ensure the full dataset is downloaded."
            warnings.warn(limit_warning)
    return ans


# Export dict of GeoDataFrames to a GeoPackage
def gdf_dict_to_gpkg(gdf_dict, path):
    for key in gdf_dict:
        gdf_dict[key].to_file(path, layer=key, driver="GPKG")


# Produce a GeoPackage from a dict of URLs
# Return the dict of GeoDataFrames produced during processing
def urls_to_gpkg(url_dict, path, max_size=500000):
    my_gdf_dict = gdf_dict(url_dict, max_size)
    gdf_dict_to_gpkg(my_gdf_dict, path)
    return my_gdf_dict
