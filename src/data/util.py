import geopandas as gp
import requests
import warnings


# Get GeoJSON from URL
def json_response(url, limit=500000):
    """Returns JSON response for the url with the specified limit parameter."""
    my_params = {"$limit": limit}
    with requests.get(url, params=my_params) as response:
        return response.json()


# Create GeoDataFrame from URL
def gdf_from_url(url, limit=500000):
    """Requests the data from url and returns a GeoDataFrame.

    Arguments:
    url - The URL to which the request will be made. Should return a GeoJSON of type FeatureCollection.
    limit - The limit parameter for the request, indicating how many records will be requested.

    Returns:
    gdf - A GeoDataFrame with the data from url."""
    response = json_response(url, limit)
    gdf = gp.GeoDataFrame.from_features(response)
    return gdf


# Create a dict of gdfs matching a dict of URLS
def gdf_dict(url_dict, limit=500000):
    """Creates a dict of GeoDataFrames from a dict of urls.

    Arguments:
    url_dict - A dict of the form {name : url}, where name is the name
    that the user will use to refer to the data from the URL. limit -
    The limit parameter for the request, indicating how many records
    will be requested.

    Returns:
    gdf_dict - A dict of the form {name : GeoDataFrame}, where the
    urls from url_dict are replaced with the corresponding
    GeoDataFrames.
    """
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
    """Writes a dict of GeoDataFrames as the layers of a GeoPackage.

    Arguments:
    gdf_dict - a dict of GDFs in the format returned by gdf_dict().
    path - the path to which the GeoPackage will be written."""
    for key in gdf_dict:
        gdf_dict[key].to_file(path, layer=key, driver="GPKG")


# Produce a GeoPackage from a dict of URLs
# Return the dict of GeoDataFrames produced during processing
def urls_to_gpkg(url_dict, path, max_size=500000):
    """Writes a GeoPackage with the data from a dict of URLs.

    Arguments:
    url_dict - A dict of the form {name : url}, where name is the name
    that the user will use to refer to the data from the URL.
    path - The path to which the GeoPackage will be written.

    Returns: 
    gdf_dict - A dict of the form {name : GeoDataFrame}, where the
    urls from url_dict are replaced with the corresponding
    GeoDataFrames.
"""
    print("Creating dict of GeoDataFrames...")
    my_gdf_dict = gdf_dict(url_dict, max_size)
    print("GeoDataFrame dict complete.")
    print("Writing to GeoPackage...")
    gdf_dict_to_gpkg(my_gdf_dict, path)
    print(f"GeoPackage written to {path}.")
    return my_gdf_dict
