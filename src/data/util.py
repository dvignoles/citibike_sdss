import gzip
import warnings

import geopandas as gpd
import requests


# Classes for handling sources
class Source:
    """Holds information about a web-based data source.

    Attributes:
    name: the user-defined name of the source
    description: the user-defined description of the source

    """

    def __init__(self, name: str, description: str, epsg: int):
        """"""
        self.name = name
        self.description = description
        self.epsg = epsg


class SourceDict:
    """Holds a dict of Source objects.

    Attributes:
    sources: dict of Source objects of the form {"name": Source}
    """

    sources = {}

    def __init__(self, sources=None):
        """"""
        if type(sources) == list:
            for source in sources:
                self.add(source)
        elif type(sources) in Source.__subclasses__():
            self.add(sources)
        elif sources is None:
            self.sources = {}
        else:
            raise TypeError

    def add(self, source):
        """Add a Source to the SourceDict.

        Arguments:
        source (Source): The Source object to be added.
        """

        if type(source) in Source.__subclasses__():
            self.sources[source.name] = source
        else:
            raise TypeError

    def remove(self, source):
        """Remove a Source from the SourceList.

        Arguments:
        source (Source or str): The Source object to be removed; can also be specified by a str corresponding to the Source's name.
        """
        if type(source) == str:
            self.sources.pop(source)
        elif type(source) in Source.__subclasses():
            self.sources.pop(source.name)
        else:
            raise TypeError

    def names(self):
        """Returns a list of the names of the sources in the SourceDict."""
        return [source for source in self.sources]

    def url_dict(self):
        """Returns a dict of the form {name : data_url} for the SourceDict."""
        return {source: self[source].data_url for source in self.sources}

    def get(self):
        data_dict = DataDict(self)
        return data_dict

    def __getitem__(self, name):
        """Returns the item in self.sources corresponding to name."""
        return self.sources[name]


class DataDict:
    """Holds a dict of GeoDataFrames produced by a SourceDict.

    attributes:
    data: dict of the form {name : GeoDataFrame}
    source_dict: associated SourceDict object
    """

    def __init__(self, source_dict):
        """Downloads data and creates dataframe for each Source in self.sources"""
        self.data = {}
        self.source_dict = source_dict
        for name in self.source_dict.sources:
            self.data[name] = self.source_dict[name].get()

    def __getitem__(self, name):
        """Returns the GeoDataFrame in self.data corresponding to name."""
        return self.data[name]

    def set_crs(self):
        """Sets the CRS of each GeoDataFrame in self.data by associated Sources."""
        for name in self.source_dict.sources:
            source = self.source_dict[name]
            gdf = self.data[name]
            epsg = source.epsg

            if gdf.crs is None:
                gdf.set_crs(epsg, inplace=True)
            elif gdf.crs.to_epsg() != epsg:
                gdf.set_crs(epsg, inplace=True)
                raise UserWarning(
                    f"Expected EPSG {epsg} is different from actual EPSG {source.epsg} of {name} layer."
                )

    def to_crs(self, epsg: int):
        for name in self.source_dict.sources:
            self.data[name].to_crs(epsg=epsg, inplace=True)

    def to_file(self, path):
        gdf_dict_to_gpkg(self.data, path)


# Get GeoJSON from URL
def json_response(url, limit=500000):
    """Returns JSON response for the url with the specified limit parameter."""
    my_params = {"$limit": limit}
    with requests.get(url, params=my_params) as response:
        return response.json()


def download_file(url, local_filename=None, chunk_size=8192, compress=False):
    """Download file from web to disk

    Arguments:
    url - web url of file
    local_filename - file name/path to download to
    chunk_size - size of chunks to stream download in. Useful for large files
    compress - gzip compress file
    """
    if local_filename is None:
        local_filename = url.split("/")[-1]
        if compress:
            local_filename += ".gz"

    if compress:
        assert str(local_filename).endswith(
            ".gz"
        ), "compressed file must have .gz extension"

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        f = gzip.open(local_filename, "wb") if compress else open(local_filename, "wb")
        for chunk in r.iter_content(chunk_size=chunk_size):
            f.write(chunk)
        f.close()
        return local_filename


# Create GeoDataFrame from URL
def gdf_from_url(url, limit=500000):
    """Requests the data from url and returns a GeoDataFrame.

    Arguments:
    url - The URL to which the request will be made. Should return a GeoJSON of type FeatureCollection.
    limit - The limit parameter for the request, indicating how many records will be requested.

    Returns:
    gdf - A GeoDataFrame with the data from url.
    """

    print(f"Downloading data from {url}...")
    response = json_response(url, limit)
    print("Creating GeoDataFrame...")
    gdf = gpd.GeoDataFrame.from_features(response)
    print("GeoDataFrame complete.\n")
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
        print(f"Requesting data for {key} layer...")
        ans[key] = gdf_from_url(url_dict[key], limit)

        # Warn the user if the GeoDataFrame reached the size limit;
        # this probably means that the full dataset was not downloaded.
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
    print("Creating GeoPackage...")
    for key in gdf_dict:
        print(f"Writing {key} layer...")
        gdf_dict[key].to_file(path, layer=key, driver="GPKG")
    print(f"GeoPackage written to {path}.")


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
    GeoDataFrames."""
    print("Creating dict of GeoDataFrames...")
    my_gdf_dict = gdf_dict(url_dict, max_size)
    print("GeoDataFrame dict complete.")
    print("Writing to GeoPackage...")
    gdf_dict_to_gpkg(my_gdf_dict, path)
    print(f"GeoPackage written to {path}.")
    return my_gdf_dict
