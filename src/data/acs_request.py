import urllib
import cenpy
import util
import geopandas as gpd

# To do:
# Fix CRS issue
# Figure out correct variable code for total population--lots of tracts have 0


acs = cenpy.products.ACS(2018)

new_york = acs.from_place("New York, NY", level='tract', variables = ['B01003_001E'], strict_within=False)

queens = acs.from_county("Queens, NY", level='tract', variables = ['B01003_001E'])

new_york.to_file("new_york.gpkg")

# Borough boundaries
boroughs_sources = util.SourceDict(util.Source(
    name="boroughs",
    data_url="https://data.cityofnewyork.us/resource/7t3b-ywvw.geojson",
    info_url="https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm",
    description="Boundaries of NYC boroughs, water areas excluded."))

boroughs_dict = util.urls_to_gpkg(boroughs_sources.url_dict(), "boroughs.gpkg")
boroughs = boroughs_dict["boroughs"]
boroughs = boroughs.set_crs(4236)

clipped_census = gpd.clip(new_york, mask=boroughs.to_crs(3857))
clipped_census.to_file("clipped_census.gpkg")


