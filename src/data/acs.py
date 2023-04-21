import cenpy
import geopandas as gpd
import util


class CensusSource(util.Source):
    """Holds information about a source from the 2019 American Community Survey.

    Attributes:
    place: Representation of place (e.g. "New York, NY") to pass to cenpy query
    variables: List of census variable names to pass to cenpy query
    """

    def __init__(
        self, name: str, description: str, epsg: int, place: str, variables: list
    ):
        self.place = place
        self.variables = variables
        super().__init__(name=name, description=description, epsg=epsg)

    def get(self):
        """Use cenpy to request American Community Census data."""
        # Set source to ACS 2019 5-year estimates
        acs = cenpy.products.ACS(2019)
        # Download data for place name (clipping likely necessary)
        gdf = acs.from_place(
            place=self.place,
            level="tract",
            variables=self.variables,
            strict_within=False,
        )
        gdf.set_crs(self.epsg, inplace=True)
        return gdf


def get_census_acs_pop(crs=2263, mask=None):
    census_acs_pop = CensusSource(
        name="acs_population",
        description="Total population five-year estimates from ACS 2019.",
        place="New York, NY",
        variables=["B01003_001E"],
        epsg=3857,
    ).get()

    census_acs_pop.to_crs(crs, inplace=True)

    census_acs_pop.rename(
        columns={"B01003_001E": "population"}, inplace=True
    )

    if mask is not None:
        census_acs_pop = gpd.clip(census_acs_pop, mask)

    return census_acs_pop
