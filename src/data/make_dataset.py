"""Dataset download and clean driver script"""
import logging
from datetime import date
from pathlib import Path

import acs
import citibike
import click
import gbfs
import geopandas as gpd
import mta
import open_data
import sas
from dotenv import find_dotenv, load_dotenv

OPEN_DATA_GPKG = "data/prepared/open_data.gpkg"
ACS_GPKG = "data/prepared/acs.gpkg"
GBFS_GPKG = "data/prepared/gbfs.gpkg"
SAS_GPKG = "data/prepared/sas.gpkg"


def make_open_data(project_dir, logger=None):
    if logger is not None:
        logger.info("downloading NYC Open Data")

    # Create util.SourceDict of open data sources
    open_sources = open_data.open_data_sources()

    # Download data from util.SourceDict into util.DataDict
    open_data_dict = open_sources.get()

    # Project, filter, and clean data
    open_data_dict.set_crs()
    open_data_dict.to_crs(2263)
    open_data.clean_open_data(open_data_dict)

    # Write open_data to GeoPackage
    open_data_dict.to_file(project_dir.joinpath(OPEN_DATA_GPKG))


def make_census_pop(project_dir, logger=None):
    if logger is not None:
        logger.info("downloading ACS Census population")

    if project_dir.joinpath(OPEN_DATA_GPKG).exists():
        mask = gpd.read_file(project_dir.joinpath(OPEN_DATA_GPKG), layer="boroughs")
    else:
        mask = open_data.boroughs().get().set_crs(4326).to_crs(2263)

    gdf = acs.get_census_acs_pop(crs=2263, mask=mask)

    gdf.to_file(project_dir.joinpath(ACS_GPKG))


def make_gbfs_stations(project_dir, logger=None):
    if logger is not None:
        logger.info("downloading Citi Bike GBFS Station Information")

    stations = gbfs.Stations()
    stations._download_raw(
        output_file=project_dir.joinpath("data/raw/station_info.json.gz")
    )
    stations.process(output_file=project_dir.joinpath(GBFS_GPKG))


def make_gbfs_status(project_dir, logger):
    logger.info("downloading Citi Bike GBFS Station Information")

    output_file = project_dir.joinpath(GBFS_GPKG)
    raw_dir = project_dir.joinpath("data/raw/station_status")
    status = gbfs.StationStatus(output_file, raw_dir)
    count = status.process()
    logger.info(f"{count} GBFS captures processed")


def make_sas_infill(project_dir, logger):
    logger.info("downloading Citi Bike Infill Suggest A Station")
    output_file = project_dir.joinpath(SAS_GPKG)
    infill = sas.SuggestAStation()
    infill.process(output_file)


def make_mta_turnstile(project_dir, logger):
    # manually prepared lookup tables
    remote_lookup_csv = project_dir.joinpath("data/raw/mta/remote_complex_lookup.csv")
    stations_csv = project_dir.joinpath("data/raw/mta/stations.csv")

    # TODO: Parameterize year range
    for year in range(2019, 2024):
        raw_dir = project_dir.joinpath("data", "raw", "mta", "turnstile", str(year))
        raw_dir.mkdir(parents=True, exist_ok=True)

        raw_gpkg = raw_dir.parent.joinpath(f"mta_{year}.gpkg")
        ts = mta.MtaTurnstiles(
            raw_dir,
            raw_gpkg,
            start_date=date(year, 1, 1),
            end_date=date(year + 1, 1, 1),
        )

        logger.info(f"downloading MTA Turnstile Data {year}")
        ts.download_raw()

        logger.info(f"initializing MTA geopackage {year}")
        ts.setup_gpkg(remote_lookup_csv, stations_csv, replace=True, crs=2263)

        logger.info(f"processing turnstile data {year}")
        ts.raw_to_gpkg()

        # move to prepared data
        prepared_gpkg = project_dir.joinpath("data", "prepared", f"mta_{year}.gpkg")
        if prepared_gpkg.exists():
            prepared_gpkg.unlink()

        raw_gpkg.rename(prepared_gpkg)


def make_citibike_trips(project_dir, logger):
    today = date.today()
    prepared_dir = project_dir.joinpath("data", "prepared")
    if not prepared_dir.exists():
        prepared_dir.mkdir(parents=True)

    # TODO: Parameterize year range
    for year in range(2019, today.year + 1):
        raw_dir = project_dir.joinpath("data", "raw", "cb_tripdata", str(year))
        if not raw_dir.exists():
            raw_dir.mkdir(parents=True)

        year_gpkg = prepared_dir.joinpath(f"citibike_trips_{year}.gpkg")
        end_month = today.month - 1 if year == today.year else 12
        td = citibike.TripData(raw_dir, year_gpkg, year, 1, year, end_month)
        logger.info(f"downloading Citi Bike trip data {year}")
        td.download_raw()
        logger.info(f"preparing Citi Bike trip data {year}")
        td.to_gpkg(replace=True, crs=2263)


def make_all(project_dir, logger):
    make_open_data(project_dir, logger)
    make_census_pop(project_dir, logger)
    make_gbfs_stations(project_dir, logger)
    make_gbfs_status(project_dir, logger)
    make_sas_infill(project_dir, logger)
    make_mta_turnstile(project_dir, logger)
    make_citibike_trips(project_dir, logger)


@click.group()
@click.pass_context
def cli(ctx):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    ctx.ensure_object(dict)
    ctx.obj["logger"] = logging.getLogger(__name__)


@cli.command(help="Get NYC OpenData")
@click.pass_context
def get_opendata(ctx):
    make_open_data(ctx.obj["project_dir"], logger=ctx.obj["logger"])


@cli.command(help="Get GBFS Station Information")
@click.pass_context
def get_stations(ctx):
    make_gbfs_stations(ctx.obj["project_dir"], logger=ctx.obj["logger"])


@cli.command(help="Get GBFS Station Status")
@click.pass_context
def get_status(ctx):
    make_gbfs_status(ctx.obj["project_dir"], logger=ctx.obj["logger"])


@cli.command(help="Get ACS Census Population")
@click.pass_context
def get_acs_population(ctx):
    make_census_pop(ctx.obj["project_dir"], logger=ctx.obj["logger"])


@cli.command(help="Get Suggest A Station Infill")
@click.pass_context
def get_sas_infill(ctx):
    make_sas_infill(ctx.obj["project_dir"], logger=ctx.obj["logger"])


@cli.command(help="Get MTA Turnstile Records")
@click.pass_context
def get_mta_turnstile(ctx):
    make_mta_turnstile(ctx.obj["project_dir"], logger=ctx.obj["logger"])


@cli.command(help="Get Citi Bike Trip Data")
@click.pass_context
def get_citibike_trips(ctx):
    make_citibike_trips(ctx.obj["project_dir"], logger=ctx.obj["logger"])


@cli.command(help="Get all datasets")
@click.pass_context
def get_all(ctx):
    make_all(ctx.obj["project_dir"], logger=ctx.obj["logger"])


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    cli(obj={"project_dir": project_dir})
