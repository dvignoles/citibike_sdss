"""Dataset download and clean driver script"""
import logging
import click
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import open_data
import gbfs


def make_open_data(project_dir, logger=None):

    if logger is not None:
        logger.info("downloading NYC Open Data")

    open_data.open_data_to_gpkg(
        path=project_dir.joinpath("data/raw/opendata.gpkg"), limit=2000000
    )


def make_gbfs_stations(project_dir, logger=None):

    if logger is not None:
        logger.info("downloading Citi Bike GBFS Station Information")

    stations = gbfs.Stations()
    stations._download_raw(
        output_file=project_dir.joinpath("data/raw/station_info.json.gz")
    )
    stations.process(output_file=project_dir.joinpath("data/processed/gbfs.gpkg"))


def make_all(project_dir, logger=None):
    make_open_data(project_dir, logger=logger)
    make_gbfs_stations(project_dir, logger=logger)


@click.group()
@click.pass_context
def cli(ctx):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    ctx.ensure_object(dict)
    ctx.obj['logger'] = logging.getLogger(__name__)


@cli.command(help='Get NYC OpenData')
@click.pass_context
def get_opendata(ctx):
    make_open_data(ctx.obj['project_dir'], logger=ctx.obj['logger'])


@cli.command(help='Get GBFS Station Status')
@click.pass_context
def get_stations(ctx):
    make_gbfs_stations(ctx.obj['project_dir'], logger=ctx.obj['logger'])


@cli.command(help='Get all datasets')
@click.pass_context
def get_all(ctx):
    make_all(ctx.obj['project_dir'], logger=ctx.obj['logger'])


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    cli(obj={'project_dir': project_dir})
