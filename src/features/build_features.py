"""Everything related to metric creation & transformation of datasets"""
import logging
from pathlib import Path

import click
from dotenv import find_dotenv, load_dotenv


@click.group()
@click.pass_context
def cli(ctx):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    ctx.ensure_object(dict)
    ctx.obj["logger"] = logging.getLogger(__name__)


@cli.command(help="placeholder command")
@click.pass_context
def placeholder(ctx):
    ctx.obj["logger"].info("Categorizing NTAs as residential / non-residential")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    cli(obj={"project_dir": project_dir})
