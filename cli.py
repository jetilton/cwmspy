import click
import os
from cwmspy import CWMS
import pandas as pd
import logging


@click.group()
def cli():
    pass


@cli.command("loc")
@click.option("--loc")
@click.option("--verbose", is_flag=True)
@click.option("--name", default=None)
@click.option("--d", is_flag=True)
def loc(verbose, name, loc, d):
    cwms = CWMS(verbose=verbose)
    cwms.connect(name=name)
    try:
        if d:
            cwms.delete_location(loc, "DELETE TS DATA")
            cwms.delete_location(loc, "DELETE TS ID")
            cwms.delete_location(loc)
        else:
            cwms.store_location(p_location_id=loc)
    except:
        cwms.close()
        return 0

    cwms.close()


if __name__ == "__main__":
    cli()
