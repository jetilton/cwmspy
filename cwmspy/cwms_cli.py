import click
import sys
import fileinput
import re

from .core import CWMS


cwms = None


@click.group()
def cli():
    pass


@cli.command("connect")
@click.option("--verbose", default=False)
def connect(verbose):
    global cwms
    cwms = CWMS(verbose=verbose)
    cwms.connect()


@cli.command("close")
def close():
    global cwms
    cwms.close()


@cli.command("load")
@click.option("--fn", default=False)
def load(fn):
    if fn:
        f = open(fn, "r")
        string = f.read()
    else:
        string = sys.stdin
    data_list = re.split("-{3,}", string)
    for data in data_list:
        p_cwms_ts_id = list(data.keys())[0]
        timezone = data["timezone"]
        units = data["units"]
        times = [k for k, v in data["timeseries"].items()]
        values = [v for k, v in data["timeseries"].items()]


@cli.command("test")
@click.option("--verbose", default=False)
def test(verbose):
    if verbose:
        print(True)
    else:
        print(False)


if __name__ == "__main__":
    cli()


string = """

askjfdlksjf;lsajkflsjf
---
alsfdj;lsjflskjf
-
asfjdk;lsakjfjf
"""

f = open("insta.txt")
insta = f.read()
f.close()

import re

insta.replace("\n", "")
insta_list = re.split("-{3,}", insta)

import json

s = json.loads(insta_list[-2])
