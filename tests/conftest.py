from pathlib import Path
import pytest

from partisan.icommands import iput, imkdir, irm


@pytest.fixture
def irods_inputs():

    imkdir("text_inputs")
    datafiles = Path("tests/data/datafiles/")
    remote_location = "/testZone/home/irods/"
    iput(datafiles, remote_location, recurse=True)
    try:
        yield remote_location + "datafiles"
    finally:
        irm(remote_location, recurse=True)


@pytest.fixture
def irods_output_dir():
    remote_location = "text_outputs"
    imkdir(remote_location)
    try:
        yield remote_location
    finally:
        irm(remote_location, recurse=True)
