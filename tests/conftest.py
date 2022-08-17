# Copyright (c) 2022 Genome Research Ltd.
#
# Author: Adam Blanchet <ab59@sanger.ac.uk>
#
# This file is part of transponster.
#
# transponster is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
import os
from pathlib import Path
import pytest

from partisan.icommands import iput, imkdir, irm


@pytest.fixture
def irods_inputs():

    datafiles = Path("tests/data/datafiles/")
    remote_location = "/testZone/home/irods/"
    iput(datafiles, remote_location, recurse=True)
    try:
        yield remote_location + "datafiles"
    finally:
        irm(remote_location + "datafiles", recurse=True)


@pytest.fixture
def irods_output_dir():
    remote_location = "text_outputs"
    imkdir(remote_location)
    try:
        yield remote_location
    finally:
        irm(remote_location, recurse=True)


@pytest.fixture(scope="function")
def script_working_dir(tmp_path_factory):
    """A working dir for a script"""
    scratch: Path = tmp_path_factory.mktemp("tmp")
    os.mkdir(Path(scratch, "input"))
    os.mkdir(Path(scratch, "output"))

    yield scratch


@pytest.fixture(scope="session")
def scratch_folder(tmp_path_factory):
    """A scratch location to be used for JobBatch"""
    yield tmp_path_factory.mktemp("scratch")
