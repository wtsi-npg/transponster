"""Useful types"""
from dataclasses import dataclass
import os
from os import PathLike, mkdir, remove
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from typing import List
from structlog import get_logger

from partisan.irods import DataObject, Collection


LOGGER = get_logger()


@dataclass
class LocalObject2:
    """Class to track the state of a file"""

    data_obj: DataObject
    local_name: str
    local_folder: Path
    is_local: bool = False
    is_remote: bool = True

    def download(self, tries=5):

        if self.is_local:
            raise Exception("Cannot download a LocalObject2 twice.")

        self.data_obj.get(Path(self.local_folder, self.local_name), tries=tries)
        self.is_local = True

    def upload(self, tries=5):

        if self.is_remote:
            raise Exception("Cannot upload a LocalObject2 twice.")

        if not self.is_local:
            raise Exception("Cannot upload a LocalObject2 which is not local.")

        self.data_obj.put(Path(self.local_folder, self.local_name), tries=tries)
        self.is_remote = True

    def remove_local_file(self):

        if not self.is_local:
            raise Exception("Cannot remove a file that is not local")

        remove(Path(self.local_folder, self.local_name))
        self.is_local = False


class LocalObject:
    def __init__(self, data_obj, local_name, local_folder, tmp_folder) -> None:
        self.data_obj = data_obj
        self.local_name = local_name
        self.local_folder = local_folder
        self.tmp_folder = tmp_folder

    def get_local_path_relative(self):
        return Path(self.local_folder.name, self.local_name)

    def get_local_path(self):
        return Path(self.local_folder, self.local_name)


class Script:
    """A script to run on an input file and which produces an output file

    It must meet the following conditions:
        - the script must take one input file
        - the script must produce output files in a folder named "output"
    """

    def __init__(self, path: PathLike) -> None:

        self.path = path

    def run(self, working_dir: PathLike):
        working_directory = Path(working_dir)
        input_folder = Path(working_dir, "input")
        LOGGER.info(f"run script in directory {working_directory}")
        # input("WAIT HERE WAIT HERE WAIT HERE WAIT HERE")
        process = subprocess.run(
            [self.path, input_folder],
            cwd=working_directory,
            capture_output=True,
            check=True,
        )
        LOGGER.info(process.stdout.decode())
        LOGGER.info(process.stderr.decode())
        LOGGER.info("script run")


class UploadBatch(object):
    def __init__(
        self, local_objs: List[LocalObject], tmp_dir: TemporaryDirectory
    ) -> None:

        self.local_objs = local_objs
        self.tmp_dir = tmp_dir


class JobBatch(object):
    """An object used to track files being processed."""

    input_objs: List[LocalObject2]

    def __init__(self, scratch_location=None) -> None:
        self.input_objs = []

        # Set up temporary folders
        self.tmp_dir = TemporaryDirectory(prefix="transponster-", dir=scratch_location)
        mkdir(Path(self.tmp_dir.name, "input"))
        mkdir(Path(self.tmp_dir.name, "output"))

    def get_output_objs(self, output_collection: Collection) -> List[LocalObject2]:
        """Get all objects to upload from the 'output' folder"""

        root = Path(self.tmp_dir.name, "output")
        objs: List[LocalObject2] = []

        for (dirpath, _, filenames) in os.walk(root):

            for fname in filenames:
                fpath = Path(root, dirpath)
                LOGGER.info(f"Adding {fpath}/{fname} to upload queue")
                data_obj = DataObject(Path(output_collection, fname))
                local_obj = LocalObject2(data_obj, fname, fpath, True, False)

                objs.append(local_obj)

        return objs

    def add_input_obj(self, obj: DataObject):
        """Add an object to the list of inputs"""
        local_name = obj.name
        local_folder = self.input_folder_path
        local_object = LocalObject2(
            obj, local_name, local_folder, is_local=False, is_remote=True
        )
        self.input_objs.append(local_object)

    @property
    def input_folder_path(self):
        """Input folder path for this batch"""
        return Path(self.tmp_dir.name, "input")

    @property
    def output_folder_path(self):
        """Output folder path for this batch"""
        return Path(self.tmp_dir.name, "output")
