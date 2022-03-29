"""Download thread
"""
from pathlib import Path
from queue import Queue
from tempfile import TemporaryDirectory
from threading import Thread
from partisan.irods import Collection
from partisan.irods import DataObject
from partisan.irods import log
from os import mkdir
from structlog import get_logger

from transponster.util import LocalObject


class DownloadThread(Thread):
    """Download thread"""

    def __init__(
        self,
        collection: Collection,
        to_download: Queue,
        downloaded: Queue,
        scratch_location: Path,
    ) -> None:
        Thread.__init__(self)
        self.collection = collection
        self.to_download = to_download
        self.downloaded = downloaded
        self.scratch_location = scratch_location
        self.logger = get_logger()

    def run(self):

        while not self.to_download.empty():

            self.logger.info("Getting next obj to download")
            obj: DataObject = self.to_download.get()
            self.logger.info(f"Got {obj.name} to download")
            my_tmp_dir = None
            if self.scratch_location:
                my_tmp_dir = TemporaryDirectory(
                    prefix="transponster-", dir=self.scratch_location
                )
            else:
                my_tmp_dir = TemporaryDirectory(prefix="transponster-")

            input_folder_path = Path(my_tmp_dir.name, "input").resolve()
            mkdir(input_folder_path)
            my_path = Path(input_folder_path, obj.name).resolve()

            try:
                obj.get(my_path, tries=5)
            except:
                self.logger.info("ERROR ERROR ERROR")
            self.logger.info(f"Downloaded to {my_path}")

            self.downloaded.put(LocalObject(obj, my_path, my_tmp_dir, my_tmp_dir))

        self.logger.info("Download thread done")
