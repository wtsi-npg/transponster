"""Download thread
"""
from pathlib import Path
from queue import Queue
from tempfile import TemporaryDirectory
from threading import Thread
from partisan.irods import Collection
from partisan.irods import DataObject
from partisan.irods import log

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

    def run(self):

        while not self.to_download.empty():

            print("Getting next obj to download")
            obj: DataObject = self.to_download.get()
            print(f"Got {obj.name} to download")
            my_tmp_dir = None
            if self.scratch_location:
                my_tmp_dir = TemporaryDirectory(
                    prefix="transponster-", dir=self.scratch_location
                )
            else:
                my_tmp_dir = TemporaryDirectory(prefix="transponster-")

            my_path = Path(my_tmp_dir.name, obj.name).resolve()

            try:
                obj.get(my_path, tries=5)
            except:
                print("ERROR ERROR ERROR")
            print(f"Downloaded to {my_path}")

            self.downloaded.put(LocalObject(obj, my_path, my_tmp_dir, my_tmp_dir))

        print("Download thread done")
