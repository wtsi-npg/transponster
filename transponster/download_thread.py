"""Download thread
"""
from pathlib import Path
from queue import Queue
from threading import Thread
from partisan.irods import Collection
from structlog import get_logger

from transponster.util import JobBatch


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
            batch: JobBatch = self.to_download.get()

            for obj in batch.input_objs:

                obj.download()
            self.logger.info("Finished downloading in batch")
            self.downloaded.put(batch)

        self.logger.info("Download thread done")
