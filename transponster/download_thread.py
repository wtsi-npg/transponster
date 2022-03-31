"""Download thread.
"""
from pathlib import Path
from queue import Queue
from threading import Thread
from structlog import get_logger

from transponster.util import ErrorType, FailedJobBatch, JobBatch


class DownloadThread(Thread):
    """Download thread."""

    def __init__(
        self,
        to_download: Queue,
        downloaded: Queue,
        error_queue: Queue,
        scratch_location: Path,
    ) -> None:
        Thread.__init__(self)
        self.to_download = to_download
        self.downloaded = downloaded
        self.error_queue = error_queue
        self.scratch_location = scratch_location
        self.logger = get_logger()

    def run(self):

        while not self.to_download.empty():

            self.logger.info("Getting next obj to download")
            batch: JobBatch = self.to_download.get()
            errored = False
            for obj in batch.input_objs:
                try:
                    obj.download()
                except Exception as e:
                    errored = True
                    failed_batch = FailedJobBatch(
                        batch, e.__repr__(), ErrorType.FailedToDownload
                    )
                    self.logger.error(failed_batch.get_error_message())
                    self.error_queue.put(failed_batch)
                    break

            if not errored:
                self.logger.info("Finished downloading in batch")
                self.downloaded.put(batch)
            else:
                self.downloaded.put(None)

        self.logger.info("Download thread done")
