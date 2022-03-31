"""Upload thread."""

from queue import Queue
from threading import Thread

from partisan.irods import Collection
from structlog import get_logger


from transponster.util import ErrorType, FailedJobBatch, JobBatch


class UploadThread(Thread):
    """Upload files to iRODS."""

    def __init__(
        self,
        upload_location: Collection,
        upload_queue: Queue,
        error_queue: Queue,
        max_size: int,
    ):
        Thread.__init__(self)
        self.upload_location = upload_location
        self.upload_queue = upload_queue
        self.error_queue = error_queue
        self.done = False
        self.max_size = max_size
        self.logger = get_logger()
        self.count = 0

    def run(self):
        while not (self.upload_queue.empty() and self.done):
            self.logger.info("Waiting for next batch to upload")
            batch: JobBatch = self.upload_queue.get()

            self.count += 1
            if batch == None:
                self.logger.info("Batch is empty due to previous error")
                continue
            errored = False

            for obj in batch.get_output_objs(self.upload_location):
                try:
                    obj.upload()
                    obj.remove_local_file()

                except Exception as e:
                    failed_batch = FailedJobBatch(
                        batch, e.__repr__, ErrorType.FailedToUpload
                    )
                    self.logger.error(failed_batch.get_error_message())
                    self.error_queue.put(failed_batch)
                    break

            if not errored:
                self.logger.info(f"Batch #{self.count} uploaded")

        self.logger.info("Upload thread done")
