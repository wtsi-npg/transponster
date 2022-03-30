from queue import Queue
from threading import Thread

from partisan.irods import Collection
from structlog import get_logger
import progressbar

from transponster.util import JobBatch


class UploadThread(Thread):
    def __init__(self, upload_location: Collection, upload_queue: Queue, max_size: int):
        Thread.__init__(self)

        self.upload_location = upload_location
        self.upload_queue = upload_queue
        self.done = False
        self.max_size = max_size
        self.progress_bar = progressbar.ProgressBar(
            maxval=self.max_size,
            widgets=[
                progressbar.widgets.PercentageLabelBar(),
                progressbar.SimpleProgress(),
            ],
            redirect_stdout=True,
            redirect_stderr=True,
            poll_interval=1,
        )
        self.count = 0
        self.logger = get_logger()

    def run(self):
        self.progress_bar.start()
        while not (self.upload_queue.empty() and self.done):
            self.logger.info("Waiting for next batch to upload")
            batch: JobBatch = self.upload_queue.get()

            self.count += 1
            self.progress_bar.update(self.count)

            for obj in batch.get_output_objs(self.upload_location):
                obj.upload()
                obj.remove_local_file()

            self.logger.info(f"Batch #{self.count} uploaded")

        self.logger.info("Upload thread done")
