import os
from pathlib import Path
from queue import Queue
from threading import Thread

from partisan.irods import Collection, DataObject, log
from structlog import get_logger

from transponster.util import LocalObject, UploadBatch


class UploadThread(Thread):
    def __init__(self, upload_location: Collection, upload_queue: Queue, max_size: int):
        Thread.__init__(self)

        self.upload_location = upload_location
        self.upload_queue = upload_queue
        self.done = False
        self.logger = get_logger()
        self.max_size = max_size

    def run(self):

        while not (self.upload_queue.empty() and self.done):
            self.logger.info("Waiting for next batch to upload")
            # local_obj: LocalObject = self.upload_queue.get()


            batch: UploadBatch = self.upload_queue.get()

            for local_obj in batch.local_objs:

                remote_path = Path(self.upload_location.path, local_obj.local_name)
                data_obj = DataObject(remote_path)
                local_path = local_obj.get_local_path()
                self.logger.info(f"Going to upload file {local_path} to {remote_path}")

                data_obj.put(local_path)
                self.logger.info(f"Finished uploading file {local_path} to {remote_path}")

                self.logger.info(f"Deleting local file {local_path}")
                os.remove(local_path)


        self.logger.info("Upload thread done")
