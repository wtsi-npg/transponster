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
"""Upload thread."""

from queue import Queue
from threading import Lock, Thread

from partisan.irods import Collection
from structlog import get_logger


from transponster.util import (
    ClosedException,
    ErrorType,
    FailedJobBatch,
    JobBatch,
    WrappedQueue,
)


class UploadThread(Thread):
    """Upload files to iRODS."""

    def __init__(
        self,
        upload_location: Collection,
        upload_queue: WrappedQueue,
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
        self._count = 0
        self._count_lock = Lock()

    def run(self):
        while not (self.upload_queue.empty() and self.done):
            self.logger.info("Waiting for next batch to upload")
            try:
                batch: JobBatch = self.upload_queue.get()
            except ClosedException:
                self.done = True
                break
            with self._count_lock:
                self._count += 1
            if batch is None:
                self.logger.info("Batch is empty due to previous error")
                continue
            errored = False

            self.logger.info(f"Upload: Got batch at folder {batch.tmp_dir.name}")

            for obj in batch.get_output_objs(self.upload_location):
                try:
                    obj.upload()
                    obj.remove_local_file()

                except Exception as exception:
                    failed_batch = FailedJobBatch(
                        batch, exception.__repr__, ErrorType.UPLOAD_FAILED
                    )
                    self.logger.error(failed_batch.get_error_message())
                    self.error_queue.put(failed_batch)
                    break

            if not errored:
                self.logger.info(f"Batch #{self.count} uploaded")

        self.logger.info("Upload thread done")

    @property
    def count(self):
        """Count of files that have been claimed by the upload thread."""
        with self._count_lock:
            return self._count
