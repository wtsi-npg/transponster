# -*- coding: utf-8 -*-
#
# Copyright © 2022 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# @author Adam Blanchet <ab59@sanger.ac.uk>

"""Processing thread."""

from pathlib import Path
from queue import Queue
from subprocess import CalledProcessError, SubprocessError
from threading import Thread
from shutil import rmtree

from structlog import get_logger

from transponster.util import (
    ClosedException,
    ErrorType,
    FailedJobBatch,
    JobBatch,
    Script,
    WrappedQueue,
)


class ProcessingThread(Thread):
    """Run scripts on inputs and send to upload thread."""

    def __init__(
        self,
        downloaded: WrappedQueue,
        to_upload: WrappedQueue,
        error_queue: Queue,
        script_to_run: Script,
    ):
        Thread.__init__(self)

        self.downloaded = downloaded
        self.to_upload = to_upload
        self.error_queue = error_queue
        self.script = script_to_run
        self.done = False
        self.logger = get_logger()

    def run(self):

        while not (self.downloaded.empty() and self.done):
            self.logger.info("Waiting for next batch to process")
            try:
                job_batch: JobBatch = self.downloaded.get()
            except ClosedException:
                self.done = True
                break

            if job_batch is None:
                self.logger.info("Processing: Batch is empty do to previous error")
                self.to_upload.put(None)
                continue
            self.logger.info(
                f"Processing: Got batch at folder {job_batch.tmp_dir.name}"
            )
            working_dir = Path(job_batch.tmp_dir.name).resolve()
            input_folder_path = job_batch.input_folder_path

            self.logger.info(f"Running script on {working_dir}")
            try:
                self.script.run(working_dir)
            except SubprocessError as exception:
                self.put_failed_batch(job_batch, exception, ErrorType.PROCESSING_FAILED)
                continue
            except FileNotFoundError as exception:
                self.put_failed_batch(job_batch, exception, ErrorType.FILE_NOT_FOUND)
                continue

            self.logger.info(
                f"Finished running script on {working_dir}, removing input"
            )
            # Delete input file once done
            rmtree(input_folder_path)

            # Send the batch to the upload_thread
            self.to_upload.put(job_batch)

        self.to_upload.close()
        self.logger.info("Processing thread done")

    def put_failed_batch(
        self, batch: JobBatch, exception: Exception, error_type: ErrorType
    ):

        failed_batch = FailedJobBatch(batch, exception, error_type)
        self.logger.error(failed_batch.get_error_message())
        self.error_queue.put(failed_batch)
        self.to_upload.put(None)
