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

"""Controller for the different threads"""

from dataclasses import dataclass
from logging import Logger
from pathlib import Path
from queue import Queue
import threading
from time import sleep

from structlog import get_logger
from partisan.irods import Collection
from progressbar import ProgressBar
import progressbar
from transponster.download_thread import DownloadThread
from transponster.processing_thread import ProcessingThread
from transponster.upload_thread import UploadThread


from transponster.util import FailedJobBatch, Script, WrappedQueue


@dataclass
class Controller:
    """Controller for the different threads."""

    n_batches: int
    input_queue: Queue = Queue()
    _downloaded: int = 0
    _processed: int = 0
    _uploaded: int = 0
    _started: bool = False

    def __init__(
        self,
        output_collection: Collection,
        script: Script,
        input_queue: Queue,
        n_batches: int,
        progressbar_enabled: bool,
        max_per_stage: int = 1,
        scratch_location: Path = None,
    ) -> None:
        self.input_queue = input_queue
        self.n_batches = n_batches
        self.done = False
        if progressbar_enabled:
            self._progressbar: ProgressBar = ProgressBar(
                max_value=n_batches,
                widgets=[
                    progressbar.widgets.PercentageLabelBar(),
                    progressbar.SimpleProgress(),
                ],
                redirect_stderr=True,
                redirect_stdout=True,
                poll_interval=1,
            )
        self._progressbar_enabled = progressbar_enabled

        # Set up the stages of the pipeline

        processing_queue = WrappedQueue(maxsize=max_per_stage)
        output_queue = WrappedQueue(maxsize=max_per_stage)
        self.error_queue = Queue()

        self.download_thread = DownloadThread(
            self.input_queue,
            processing_queue,
            self.error_queue,
            scratch_location,
        )
        self.processing_thread = ProcessingThread(
            processing_queue, output_queue, self.error_queue, script
        )
        self.upload_thread = UploadThread(
            output_collection,
            output_queue,
            self.error_queue,
            self.n_batches,
        )

    def run(self):
        """Start self"""

        logger: Logger = get_logger()

        self.download_thread.start()
        self.processing_thread.start()
        self.upload_thread.start()

        if self._progressbar_enabled:
            progress_thread = threading.Thread(target=self._progress_bar_worker)
            progress_thread.start()

        self.download_thread.join()
        logger.debug("Controller sees download thread is done")
        self.processing_thread.done = True
        self.processing_thread.join()
        self.upload_thread.done = True
        self.upload_thread.join()
        self.done = True
        if self._progressbar_enabled:
            progress_thread.join()

        if self.error_queue.empty():
            logger.info("All jobs completed successfully!")
            return

        # Error reporting

        message = "The following errors occured:\n\n"

        while not self.error_queue.empty():
            failed_batch: FailedJobBatch = self.error_queue.get()

            inputs = failed_batch.get_input_object_locations()

            message += f"Error: {failed_batch.get_error_message()}\n"
            message += "\t Error occured for the following inputs:\n"
            for fname in inputs:
                message += f"\t\t{fname}\n"

            message += "\n"

        logger.error(message)

    def _progress_bar_worker(self):

        self._progressbar.start()
        while not self.done:
            sleep(0.5)
            self._progressbar.update(value=self.upload_thread.count)
        self._progressbar.finish()
