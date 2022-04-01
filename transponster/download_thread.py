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

"""Download thread.
"""
from pathlib import Path
from queue import Queue
from threading import Thread
from structlog import get_logger

from transponster.util import ErrorType, FailedJobBatch, JobBatch, WrappedQueue


class DownloadThread(Thread):
    """Download thread."""

    def __init__(
        self,
        to_download: Queue,
        downloaded: WrappedQueue,
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
            self.logger.info(f"Download: Got batch at folder {batch.tmp_dir.name}")
            for obj in batch.input_objs:
                try:
                    obj.download()
                except Exception as exception:
                    errored = True
                    failed_batch = FailedJobBatch(
                        batch, exception.__repr__(), ErrorType.DOWNLOAD_FAILED
                    )
                    self.logger.error(failed_batch.get_error_message())
                    self.error_queue.put(failed_batch)
                    break

            if not errored:
                self.logger.info("Finished downloading files in batch")
                self.downloaded.put(batch)
            else:
                self.downloaded.put(None)

        self.downloaded.close()
        self.logger.info("Download thread done")
