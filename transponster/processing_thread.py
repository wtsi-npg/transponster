import os
from pathlib import Path
from queue import Queue
from threading import Thread
from shutil import rmtree

from transponster.util import LocalObject, Script, UploadBatch
from structlog import get_logger


class ProcessingThread(Thread):
    def __init__(self, downloaded: Queue, to_upload: Queue, script_to_run: Script):
        Thread.__init__(self)

        self.downloaded = downloaded
        self.to_upload = to_upload
        self.script = script_to_run
        self.done = False
        self.logger = get_logger()

    def run(self):

        while not (self.downloaded.empty() and self.done):

            input_obj: LocalObject = self.downloaded.get()
            working_dir = Path(input_obj.local_folder.name).resolve()
            input_folder_path = Path(working_dir, "input")

            self.logger.info(f"current directory is {os.getcwd()}")
            self.logger.info(f"Running script on {working_dir}")
            self.script.run(working_dir)
            self.logger.info(
                f"Finished running script on {working_dir}, removing input"
            )
            # Delete input file once done
            rmtree(input_folder_path)

            upload_batch = UploadBatch(list(), input_obj.tmp_folder)

            # Add all items to upload queue
            for (dirpath, dirnames, filenames) in os.walk(Path(working_dir, "output")):

                for fname in filenames:
                    fpath = Path(working_dir.parent, dirpath)
                    self.logger.info(f"Adding {fpath}/{fname} to upload queue")

                    local_obj = LocalObject(None, fname, fpath, input_obj.tmp_folder)
                    # self.to_upload.put(local_obj)

                    upload_batch.local_objs.append(local_obj)
            self.to_upload.put(upload_batch)

        self.logger.info("Processing thread done")
