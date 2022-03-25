import os
from pathlib import Path
from queue import Queue
from threading import Thread

from transponster.util import LocalObject, Script


class ProcessingThread(Thread):
    def __init__(self, downloaded: Queue, to_upload: Queue, script_to_run: Script):
        Thread.__init__(self)

        self.downloaded = downloaded
        self.to_upload = to_upload
        self.script = script_to_run
        self.done = False

    def run(self):

        while not (self.downloaded.empty() and self.done):

            input_obj: LocalObject = self.downloaded.get()
            input_path = input_obj.get_local_path_relative()

            print(f"current directory is {os.getcwd()}")
            print(f"Running script on {input_path}")
            self.script.run(input_path)
            print(f"Finished running script on {input_path}, removing input")
            # Delete input file once done
            os.remove(input_path)

            # Add all items to upload queue
            for (dirpath, dirnames, filenames) in os.walk(input_path.parent):

                for fname in filenames:
                    fpath = Path(input_path.parent, dirpath)
                    print(f"Adding {fpath}/{fname} to upload queue")

                    local_obj = LocalObject(None, fname, fpath, input_obj.tmp_folder)
                    self.to_upload.put(local_obj)

        print("Processing thread done")
