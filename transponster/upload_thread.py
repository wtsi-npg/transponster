import os
from pathlib import Path
from queue import Queue
from threading import Thread

from partisan.irods import Collection, DataObject, log

from transponster.util import LocalObject


class UploadThread(Thread):
    def __init__(self, upload_location: Collection, upload_queue: Queue):
        Thread.__init__(self)

        self.upload_location = upload_location
        self.upload_queue = upload_queue
        self.done = False

    def run(self):

        while not (self.upload_queue.empty() and self.done):
            print("Waiting for next file to upload")
            local_obj: LocalObject = self.upload_queue.get()
            remote_path = Path(self.upload_location.path, local_obj.local_name)
            data_obj = DataObject(remote_path)
            local_path = local_obj.get_local_path()
            print(f"Going to upload file {local_path} to {remote_path}")

            data_obj.put(local_path)
            print(f"Finished uploading file {local_path} to {remote_path}")

            print(f"Deleting local file {local_path}")
            os.remove(local_path)

        print("Upload thread done")
