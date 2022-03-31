"""Main script for Transponster"""
import argparse
from pathlib import Path
from queue import Queue

# pylint: disable=wrong-import-order
import transponster.logging as _  # Control the logging, needs to be imported before partisan.irods
from partisan.irods import Collection
from structlog import get_logger

from transponster.download_thread import DownloadThread
from transponster.processing_thread import ProcessingThread
from transponster.upload_thread import UploadThread
from transponster.util import JobBatch, Script


def main():
    """Entry point."""
    LOGGER = get_logger()

    parser = argparse.ArgumentParser("transponster")
    parser.add_argument("--input_collection", required=True)
    parser.add_argument("--output_files_path", required=True)
    parser.add_argument("--script", required=True)
    parser.add_argument("--scratch_location")
    parser.description = "Execute a script on files stored in iRODS"
    args = parser.parse_args()

    MAX_DOWNLOADED_FILES = 2

    scratch_location = (
        Path(args.scratch_location).resolve()
        if args.scratch_location is not None
        else None
    )
    input_collection_path = args.input_collection

    # Check script exists
    script_path = Path(args.script).resolve()
    if not script_path.exists():
        raise Exception(f"Script {script_path} does not exist, exiting")

    script = Script(script_path)

    download_queue = Queue()
    downloaded_queue = Queue(maxsize=MAX_DOWNLOADED_FILES)
    upload_queue = Queue(maxsize=MAX_DOWNLOADED_FILES)

    my_collection = Collection(input_collection_path)
    if not my_collection.exists():
        raise Exception(f"Error: Collection {input_collection_path} does not exist.")

    # The following loop can be improved to allow batching of processing inputs.
    n_batches = 0
    for obj in my_collection.iter_contents():
        if isinstance(obj, Collection):
            raise NotImplementedError(
                f" Collection {obj.path} found. Subcollections are not yet supported"
            )

        LOGGER.info(f"Adding {obj.name} as a job batch")
        job_batch = JobBatch(scratch_location=scratch_location)
        job_batch.add_input_obj(obj)
        download_queue.put(job_batch)

        n_batches += 1

    download_thread = DownloadThread(
        my_collection, download_queue, downloaded_queue, scratch_location
    )
    processing_thread = ProcessingThread(downloaded_queue, upload_queue, script)
    upload_thread = UploadThread(
        Collection(args.output_files_path), upload_queue, n_batches
    )

    download_thread.start()
    processing_thread.start()
    upload_thread.start()

    download_thread.join()
    processing_thread.done = True
    processing_thread.join()
    upload_thread.done = True
    upload_thread.join()


if __name__ == "__main__":
    main()
