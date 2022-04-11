from os import PathLike
from queue import Queue
from typing import List

from structlog import get_logger

from partisan.irods import Collection, DataObject

from transponster.util import JobBatch

LOGGER = get_logger()


def scan_input_file(path: PathLike) -> List[PathLike]:

    with open(path, "r") as input_file:
        return [s.strip() for s in input_file.readlines()]


def gen_download_queue_from_collection(
    input_collection: Collection,
    download_queue: Queue,
    scratch_location: PathLike,
    batch_size: int = 1,
) -> int:
    """Populate the download queue with batches to be downloaded from an iRODS Collection.

    Args:
        input_collection: the iRODS Collection in which to search.
        download_queue: the queue to populate.
        scratch_location: PathLike for the scratch space location.
        batch_size: the number of items per batch.

    Returns:
        The number of batches added to the download queue.
    """
    n_batches = 0

    collection_contents = input_collection.contents()

    for start_index in range(0, len(collection_contents), batch_size):

        end_index = min(start_index + batch_size, len(collection_contents))
        job_batch = JobBatch(scratch_location=scratch_location)
        for obj in collection_contents[start_index:end_index]:

            if isinstance(obj, Collection):
                raise NotImplementedError(
                    f"Collection {obj.path} found. Subcollections are not yet supported"
                )

            LOGGER.info(f"Adding {obj.name} to a job batch")

            job_batch.add_input_obj(obj)

        LOGGER.info(f"Adding {job_batch} to download_queue")
        download_queue.put(job_batch)

        n_batches += 1

    return n_batches


def gen_download_queue_from_file(
    file: PathLike,
    download_queue: Queue,
    scratch_location: PathLike,
    batch_size: int = 1,
):
    """Populate the download queue with items listed in a file, to be downloaded from iRODS.

    Args:
        file: The path to the file containing the newline-separated iRODS locations.
        download_queue: The download queue to populate.
        scratch_location: PathLike for the scratch space location.
        batch_size: the number of items per batch.

    Returns:
        The number of batches added to the download queue.
    """

    input_paths: List[PathLike] = scan_input_file(file)
    n_batches = 0
    for start_index in range(0, len(input_paths), batch_size):

        end_index = min(start_index + batch_size, len(input_paths))
        job_batch = JobBatch(scratch_location=scratch_location)
        for path in input_paths[start_index:end_index]:

            obj = DataObject(path)

            LOGGER.info(f"Adding {obj.name} to a job batch")

            job_batch.add_input_obj(obj)

        LOGGER.info(f"Adding {job_batch} to download_queue")
        download_queue.put(job_batch)

        n_batches += 1

    return n_batches
