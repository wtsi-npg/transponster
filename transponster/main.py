"""Main script for Transponster"""
import argparse
from pathlib import Path
from queue import Queue

# pylint: disable=wrong-import-order
import transponster.logging as _  # Control the logging, needs to be imported before partisan.irods
from partisan.irods import Collection
from structlog import get_logger

from transponster.controller import Controller
from transponster.util import JobBatch, Script


def main():
    """Entry point."""
    LOGGER = get_logger()

    parser = argparse.ArgumentParser("transponster")
    parser.add_argument("-i", "--input_collection", required=True)
    parser.add_argument("-o", "--output_collection", required=True)
    parser.add_argument("-s", "--script", required=True)
    parser.add_argument("--scratch_location")
    parser.add_argument("-n", "--max_items_per_stage", type=int, default=1)
    parser.description = """Execute a script on files stored in iRODS. 
        The script must take as input a folder, and place its ouput in
        a folder named 'output' which will be created for it."""
    args = parser.parse_args()

    if args.max_items_per_stage <= 0:
        raise Exception("max_items_per_stage must be strictly positive.")

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

    input_collection = Collection(args.input_collection)
    if not input_collection.exists():
        raise Exception(
            f"Error: Input Collection {input_collection_path} does not exist."
        )

    output_collection = Collection(args.output_collection)
    if not output_collection.exists():
        raise Exception(
            f"Error: Output Collection {args.output_collection} does not exsits."
        )
    # The following loop can be improved to allow batching of processing inputs.
    n_batches = 0
    download_queue = Queue()
    for obj in input_collection.iter_contents():
        if isinstance(obj, Collection):
            raise NotImplementedError(
                f" Collection {obj.path} found. Subcollections are not yet supported"
            )

        LOGGER.info(f"Adding {obj.name} as a job batch")
        job_batch = JobBatch(scratch_location=scratch_location)
        job_batch.add_input_obj(obj)
        download_queue.put(job_batch)

        n_batches += 1

    controller = Controller(
        output_collection,
        script,
        download_queue,
        n_batches,
        args.max_items_per_stage,
        scratch_location,
    )

    controller.run()


if __name__ == "__main__":
    main()
