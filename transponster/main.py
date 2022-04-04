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

"""Main script for Transponster"""

from pathlib import Path
from queue import Queue

# To control the logging level, needs to be imported before partisan.irods
# pylint: disable=wrong-import-order
from transponster.cli import args
from partisan.irods import Collection

# pylint: disable=ungrouped-imports
from transponster.controller import Controller
from transponster.util import LOGGER, JobBatch, Script


def main():
    """Entry point."""

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
        args.progress_bar,
        args.max_items_per_stage,
        scratch_location,
    )

    controller.run()


if __name__ == "__main__":
    main()
