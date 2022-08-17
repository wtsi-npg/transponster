# Copyright (c) 2022 Genome Research Ltd.
#
# Author: Adam Blanchet <ab59@sanger.ac.uk>
#
# This file is part of transponster.
#
# transponster is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
"""Main script for Transponster"""

from pathlib import Path
from queue import Queue

# To control the logging level, needs to be imported before partisan.irods
# pylint: disable=wrong-import-order
from transponster.cli import args
from partisan.irods import Collection

# pylint: disable=ungrouped-imports
from transponster.controller import Controller
from transponster.util import Script
from transponster.input import (
    gen_download_queue_from_collection,
    gen_download_queue_from_file,
)


def main():
    """Entry point."""

    if args.max_items_per_stage <= 0:
        raise Exception("max_items_per_stage must be strictly positive.")

    if args.batch_size <= 0:
        raise Exception("batch_size must be strictly positive")

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

    output_collection = Collection(args.output_collection)
    if not output_collection.exists():
        raise Exception(
            f"Error: Output Collection {args.output_collection} does not exsits."
        )

    download_queue = Queue()
    n_batches = 0

    if args.input_list_file is not None:
        n_batches = gen_download_queue_from_file(
            args.input_list_file, download_queue, scratch_location, args.batch_size
        )
    elif args.input_collection is not None:
        input_collection = Collection(args.input_collection)
        if not input_collection.exists():
            raise Exception(
                f"Error: Input Collection {input_collection_path} does not exist."
            )
        n_batches = gen_download_queue_from_collection(
            input_collection, download_queue, scratch_location, args.batch_size
        )
    else:
        # Should never get here
        raise Exception("No input locations were provided")

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
