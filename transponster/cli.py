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

"""CLI argument parser and logging setup."""
import argparse
import logging
import structlog


parser = argparse.ArgumentParser("transponster")
input_group = parser.add_mutually_exclusive_group(required=True)
input_group.add_argument("-i", "--input_collection")
input_group.add_argument("-f", "--input_list_file")
parser.add_argument("-o", "--output_collection", required=True)
parser.add_argument("-s", "--script", required=True)
parser.add_argument("--scratch_location")
parser.add_argument("-n", "--max_items_per_stage", type=int, default=1)
parser.add_argument("--batch_size", type=int, default=1)
parser.add_argument(
    "-p", "--progress_bar", action=argparse.BooleanOptionalAction, default=False
)
parser.add_argument(
    "-v", "--verbose", action=argparse.BooleanOptionalAction, default=False
)
parser.description = """Execute a script on files stored in iRODS.
    The script must take as input a folder, and place its ouput in
    a folder named 'output' which will be created for it."""

args = parser.parse_args()

if args.verbose:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
    )
else:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )
