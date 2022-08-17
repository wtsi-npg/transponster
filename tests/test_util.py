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
from pathlib import Path

import os
from subprocess import SubprocessError
import threading
from time import sleep
from transponster.util import (
    FailedJobBatch,
    JobBatch,
    LocalObject2,
    Script,
    WrappedQueue,
    ClosedException,
)
from pytest import raises
from partisan.irods import DataObject, Collection


class TestWrappedQueue:
    def test_empty_closed(self):

        queue = WrappedQueue()
        queue.close()
        with raises(ClosedException):
            queue.get()

    def test_not_empty_closed(self):

        queue = WrappedQueue()
        queue.put("Hello")
        queue.close()
        assert queue.get() == "Hello"
        with raises(ClosedException):
            queue.get()

    def test_fail_put_to_closed_queue(self):

        queue = WrappedQueue()
        queue.close()
        with raises(ClosedException):
            queue.put("Fail")

    def successfully_notify_closed():
        queue = WrappedQueue()

        def worker():
            with raises(ClosedException):
                queue.get()
                queue.get()

        thread = threading.Thread(target=worker)
        queue.put(123)
        sleep(3)
        queue.close()
        thread.join()


class TestScript:
    def test_run(self, script_working_dir):
        """Test running a script."""
        script = Script("./tests/data/scripts/hello_output.sh")
        script.run(script_working_dir)

        # Check that new file is created containing a message
        output_file_location = Path(script_working_dir, "output", "output.txt")
        assert os.path.exists(output_file_location)
        with open(output_file_location, "r") as actual_file:

            assert actual_file.read() == "Hello, world!\n"

    def test_failed_script(self, script_working_dir):
        """Test a failing script"""

        script = Script("./tests/data/scripts/fail.sh")

        with raises(SubprocessError):
            script.run(script_working_dir)


class TestLocalObject2:
    """Tests for the TestLocalObject2 class"""

    def test_remove_object(self, script_working_dir):
        """Test removing an object from local storage."""

        to_delete_path = Path(script_working_dir, "input/to_delete.txt")

        open(to_delete_path, "a").close()

        localobj2 = LocalObject2(
            None,
            "to_delete.txt",
            Path(script_working_dir, "input"),
            is_local=True,
            is_remote=False,
        )

        localobj2.remove_local_file()

        assert not localobj2.is_local

        assert not os.path.exists(to_delete_path)

        with raises(Exception, match="Cannot remove a file that is not local"):
            localobj2.remove_local_file()

    def test_download_object(self, script_working_dir, irods_inputs):
        """Test downloading an object from iRODS."""

        _ = irods_inputs
        input_folder = Path(script_working_dir, "input")

        localobj2 = LocalObject2(
            DataObject("/testZone/home/irods/datafiles/2.txt"),
            "input.txt",
            input_folder,
        )

        localobj2.download()

        assert localobj2.is_local

        local_fname = Path(input_folder, localobj2.local_name)
        assert os.path.exists(local_fname)

        with open(local_fname, "r") as local_file:

            assert local_file.read() == "File number 2"

        # Downloading again should raise exception
        with raises(Exception):
            localobj2.download()

    def test_upload_object(self, script_working_dir):
        """Test uploading an object to iRODS."""

        localobj2 = LocalObject2(
            DataObject("/testZone/home/irods/destination.txt"),
            local_name="2.txt",
            local_folder="tests/data/datafiles",
            is_local=True,
            is_remote=False,
        )

        localobj2.upload()

        assert localobj2.data_obj.exists()

        redownloaded_path = Path(script_working_dir, "destination.txt")
        localobj2.data_obj.get(redownloaded_path)
        with open(redownloaded_path, "r") as redownloaded:
            assert redownloaded.read() == "File number 2"

        with raises(Exception, match="Cannot upload a LocalObject2 twice."):
            localobj2.upload()

    def test_upload_nonexistent_file(self):
        """Test uploading a nonexistent file."""

        localobj2 = LocalObject2(
            None,
            "doesnotexist",
            "/definitelydoesntexist",
            is_local=True,
            is_remote=False,
        )

        with raises(Exception):
            localobj2.upload()

    def test_upload_non_local_file(self):

        localobj2 = LocalObject2(
            None,
            "doesntmatter",
            "wherethefileis",
            is_local=False,
            is_remote=False,
        )

        with raises(
            Exception, match="Cannot upload a LocalObject2 which is not local."
        ):
            localobj2.upload()


class TestJobBatch:
    def test_output_objs_getter(self, tmp_path_factory):
        """Test getting all output from JobBatch."""
        scratch: Path = tmp_path_factory.mktemp("tmp")
        job_batch = JobBatch(scratch_location=scratch)
        # Create a bunch of output files
        for i in range(16):
            open(Path(job_batch.output_folder_path, f"file-{i}.txt"), "a").close()

        outputs = job_batch.get_output_objs(Collection("/testZone/home/irods/outputs"))

        assert len(outputs) == 16

        paths = [f"file-{i}.txt" for i in range(16)]

        for output in outputs:
            assert output.is_local
            assert not output.is_remote
            assert output.local_folder == job_batch.output_folder_path
            assert output.local_name in paths
            paths.remove(output.local_name)

        assert len(paths) == 0

    def test_add_input_obj(self, irods_inputs, scratch_folder):
        """Test adding an input object to JobBatch."""

        to_add = DataObject(irods_inputs + "/2.txt")

        job_batch = JobBatch(scratch_location=scratch_folder)

        job_batch.add_input_obj(to_add)

        assert len(job_batch.input_objs) == 1

        input_obj = job_batch.input_objs[0]

        assert input_obj.local_name == "2.txt"
        assert not input_obj.is_local
        assert input_obj.is_remote

    def test_input_folder_path(self, tmp_path_factory):
        """Test correct input folder path."""
        scratch: Path = tmp_path_factory.mktemp("tmp")
        job_batch = JobBatch(scratch)

        assert (
            job_batch.input_folder_path.resolve()
            == Path(scratch, job_batch.tmp_dir.name, "input").resolve()
        )

    def test_output_folder_path(self, tmp_path_factory):
        """Test correct output folder path."""
        scratch: Path = tmp_path_factory.mktemp("tmp")
        job_batch = JobBatch(scratch)

        assert (
            job_batch.output_folder_path.resolve()
            == Path(scratch, job_batch.tmp_dir.name, "output").resolve()
        )


class TestFailedJobBatch:
    def test_cleanup_tmp(self, scratch_folder):

        job_batch = JobBatch(scratch_location=scratch_folder)

        # Populate folders with things
        for i in range(5):
            open(Path(job_batch.input_folder_path, f"f{i}.txt"), "a").close()
            open(Path(job_batch.output_folder_path, f"f{i}.txt"), "a").close()

        failed_batch = FailedJobBatch(job_batch, None, None)

        failed_batch.cleanup_tmp()

        assert not os.path.exists(job_batch.output_folder_path)
        assert not os.path.exists(job_batch.input_folder_path)
        assert not os.path.exists(job_batch.tmp_dir.name)

    def test_get_input_object_locations(self, scratch_folder, irods_inputs):

        job_batch = JobBatch(scratch_location=scratch_folder)
        input_paths = []
        for obj in Collection(irods_inputs).iter_contents():

            assert isinstance(obj, DataObject)
            input_paths.append(obj.name)
            job_batch.add_input_obj(obj)

        failed_batch = FailedJobBatch(job_batch, None, None)

        assert set(input_paths) == set(failed_batch.get_input_object_locations())
