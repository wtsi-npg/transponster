import os
from pathlib import Path
from queue import Queue
import shutil
from subprocess import CalledProcessError

import pytest

from transponster.processing_thread import ProcessingThread
from transponster.util import (
    FailedJobBatch,
    LocalObject2,
    Script,
    WrappedQueue,
    JobBatch,
)


@pytest.fixture
def setup_input_queue():
    """Setup a mock queue of input files."""
    input_queue = WrappedQueue()

    for dirpath, _, filenames in os.walk("tests/data/datafiles"):
        filenames.sort()
        for filename in filenames:
            batch = JobBatch()

            shutil.copyfile(
                Path(dirpath, filename), Path(batch.input_folder_path, filename)
            )

            batch.input_objs.append(
                LocalObject2(
                    None,
                    filename,
                    batch.input_folder_path,
                    is_local=True,
                    is_remote=False,
                )
            )
            input_queue.put(batch)

    return input_queue


class TestFailures:
    def test_script_executable_not_found(self, setup_input_queue):
        """Test a script failing due to a wrong shebang."""
        input_queue = setup_input_queue
        assert isinstance(input_queue, WrappedQueue)
        output_queue = WrappedQueue()
        error_queue = Queue()

        script = Script("tests/data/scripts/shebang_not_found.sh")

        processing_thread = ProcessingThread(
            input_queue, output_queue, error_queue, script
        )

        processing_thread.start()
        processing_thread.done = True
        processing_thread.join()
        failed_batch: FailedJobBatch = error_queue.get()

        assert isinstance(failed_batch.exception, FileNotFoundError)

    def test_script_fails_on_certain_files(self, setup_input_queue):
        """Test a script failing only on certain inputs."""
        input_queue = setup_input_queue
        assert isinstance(input_queue, WrappedQueue)
        output_queue = WrappedQueue()
        errors_queue = Queue()

        script = Script(Path("tests/data/scripts/fails_on_7_and_13.sh").resolve())

        processing_thread = ProcessingThread(
            input_queue, output_queue, errors_queue, script
        )

        processing_thread.start()
        processing_thread.done = True
        processing_thread.join()

        assert not errors_queue.empty()

        failed_batch: FailedJobBatch = errors_queue.get()
        assert isinstance(failed_batch.exception, CalledProcessError)
        assert failed_batch.job_batch.input_objs[0].local_name == "13.txt"

        failed_batch: FailedJobBatch = errors_queue.get()
        assert isinstance(failed_batch.exception, CalledProcessError)
        assert failed_batch.job_batch.input_objs[0].local_name == "7.txt"

        assert errors_queue.empty()

    def test_not_executable(self, setup_input_queue):
        """Test running ProcessingThread with a non-executable script."""

        input_queue = setup_input_queue
        assert isinstance(input_queue, WrappedQueue)
        output_queue = WrappedQueue()
        errors_queue = Queue()

        script = Script(Path("tests/data/scripts/not_executable.sh").resolve())

        processing_thread = ProcessingThread(
            input_queue, output_queue, errors_queue, script
        )

        processing_thread.start()
        processing_thread.done = True
        processing_thread.join()

        assert not errors_queue.empty()

        while not errors_queue.empty():
            failed_batch: FailedJobBatch = errors_queue.get()
            assert isinstance(failed_batch.exception, PermissionError)
