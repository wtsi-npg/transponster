from pathlib import Path
from queue import Queue
from math import ceil

from partisan.irods import Collection

from transponster.input import (
    gen_download_queue_from_collection,
    gen_download_queue_from_file,
    scan_input_file,
)
from transponster.util import JobBatch


class TestInput:
    def test_load_input_objects_from_file(self):

        expected = [
            "/seq/POG123/pass/1.fast5",
            "/seq/POG123/pass/2.fast5",
            "/seq/POG123/pass/3.fast5",
            "/seq/POG123/pass/4.fast5",
            "/seq/POG123/pass/5.fast5",
            "/seq/POG123/pass/6.fast5",
        ]

        actual = scan_input_file("tests/data/input_list")

        assert actual == expected

    def test_get_objs_from_irods_from_file(self, irods_inputs, tmp_path_factory):
        _ = irods_inputs
        scratch = tmp_path_factory.mktemp("tmp")

        input_file_path = Path("tests/data/datafiles_list")

        queue = Queue()

        datafiles = [f"{i}.txt" for i in range(1, 15)]

        n_batches = gen_download_queue_from_file(
            input_file_path, queue, scratch_location=scratch, batch_size=1
        )

        assert n_batches == 14

        while not queue.empty():

            batch: JobBatch = queue.get()

            for obj in batch.input_objs:

                assert obj.local_name in datafiles
                datafiles.remove(obj.local_name)

    def test_batch_size_collection(self, irods_inputs, tmp_path_factory):
        scratch = tmp_path_factory.mktemp("tmp")
        actual_n_files = 15

        for i in range(1, actual_n_files + 1):
            download_queue = Queue()
            collection = Collection(irods_inputs)

            n_batches = gen_download_queue_from_collection(
                collection,
                download_queue,
                scratch_location=scratch,
                batch_size=i,
            )

            assert n_batches == ceil(actual_n_files / i)

            count = 0
            while not download_queue.empty():
                _ = download_queue.get()
                count += 1

            assert n_batches == count

    def test_batch_size_from_file(self, irods_inputs, tmp_path_factory):
        _ = irods_inputs
        scratch = tmp_path_factory.mktemp("tmp")
        actual_n_files = 14

        for i in range(1, actual_n_files + 1):
            download_queue = Queue()

            n_batches = gen_download_queue_from_file(
                "tests/data/datafiles_list",
                download_queue,
                scratch_location=scratch,
                batch_size=i,
            )

            assert n_batches == ceil(actual_n_files / i)

            count = 0
            while not download_queue.empty():
                _ = download_queue.get()
                count += 1

            assert n_batches == count
