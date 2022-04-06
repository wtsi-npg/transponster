from transponster.input import scan_input_file


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
