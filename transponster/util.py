from fileinput import filename
from os import PathLike
from pathlib import Path
import subprocess


class LocalObject:
    def __init__(self, data_obj, local_name, local_folder, tmp_folder) -> None:
        self.data_obj = data_obj
        self.local_name = local_name
        self.local_folder = local_folder
        self.tmp_folder = tmp_folder

    def get_local_path_relative(self):
        return Path(self.local_folder.name, self.local_name)

    def get_local_path(self):
        return Path(self.local_folder, self.local_name)


class Script:
    """A script to run on an input file and which produces an output file

    It must meet the following conditions:
        - the script must take one input file
        - the script must produce output files in a folder named "output"
    """

    def __init__(self, path: PathLike) -> None:

        self.path = path

    def run(self, input_path: PathLike):
        working_directory = Path(input_path).parent
        filename = Path(input_path).name
        print(f"run script in directory {working_directory}")
        # input("WAIT HERE WAIT HERE WAIT HERE WAIT HERE")
        subprocess.run([self.path, filename], cwd=working_directory)
        print("script run")
