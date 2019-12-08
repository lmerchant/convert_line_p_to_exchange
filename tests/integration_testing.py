# Process testing data and save output

# Test files are in windows-1252 encoding with '\n' line endings

# Set testing data filename as environment variable

import pathlib
import os

from convert_line_p_to_exchange.convert_line_p_to_exchange import main as ctd


def main():

    # loop through files in tests/Data folder
    directory = pathlib.Path.cwd() / 'tests' / 'data'

    pathlist = directory.glob('**/*.csv')


    for path in pathlist:
         # because path is object not string
         filename = str(path)
         os.environ["TESTING_FILE"] = filename
         ctd()


if __name__ == '__main__':

  main()

