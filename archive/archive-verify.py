#!/usr/bin/env python3

import os
import boto3
import json
import hashlib
import argparse


# Parse command-line options
###########################################
def options():
    """Parse command-line options
    """
    parser = argparse.ArgumentParser(description="Verify files were archived into AWS and remove locally.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dir", help="Directory containing one or more files that were archived.", required=True)
    args = parser.parse_args()

    return args


# Main
###########################################
def main():
    """Main program.
    """
    args = options()

    # Recursively walk through the input directory
    for root, dirs, files in os.walk(args.dir):
        # We only need to worry about verifying files
        for filename in files:
            # Find the files that have .archived extensions
            if filename[-8:] == "archived":
                verify_archive(path=root, archive=filename)


def verify_archive(path, archive):
    # The archive should refer to the name of a file without the .archive extension, make sure it exists
    file = os.path.join(path, archive[:-9])
    if os.path.exists(file):
        # Read the archive metadata
        arxiv = open(os.path.join(path, archive))
        metadata = json.load(arxiv)
        arxiv.close()

        # Get a local checksum
        data = open(file, "rb")
        md5sum = hashlib.md5()
        # Loop over chunks of the file so we don't read it all into memory
        for chunk in iter(lambda: data.read(4096), b""):
            # Update the checksum with each chunk
            md5sum.update(chunk)
        md5str = md5sum.hexdigest()
        if md5str == metadata["ETag"]:
            print("File archived successfully!")
    else:
        # The file may have been removed already and can be ignored
        pass


if __name__ == "__main__":
    main()
