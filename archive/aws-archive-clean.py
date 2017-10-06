#!/usr/bin/env python3

import os
import argparse


# Parse command-line options
###########################################
def options():
    """Parse command-line options
    """
    parser = argparse.ArgumentParser(description="Remove local files that were archived successfully (verified).",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--file", help="Input file containing file verification results.", required=True)
    parser.add_argument("-i", "--interactive", help="Interactively decided whether to delete files.", default=False,
                        action="store_true")
    args = parser.parse_args()

    return args


# Main
###########################################
def main():
    """Main program.
    """
    args = options()

    log = open(args.file, "r")
    for row in log:
        row = row.rstrip("\n")
        file, verified = row.split("\t")
        if verified == "True" and os.path.exists(file):
            delete = True
            if args.interactive:
                while True:
                    response = input("Delete local file {0}? (y/n): ".format(file))
                    if response not in ("y", "Y", "yes", "Yes", "YES", "n", "N", "no", "No", "NO"):
                        print("Invalid response.")
                        continue
                    else:
                        if response in ("n", "N", "no", "No", "NO"):
                            delete = False
                        break
            if delete is True:
                print("Removing local file {0}.".format(file))
                os.remove(file)
        else:
            print("Verification of file {0} failed, skipping.".format(file))


if __name__ == "__main__":
    main()
