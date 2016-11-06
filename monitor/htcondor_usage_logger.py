#!/usr/bin/env python
import argparse
import sqlite3
from subprocess import Popen, PIPE


# Parse command-line arguments
###########################################
def options():
    """Parse command line options.

    Args:

    Returns:
        argparse object.
    Raises:

    :return: argparse object
    """
    parser = argparse.ArgumentParser(description='HTCondor user/group usage logger.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--db", help="SQLite database filename.", required=True)
    args = parser.parse_args()

    return args
###########################################


# Main
###########################################
def main():
    """Main program.

    Args:

    Returns:

    Raises:

    """

    # Get options
    args = options()

    ps = Popen(['condor_userprio', "-allusers"], stdout=PIPE)
    report = ps.communicate()
    rc = ps.returncode()
    print(report)
###########################################


if __name__ == '__main__':
    main()
