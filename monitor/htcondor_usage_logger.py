#!/usr/bin/env python
import argparse
import sqlite3 as sq
import datetime
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
    # Sample time
    date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser(description='HTCondor user/group usage logger.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--db", help="SQLite database filename.", required=True)
    args = parser.parse_args()

    args.date = date

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

    connect = sq.connect(args.db)
    db = connect.cursor()

    ps = Popen(['condor_userprio', "-allusers", "-autoformat:r", "Name", "WeightedAccumulatedUsage"], stdout=PIPE)
    stdout = ps.communicate()

    report = stdout[0]
    usage_report = report.split('\n')

    for row in usage_report:
        # Skip blank rows
        if len(row) > 0:
            if "@" in row:
                # Then this is a user row
                # Split the user and usage
                identity, usage = row.split(" ")
                # Remove quotes
                identity = identity.replace('"', "")
                # Remove the user domain
                group_user = identity.replace("@ddpsc.org", "")
                group_user = group_user.replace("@datasci.danforthcenter.org", "")
                # Remove "nice-user"
                group_user = group_user.replace("nice-user.", "")
                group, user = "None", "None"
                if "." in group_user:
                    # The user specified their accounting group
                    group, user = group_user.split(".")
                else:
                    user = group_user
                # print("Date: " + args.date + ", User: " + user + ", Group: " + group + ', Usage: ' + usage)
                db.execute("INSERT INTO user_stats VALUES (?, ?, ?, ?)", (args.date, user, group, usage))
            elif "group" in row:
                # Then this is a group total row
                # Remove quotes
                row = row.replace('"', "")
                # Split the group and usage
                group, usage = row.split(" ")
                # print("Date: " + args.date + ", Group: " + group + ", Usage: " + usage)
                db.execute("INSERT INTO group_stats VALUES (?, ?, ?)", (args.date, group, usage))

    connect.commit()
    db.close()
    connect.close()
###########################################


if __name__ == '__main__':
    main()
