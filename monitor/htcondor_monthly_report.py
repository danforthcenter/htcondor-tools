#!/usr/bin/env python
import argparse
import sqlite3 as sq
import datetime


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

    parser = argparse.ArgumentParser(description='HTCondor user/group monthly usage report.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--db", help="SQLite database filename.", required=True)
    parser.add_argument("-o", "--outfile", help="Report output prefix.", required=True)
    parser.add_argument("-s", "--start", help="Report start date (YYYY-mm-dd).", required=True)
    parser.add_argument("-e", "--end", help="Report end date (YYYY-mm-dd).", required=True)
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

    connect = sq.connect(args.db)

    # Replace the row_factory result constructor with a dictionary constructor
    connect.row_factory = dict_factory

    # Change the text output format from unicode to UTF-8
    connect.text_factory = str

    # Database handler
    db = connect.cursor()

    # Get list of groups
    groups = []
    for row in db.execute('SELECT DISTINCT(`group`) FROM `group_stats`'):
        groups.append(row['group'])

    # Generate report for group usage
    out = open(args.outfile + "_group_stats.txt", "w")
    out.write("Group\tUsage\n")
    for group in groups:
        start_usage = get_group_usage(group, args.start, db)
        end_usage = get_group_usage(group, args.end, db)
        month_usage = end_usage - start_usage
        month_usage /= 60
        month_usage /= 60
        out.write(group + "\t" + str(month_usage) + "\n")
    out.close()

    # Generate user reports for each group
    for group in groups:
        out = open(args.outfile + "_" + group + "_stats.txt", "w")
        out.write("User\tUsage\n")

        # Get users for group
        users = []
        terms = (group,)
        for row in db.execute('SELECT DISTINCT(`user`) FROM `user_stats` WHERE `group` = ?', terms):
            users.append(row['user'])

        # Generate report for users in the group
        for user in users:
            start_usage = get_user_usage(group, user, args.start, db)
            end_usage = get_user_usage(group, user, args.end, db)
            month_usage = end_usage - start_usage
            month_usage /= 60
            month_usage /= 60
            out.write(user + "\t" + str(month_usage) + "\n")

    db.close()
    connect.close()

###########################################


# Dictionary factory for SQLite query results
###########################################
def dict_factory(cursor, row):
    """
    Replace the row_factory result constructor with a dictionary constructor.

    Args:
        cursor: (object) the sqlite3 database cursor object.
        row: (list) a result list.
    Returns:
        d: (dictionary) sqlite3 results dictionary.
    Raises:

    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


# Query group-level statistics
###########################################
def get_group_usage(group, date, db):
    """
    Query the database for group usage on a specific date.

    :param group: str
    :param date: str
    :param db: object
    :return usage: int
    """
    terms = (group, date + '%',)
    db.execute('SELECT * FROM `group_stats` WHERE `group` = ? AND `datetime` LIKE ?', terms)
    result = db.fetchone()
    return int(result['usage'])


# Query group-level statistics
###########################################
def get_user_usage(group, user, date, db):
    """
    Query the database for group usage on a specific date.

    :param group: str
    :param date: str
    :param db: object
    :return usage: int
    """
    terms = (group, user, date + '%',)
    db.execute('SELECT * FROM `user_stats` WHERE `group` = ? AND `user` = ? AND `datetime` LIKE ?', terms)
    usage = 0
    for result in db:
        if result is not None:
            usage += result['usage']
    return int(usage)

if __name__ == '__main__':
    main()
