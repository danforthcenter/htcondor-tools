#!/usr/bin/env python
import os
from subprocess import Popen, PIPE
from datetime import datetime
import argparse
import MySQLdb
import json


# Parse command-line options
###########################################
def options():
    """Parse command-line options
    """
    parser = argparse.ArgumentParser(description="HTCondor job statistics monitor.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--config", help="MySQL database configuration JSON file.", required=True)
    args = parser.parse_args()

    return args


# Main
###########################################
def main():
    """Main program.
    """

    # Parse arguments
    args = options()

    # Job universe IDs
    uni = {
        1: "standard",
        5: "vanilla",
        7: "scheduler",
        8: "MPI",
        9: "grid",
        10: "java",
        11: "parallel",
        12: "local",
        13: "vm"
    }

    # Read the database connection configuration file
    config = open(args.config, "rU")
    # Load the JSON configuration data
    conf = json.load(config)

    # Connect to the MySQL database
    db = MySQLdb.connect(host=conf["hostname"], db=conf["database"], user=conf["username"], passwd=conf["password"])

    # Create a database cursor
    c = db.cursor(MySQLdb.cursors.DictCursor)

    # condor_q parameters
    cmd = ["condor_q", "-allusers", "-autoformat:t", "ClusterId", "ProcId", "Owner", "RemoteHost", "RequestCpus",
           "RequestMemory", "MemoryUsage", "RequestDisk", "DiskUsage", "JobStartDate", "ServerTime", "Cmd",
           "JobUniverse", "WantDocker", "ShouldTransferFiles", "AcctGroup"]

    machine = ["condor_status", "-autoformat:t", "JobId", "LoadAvg"]

    # Run condor_status to get load averages per job
    loads = {}
    ps = Popen(machine, stdout=PIPE)
    stdout = ps.communicate()

    # Process condor_q output
    raw = stdout[0]
    raw = raw.decode()
    table = raw.split("\n")

    # For each row in the output
    for row in table:
        # Remove the newline if any
        row = row.rstrip("\n")
        # Split up the columns on tab characters
        cols = row.split("\t")
        # Ignore blank lines and machines that have no job
        if len(cols) > 1 and cols[0] != "undefined":
            job_id = cols[0]
            loadave = cols[1]
            loads[job_id] = loadave

    # Run condor_q
    ps = Popen(cmd, stdout=PIPE)
    stdout = ps.communicate()

    # Process condor_q output
    raw = stdout[0]
    raw = raw.decode()
    # Split output into rows
    table = raw.split("\n")

    # For each row in the output
    for row in table:
        # Remove the newline if any
        row = row.rstrip("\n")
        # Split up the columns on tab characters
        cols = row.split("\t")
        # Ignore blank lines and jobs that have no host (queued jobs)
        if len(cols) > 1 and cols[3] != "undefined":
            stats = {}
            # If any of the request/usage values are undefined, set them to zero
            for i, value in enumerate(cols):
                if value == "undefined":
                    cols[i] = 0
            # Cluster ID
            stats["cluster"] = int(cols[0])
            # Process/Job ID
            stats["process"] = int(cols[1])
            # Job ID
            job_id = str(stats["cluster"]) + "." + str(stats["process"])
            # Username
            stats["username"] = cols[2]
            # Host
            stats["host"] = cols[3].replace("slot1@", "")

            # CPU
            # CPUs requested
            stats["cpu"] = int(cols[4])
            # Load average
            stats["cpu_load"] = 0
            if job_id in loads:
                stats["cpu_load"] = float(loads[job_id])

            # Memory
            # Requested memory in MiB
            stats["memory"] = int(cols[5])
            # Actual current memory usage in MiB
            stats["memory_usage"] = int(cols[6])

            # Disk
            # Requested disk (usually scratch) in KiB
            stats["disk"] = int(cols[7])
            # Actual disk usage in KiB
            stats["disk_usage"] = int(cols[8])

            # Runtime
            # Job start date in epoch seconds
            stats["start_date"] = datetime.fromtimestamp(int(cols[9])).strftime("%Y-%m-%d %H:%M:%S")
            # Current time in epoch seconds
            stats["datetime"] = datetime.fromtimestamp(int(cols[10])).strftime("%Y-%m-%d %H:%M:%S")

            # Get the command name, excluding path
            stats["exe"] = os.path.basename(cols[11])
            # Rename interactive jobs
            if stats["exe"] == "sleep":
                stats["exe"] = "(interactive)"

            # Get the job universe
            if int(cols[12]) in uni:
                # If this is not a Docker job, look up the universe
                if cols[13] == "true":
                    stats["universe"] = "docker"
                else:
                    stats["universe"] = uni[int(cols[12])]
            else:
                stats["universe"] = "unknown:" + str(cols[12])

            # Does this job use file transfers?
            if cols[14] == "YES":
                stats["transfer"] = 1
            else:
                stats["transfer"] = 0

            # Groupname
            stats["groupname"] = cols[15].replace("group_", "")

            # Is this job already in the database?
            c.execute("""SELECT id FROM jobs WHERE cluster = %s AND process = %s""",
                      (stats["cluster"], stats["process"]))
            stats["id"] = c.fetchone()
            # This is a new job
            if stats["id"] is None:
                c.execute(
                    """INSERT INTO jobs (cluster, process, username, groupname, start_date, cpu, memory, disk, host,
                    universe, exe, transfer) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (stats["cluster"], stats["process"], stats["username"], stats["groupname"], stats["start_date"],
                     stats["cpu"], stats["memory"], stats["disk"], stats["host"], stats["universe"], stats["exe"],
                     stats["transfer"]))
                c.execute("""SELECT id FROM jobs WHERE cluster = %s AND process = %s""",
                          (stats["cluster"], stats["process"]))
                stats["id"] = c.fetchone()

            # Insert the job statistics
            c.execute(
                """INSERT INTO job_stats (id, datetime, cpu_load, memory_usage, disk_usage) VALUES (%s, %s, %s,
                %s, %s)""", (stats["id"]["id"], stats["datetime"], stats["cpu_load"], stats["memory_usage"],
                             stats["disk_usage"]))


if __name__ == '__main__':
    main()
