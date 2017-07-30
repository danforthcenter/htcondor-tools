#!/usr/bin/python
import sys
import os
from subprocess import Popen, PIPE
import argparse


# Parse command-line options
###########################################
def options():
    """Parse command-line options
    """
    parser = argparse.ArgumentParser(description="Create HTCondor job description files from condor_history.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--ids",
                        help="One or more (comma-separated) HTCondor job IDs (cluster ID and process ID)",
                        required=True)
    args = parser.parse_args()

    return args


# Main
###########################################
def main():
    """Main program.

    Args:

    Returns:

    Raises:

    """
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

    ids = args.ids.split(",")
    for job_id in ids:
        job = {}
        # Run condor_history to get job ClassAd
        ps = Popen(["condor_history", "-long", job_id], stdout=PIPE)
        stdout = ps.communicate()
        # Process output
        raw = stdout[0]
        classad = raw.split("\n")
        # Parse ClassAd attributes
        for row in classad:
            # Remove the newline if any
            row = row.rstrip("\n")
            # Split key and value pair
            key, value = row.split(" = ")
            job[key] = value
        if len(job) > 0:
            out = open(job["ClusterId"] + "." + job["ProcId"] + ".condor", "w")
            out.write("# Job was run from " + job["Iwd"] + "\n\n")
            out.write("universe = " + uni[job["JobUniverse"]] + "\n")
            out.write("environment = " + job["Environment"] + "\n")
            out.write("accounting_group = " + job["AcctGroup"] + "\n")
            out.write("request_cpus = " + job["RequestCpus"] + "\n")
            out.write("request_memory = " + job["RequestMemory"] + "\n")
            out.write("request_disk = " + job["524288000"] + "\n")
            out.write("log = " + job["UserLog"] + "\n")
            out.write("out = " + job["Out"] + "\n")
            out.write("error = " + job["Err"] + "\n")
            out.write("executable = " + job["Cmd"] + "\n")
            out.write("arguments = " + job["Args"] + "\n")
            out.write("transfer_executable = " + job["TransferExecutable"] + "\n")
            out.write("should_transfer_files = " + job["ShouldTransferFiles"] + "\n")
            out.write("when_to_transfer_output = " + job["WhenToTransferOutput"] + "\n")
            if "TransferInput" in job:
                out.write("transfer_input_files = " + job["TransferInput"] + "\n")
            if "TransferOutput" in job:
                out.write("transfer_output_files = " + job["TransferOutput"] + "\n")
            out.write("queue\n")


if __name__ == '__main__':
    main()
