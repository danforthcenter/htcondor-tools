#!/usr/bin/env python
# submit jobs to queue on behalf of pacbio software

import os, sys
import argparse
import subprocess


# build job file using pacbio's args
def build_jobfile( pbargs ):
    jobfile = ["universe         = vanilla\n",
               "getenv           = true\n",
               "accounting_group = $ENV(CONDOR_GROUP)\n"]

    jobfile.append( "executable  = /bin/bash\n" )
    jobfile.append( "arguments  = \"{cmd}\"\n".format( cmd=' '.join(pbargs.cmd) ) )
    jobfile.append( "log  = {log}.log\n".format( log=pbargs.jobid ) )
    jobfile.append( "output  = {out}\n".format( out=pbargs.outfile ) )
    jobfile.append( "error  = {err}\n".format( err=pbargs.errfile ) )
    jobfile.append( "request_cpus  = {}\n".format( pbargs.threads ) )
    jobfile.append( "request_memory  = {}\n".format( pbargs.threads * pbargs.memory ) )
    jobfile.append( "+pbsmrtpipe_jobid = \"{}\"\n".format(pbargs.jobid) )
    jobfile.append( "queue\n" )

    return jobfile

# write jobfile to file
def write_jobfile( jobfile, filename ):
    with open( filename, 'w' ) as fh:
        for line in jobfile:
            fh.write( line )


################
##### MAIN #####
################
def main( prog_name, argv ):
    # ARG PROCESSING
    parser = argparse.ArgumentParser( prog=prog_name, description= '',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument('-o', dest='outfile', metavar='STDOUT_FILE', required=True, type=str, help='STDOUT_FILE for pbsmrtpipe')
    parser.add_argument('-e', dest='errfile', metavar='STDERR_FILE', required=True, type=str, help='STDERR_FILE for pbsmrtpipe')
    parser.add_argument('-j', dest='jobid', metavar='JOB_ID', required=True, type=str, help='JOB_ID for pbsmrtpipe')
    parser.add_argument('-c', dest='cmd', metavar='CMD', nargs='+', type=str, help='CMD for pbsmrtpipe')
    parser.add_argument('-p', dest='threads', metavar='NPROC', required=True, type=int, help='NPROC for pbsmrtpipe')
    parser.add_argument('-m', dest='memory', metavar='MEM', default=3000, type=int, help='megabytes to allocate in jobfile per thread')
    parser.add_argument('--silent', dest='silent', action='store_true', help='flag to submit job without waiting to complete (an option to run outside of pbsmrtpipe)')
    args = parser.parse_args(argv)

    # SET VARIBLES FROM COMMAND LINE ARGS
    outfile = args.outfile
    errfile = args.errfile
    jobid = args.jobid
    pbcmd = args.cmd
    threads = args.threads

    # create name for jobfile
    jobfilename = "{}.job".format(args.jobid)

    ## CREATE JOBFILE
    jobfile = build_jobfile( args )

    ## WRITE JOBFILE
    write_jobfile( jobfile, jobfilename )

    ## SUBMIT JOBFILE TO QUEUE
    queue_id = subprocess.check_output( ["condor_submit", jobfilename] )
   
    ## WAIT FOR JOB TO FINISH BEFORE CLOSING PROCESS
    if not args.silent:
        subprocess.call( [ "condor_wait", jobid + ".log" ] )

if __name__ =='__main__':
    main(sys.argv[0], sys.argv[1:])	
