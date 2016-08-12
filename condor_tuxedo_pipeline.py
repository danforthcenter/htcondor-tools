#!/usr/bin/env python
# submit jobs to queue on behalf of pacbio software

import os, sys
import argparse
import subprocess


def hisat_stringtie_script( args ):
    script = ["#!/bin/bash\n"]

    ## CREATE HISAT LINE
    hisat_line = "hisat2 --dta-cufflinks -p {threads} --max-intronlen {maxintron} -x {idxfile}".format(threads=args.threads, maxintron=args.max_intronlen, idxfile=args.hisat_index)
    
    # add reads files to hisat_line
    if args.single:
        hisat_line += " -U $2"
    else:
        hisat_line += " -1 $2 -2 $3"

    # add samtools sort line
    hisat_line += " | samtools sort -@{threads} -T $1.HISAT2tmp - -o $1.HISAT2.bam\n".format(threads=args.threads)

    ## CREATE STRINGTIE LINE
    stringtie_line = "stringtie $1.HISAT2.bam -G {gff} -p {threads} -o $1.stringtie.gtf\n".format(gff=args.gffref, threads=args.threads)

    ## FINISH SCRIPT
    script.append( hisat_line )
    script.append( stringtie_line )

    return script

# Build job file for hisat/stringtie
def hisat_stringtie_jobfile( args, hisat_script_filename ):
    jobfile = ["universe         = vanilla\n",
               "getenv           = true\n",
               "accounting_group = {}\n".format(args.condor_group),
               "logdir           = {}\n".format(args.logdir),
               "initialdir       = {}\n".format(args.initialdir),
               "executable       = /bin/bash\n"
               ]
    config_fields = "outprefix,condition"
    
    # check if reads are single end or paired end
    if args.single:
        cmdstr = hisat_script_filename + " $(outprefix) $(reads)"
        config_fields+= ",reads"
    else:
        cmdstr = hisat_script_filename + " $(outprefix) $(reads1) $(reads2)"
        config_fields += ",reads1,reads2"

    jobfile.append( "arguments  = \"{}\"\n".format(cmdstr) )
    jobfile.append( "log  = $(logdir)/$(outprefix).log\n" )
    jobfile.append( "output  = $(logdir)/$(outprefix).out\n" )
    jobfile.append( "error  = $(logdir)/$(outprefix).error\n" )
    jobfile.append( "request_cpus  = {}\n".format( args.threads ) )
    jobfile.append( "request_memory  = {}\n".format( args.threads * args.memory ) )
    
    jobfile.append( "queue {cfields} from {cfile}\n".format( cfields=config_fields, cfile=args.configfile ) )
    
    return jobfile

# build cuffmerge/cuffdiff script
def cuffmerge_cuffdiff_script( args ):
    script = ["#!/bin/bash\n"]

    assembly_list = ""
    bamfiles = ""
    bamlist = {}
    labels = []

    # build necessary strings for script
    with open(args.configfile, 'r') as fh:
        for line in fh:
            line = line.split()
            
            assembly_list += line[0] + ".stringtie.gtf "
            if not line[1] in bamlist:
                bamlist[line[1]] = []

            bamlist[line[1]].append(line[0] + ".HISAT2.bam")
            labels.append( line[1] )

    # build bam string
    for l in labels:
        bamfiles += ",".join(bamlist[l]) + " "

    script.append("ls {} > assembly_list.txt\n".format(assembly_list) )
    script.append("cuffmerge -p {threads} -g {gff} assembly_list.txt\n".format(threads=args.threads, gff=args.gffref) )
    script.append("cuffdiff -p {threads} -o {expprefix}.cuffdiff merged_asm/merged.gtf -max-bundle-frags 50000000 {bamfiles} -L {labels}\n".format(threads=args.threads, expprefix=args.expprefix, bamfiles=bamfiles, labels=",".join(labels) ) )

    return script


# build job file using pacbio's args
def cuffmerge_cuffdiff_jobfile( args, cuffmerge_cuffdiff_script_filename ):
    jobfile = ["universe         = vanilla\n",
               "getenv           = true\n",
               "accounting_group = {}\n".format(args.condor_group),
               "logdir           = {}\n".format(args.logdir),
               "initialdir       = {}\n".format(args.initialdir),
               "executable       = /bin/bash\n"
               ]

    jobfile.append( "arguments  = \"{}\"\n".format(cuffmerge_cuffdiff_script_filename) )
    jobfile.append( "log  = $(logdir)/{}.log\n".format(args.expprefix) )
    jobfile.append( "output  = $(logdir)/{}.out\n".format(args.expprefix) )
    jobfile.append( "error  = $(logdir)/{}.error\n".format(args.expprefix) )
    jobfile.append( "request_cpus  = {}\n".format( args.threads ) )
    jobfile.append( "request_memory  = {}\n".format( args.threads * args.memory ) )
    
    jobfile.append( "queue\n" )
    return jobfile

# build dagman file for managing DAG
def dagman_file(args, hisat_jobfile_filename, cuffmerge_cuffdiff_jobfile_filename):
    dagfile = ["JOB hisat {}\n".format(hisat_jobfile_filename),
               "JOB cuff {}\n".format(cuffmerge_cuffdiff_jobfile_filename),
               "PARENT hisat CHILD cuff\n"
              ]

    return dagfile

# write job/script file
def write_file( jobfile, filename ):
    with open( filename, 'w' ) as fh:
        for line in jobfile:
            fh.write( line )


################
##### MAIN #####
################
def main( prog_name, argv ):
    # ARG PROCESSING
    parser = argparse.ArgumentParser( prog=prog_name, description= 'designed to build and run a tuxedo suite pipeline',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument('-g','--gff-ref', dest='gffref', metavar='GFFREF', required=True, type=str, help='gff reference file to use in cuffdiff and cuffmerge')
    parser.add_argument('-f','--fasta-ref', dest='faref', metavar='FAREF', required=True, type=str, help='reference genome file to use')
    parser.add_argument('-c','--configfile', dest='configfile', metavar='CONFIG', required=True, type=str, help='file containing all replicate information with each line in the form "outprefix condition reads[1 reads2]"')
    parser.add_argument('-x','--hisat-index', dest='hisat_index', metavar='HISAT_INDEX', required=True, type=str, help='prefix of HISAT2 index set in hisat2-build command')
    parser.add_argument('-l','--log-directory', dest='logdir', metavar='LOGDIR', required=True, type=str, help='directory to write log files to')
    parser.add_argument('-i','--initial-directory', dest='initialdir', metavar='INITIALDIR', required=True, type=str, help='initial directory to run these commands in')
    parser.add_argument('-e','--experiment-prefix', dest='expprefix', metavar='EXPPREFIX', default="tuxedo_run", type=str, help='prefix given to cuffdiff run (this output goes in the initial directory)')
    parser.add_argument('--condor-group', dest='condor_group', metavar='CONDOR_GROUP', default="$ENV(CONDOR_GROUP)", type=str, help=argparse.SUPPRESS)
    parser.add_argument('-p','--threads', dest='threads', metavar='NPROC', default=10, type=int, help='NPROC for jobs')
    parser.add_argument('--max-intronlen', dest='max_intronlen', metavar='MAX_INTRONLEN', default=50000, type=int, help='maximum intron length to use in HISAT2')
    parser.add_argument('-m','--memory', dest='memory', metavar='MEM', default=3000, type=int, help='megabytes to allocate in jobfile per thread')
    parser.add_argument('--single', dest='single', default=False, action='store_true', help='flag to indicate single end reads are used')
    args = parser.parse_args(argv)

    ## INITIALIZE
    hisat_script_filename = args.initialdir + "/hisat_stringtie_script.sh"
    hisat_jobfile_filename = args.initialdir + "/hisat_stringtie_script.job"
    cuffmerge_cuffdiff_script_filename = args.initialdir + "/cuffmerge_cuffdiff_script.sh"
    cuffmerge_cuffdiff_jobfile_filename = args.initialdir + "/cuffmerge_cuffdiff_script.job"
    dag_filename = args.initialdir + "/" + args.expprefix + ".dag"

    #########################
    ## CREATE FILES ######
    #################

    ## CREATE HISAT/STRINGTIE SCRIPT
    hisat_stringtie_script_string = hisat_stringtie_script( args )

    ## CREATE HISAT/STRINGTIE JOBFILE
    hisat_stringtie_jobfile_string = hisat_stringtie_jobfile( args, hisat_script_filename )

    ## CREATE CUFFMERGE/CUFFDIFF SCRIPT
    cuffmerge_cuffdiff_script_string = cuffmerge_cuffdiff_script( args )

    ## CREATE CUFFMERGE/CUFFDIFF JOBFILE
    cuffmerge_cuffdiff_jobfile_string = cuffmerge_cuffdiff_jobfile( args, cuffmerge_cuffdiff_script_filename )

    ## CREATE DAGMAN FILE
    dagman_string = dagman_file(args, hisat_jobfile_filename, cuffmerge_cuffdiff_jobfile_filename)

    ########################
    ## WRITE FILES ######
    ################

    ## WRITE SCRIPTS AND JOBFILES
    write_file( hisat_stringtie_script_string, hisat_script_filename )
    write_file( hisat_stringtie_jobfile_string, hisat_jobfile_filename )
    write_file( cuffmerge_cuffdiff_script_string, cuffmerge_cuffdiff_script_filename )
    write_file( cuffmerge_cuffdiff_jobfile_string, cuffmerge_cuffdiff_jobfile_filename )

    ## WRITE DAGMAN FILE
    write_file( dagman_string, dag_filename )


    ### ~~~~~~~~~~ ###

    ## SUBMIT JOBFILE TO QUEUE
    queue_id = subprocess.check_output( ["condor_submit_dag", dag_filename] )
   
if __name__ =='__main__':
    main(sys.argv[0], sys.argv[1:])	
