#!/bin/bash
usage="$(basename "$0") [-h] [-d DIR -g GLOB -m MAXDEPTH -n NAME -e EXECUTABLE -a ARGUMENTS -t THREADS/REQUEST_CPUS -l LOG_DIRECTORY] -- program to make you a condor submit job script

where:
    -h		show this help text
    -d		directory for find
    -g		glob for find -name option
    -m		maxdepth for find
    -n		name for condor job
    -e		executable path for condor job
    -a		arguments for condor job
    -t		number of cpus requested aka threads
    -l		log directory
    -s		several logs or just one		

Example usage:
1) Before Bash Expansion:
bash `basename $0` -d \$(pwd) -g *.fastq.gz -m 1 -n Sample_Fastqc -e \$(which fastqc) -a \"-t \\\$(request_cpus) -o fastqc/ \\\$(file)\" -t 6 -l \$HOME/.logs > Sample_Fastqc.condor
2) After Bash Expansion:
bash `basename $0` -d $(pwd) -g *.fastq.gz -m 1 -n Sample_Fastqc -e $(which fastqc) -a \"-t \\\$(request_cpus) -o fastqc/ \\\$(file)\" -t 6 -l $HOME/.logs > Sample_Fastqc.condor"

# while getopts ':hdgneat:' option; do
while getopts ':hd:g:m:n:e:a:t:l:s' option; do
  case "${option}" in
    h) echo "$usage"
       exit
       ;;
    d) FIND_DIR=${OPTARG}
       ;;
    g) FIND_GLOB=${OPTARG}
       ;;
    m) FIND_MAX=${OPTARG}
       ;;
    n) NAME=$(echo ${OPTARG} | sed -e 's/[\/&]/\\&/g')
       ;;
    e) EXECUTABLE=$(echo ${OPTARG} | sed -e 's/[\/&]/\\&/g')
       ;;
    a) ARGUMENTS=$(echo ${OPTARG} | sed -e 's/[\/&]/\\&/g')
       ;;
    t) REQUEST_CPUS=$(echo ${OPTARG} | sed -e 's/[\/&]/\\&/g')
       ;;
    l) LOG_DIR=${OPTARG}
       ;;
    s) SEPARATE="true"
       ;;
    :) printf "missing argument for -%s\n" "$OPTARG" >&2
       echo "$usage" >&2
       exit 1
       ;;
   \?) printf "illegal option: -%s\n" "$OPTARG" >&2
       echo "$usage" >&2
       exit 1
       ;;
  esac
done
shift $((OPTIND - 1))

# It prints to stdout a condor_submit job file that can be redirected to a file: 

# Reads in the long multi line string to INPUT_2 feel free to change as desired.
read -r -d '' INPUT_2 <<-'EOF'
########################################################
#						       #
# Example Vanilla Universe Job			       #
# Simple HTCondor submit description file	       #
# 	 	  	 	     		       #
########################################################

name             = 
universe         = vanilla
getenv           = true
executable       = 
arguments        = 
log              = $(LOG_DIR)/$(name).log
output           = $(LOG_DIR)/$(name).out
error            = $(LOG_DIR)/$(name).error
request_cpus     = 
request_memory   = 30G
notification     = Always
Rank             = cpus

##  Do not edit  ##
accounting_group = $ENV(CONDOR_GROUP)
########################################################
EOF

# Getting the files and naming them file= and adding a queue afterwards
# INPUT_1=`find ${FIND_DIR} -maxdepth ${FIND_MAX} -name ${FIND_GLOB} | xargs printf "file=%s\nqueue\n"`
INPUT_LOG_SEP=`find ${FIND_DIR} -maxdepth ${FIND_MAX} -name ${FIND_GLOB} | xargs -I {} bash -c 'filename=$(basename {});printf "filename=${filename}\nfile={}\nqueue\n"'`

# Replacing the arguments of the bash script within the INPUT_2 variable
echo "$INPUT_2" | sed -r "s/(name\s+=\s+)/\1${NAME}/" | sed -r "s/(executable\s+=\s+)/\1${EXECUTABLE}/" | sed -r "s/(arguments\s+=\s+)/\1${ARGUMENTS}/" | sed -r "s/(request_cpus\s+=\s+)/\1${REQUEST_CPUS}/"
## If the command line paramater -s was included make the log files per file
if [ "$SEPARATE" = "true" ]; then
    printf "name\t\t = \$(name).\$(filename)\n"
else
    INPUT_LOG_SEP=$(echo "$INPUT_LOG_SEP" | sed '/filename/d')
fi

printf "LOG_DIR\t\t = ${LOG_DIR}\n"
# Printing the files
printf "%s\n" $INPUT_LOG_SEP
# printf "%s\n" $INPUT_1
