#!/bin/bash
usage="$(basename "$0") [-h] [-d DIR -g GLOB -m MAXDEPTH -n NAME -e EXECUTABLE -a ARGUMENTS -c REQUEST_CPUS -r REQUEST_MEMORY -i REQUEST_DISK -l LOG_DIRECTORY -x EXTRA_VARIABLES -f -s] -- program to make you a condor submit job script

where:
    -h		show this help text
    -d		directory to be passed to find where the directory, file, or files reside in.
    -g		glob for find -name option: can be using standard glob notation (*) or exact
    -m		maxdepth for find: 1 for current directory only, 2 for subdirectories
    -n		name for condor job
    -e		executable path for condor job
    -a		arguments for condor job. Always include \\\$(file)  as your target file or directory. Always escaping the dollar sign ($). Same applies for a multithreading parameter.
    -c		number of cpus requested aka threads
    -r		amount of memory requested
    -i		amount of disk to request
    -l		log directory where the logs will be written to
    -f	        should transfer input files to scratch?
    -s		several logs using the name variable and file variables or just one using the name variable
    -x		extra condor submit job script variables separated by comma i.e. 'Rank=memory|notification=Never|var1=blah' will overwrite any variables in the original stub as well
    -t		argument to find -type optional (experimental for now)

In the script there exists a condor stub example file that is used to primarily create a beginning condor job file. Feel free to edit it to your preferences although if updating from github it will be overwritten.

Example usage:
1) Before Bash Expansion:
bash `basename $0` -d \$(pwd) -g *.fastq.gz -m 1 -n Sample_Fastqc -e \$(which fastqc) -a \"-t \\\$(request_cpus) -o fastqc/ \\\$(file)\" -c 6 -l \$HOME/.logs > Sample_Fastqc.condor
2) After Bash Expansion:
bash `basename $0` -d $(pwd) -g *.fastq.gz -m 1 -n Sample_Fastqc -e $(which fastqc) -a \"-t \\\$(request_cpus) -o fastqc/ \\\$(file)\" -c 6 -l $HOME/.logs > Sample_Fastqc.condor"

while getopts ':hd:g:m:n:e:a:c:r:i:l:x:fst:' option; do
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
    c) REQUEST_CPUS=$(echo ${OPTARG} | sed -e 's/[\/&]/\\&/g')
       ;;
    r) REQUEST_MEMORY=$(echo ${OPTARG} | sed -e 's/[\/&]/\\&/g')
       ;;
    i) REQUEST_DISK=$(echo ${OPTARG} | sed -e 's/[\/&]/\\&/g')
       ;;
    l) LOG_DIR=${OPTARG}
       ;;
    x) EXTRA=${OPTARG}
       ;;
    f) TRANSFER="true"
       ;;
    s) SEPARATE="true"
       ;;
    t) TYPE=${OPTARG}
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

if [[ -z $FIND_DIR ]] || [[ -z $FIND_GLOB ]] || [[ -z $FIND_MAX ]] || [[ -z $NAME ]] || [[ -z $EXECUTABLE ]] || [[ -z $ARGUMENTS ]] || [[ -z $REQUEST_CPUS ]] || [[ -z $REQUEST_DISK ]] || [[ -z $REQUEST_MEMORY ]] || [[ -z $LOG_DIR ]]
then
    printf "One of the necessary arguments is missing, please provide it:\n -d DIR -g GLOB -m MAXDEPTH -n NAME -e EXECUTABLE -a ARGUMENTS -c REQUEST_CPUS -r REQUEST_MEMORY -i REQUEST_DISK -l LOG_DIRECTORY\n"
    echo "$usage" >&2
    exit 1
fi


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
request_memory   = 
request_disk     = 
notification     = Always
Rank             = cpus

##  Do not edit  ##
accounting_group = $ENV(CONDOR_GROUP)
########################################################
EOF

INPUT_2_FINAL=$(echo "$INPUT_2" | sed -r "s/(name\s+=\s+)/\1${NAME}/" | sed -r "s/(executable\s+=\s+)/\1${EXECUTABLE}/" | sed -r "s/(arguments\s+=\s+)/\1${ARGUMENTS}/" | sed -r "s/(request_cpus\s+=\s+)/\1${REQUEST_CPUS}/" | sed -r "s/(request_memory\s+=\s+)/\1${REQUEST_MEMORY}/" | sed -r "s/(request_disk\s+=\s+)/\1${REQUEST_DISK}/")

# Adding in transfering of files
if [ "$TRANSFER" = "true" ]; then
    # Getting the files and naming them file= and adding a queue afterwards
    # This also works for directories just have to make maxdepth be 1 and the glob just be the directory name exact
    if [[ -z $TYPE ]]; then
	INPUT_LOG_SEP=`find ${FIND_DIR} -maxdepth ${FIND_MAX} -name ${FIND_GLOB} | xargs -I {} bash -c 'filename=$(basename {});printf "file=${filename}\ntransfer_input_files={}\nqueue\n"'`
    else
	INPUT_LOG_SEP=`find ${FIND_DIR} -maxdepth ${FIND_MAX} -type ${TYPE} -name ${FIND_GLOB} | xargs -I {} bash -c 'filename=$(basename {});printf "file=${filename}\ntransfer_input_files={}\nqueue\n"'`
    fi
    
    # Replacing the arguments of the bash script within the INPUT_2 variable
    echo "$INPUT_2_FINAL"

    ## If the command line parameter -s was included make the log files per file instead of constantly appended
    if [ "$SEPARATE" = "true" ]; then
	printf "name\t\t = \$(name).\$(file)\n"
    else
	INPUT_LOG_SEP=$(echo "$INPUT_LOG_SEP" | sed '/filename/d')
    fi
   
    printf "should_transfer_files = YES\n"

    # Adding in the LOG_DIR variable to the condor job file
    printf "LOG_DIR\t\t = ${LOG_DIR}\n"
    echo "$EXTRA" | tr "|" "\n"
    # Printing the files
    printf "%s\n" $INPUT_LOG_SEP    
else
    # Getting the files and naming them file= and adding a queue afterwards
    # This also works for directories just have to make maxdepth be 1 and the glob just be the directory name exact
    if [[ -z $TYPE ]]; then
	INPUT_LOG_SEP=`find ${FIND_DIR} -maxdepth ${FIND_MAX} -name ${FIND_GLOB} | xargs -I {} bash -c 'filename=$(basename {});printf "filename=${filename}\nfile={}\nqueue\n"'`
    else
	INPUT_LOG_SEP=`find ${FIND_DIR} -maxdepth ${FIND_MAX} -type ${TYPE} -name ${FIND_GLOB} | xargs -I {} bash -c 'filename=$(basename {});printf "filename=${filename}\nfile={}\nqueue\n"'`
    fi
            
    # Replacing the arguments of the bash script within the INPUT_2 variable
    echo "$INPUT_2_FINAL"

    ## If the command line parameter -s was included make the log files per file instead of constantly appended
    if [ "$SEPARATE" = "true" ]; then
	printf "name\t\t = \$(name).\$(filename)\n"
    else
	INPUT_LOG_SEP=$(echo "$INPUT_LOG_SEP" | sed '/filename/d')
    fi

    # Adding in the LOG_DIR variable to the condor job file
    printf "LOG_DIR\t\t = ${LOG_DIR}\n"
    echo "$EXTRA" | tr "|" "\n"
    # Printing the files
    printf "%s\n" $INPUT_LOG_SEP
fi
