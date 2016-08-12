# pbsmrtpipe ht_condor hooks

This directory contains a python script that builds, runs, and waits for the completion of jobs produced by PacBio's pbsmrtpipe.

The `*.tmpl` files are the hooks used by pbsmrtpipe to start and stop jobs. Until we decide to create an install script for condor_shellsub.py, `start.tmpl` should be edited to provide the full path to the this script.


   python <INSERT PATH HERE>/condor_shellsub.py -o "${STDOUT_FILE}" -e "${STDERR_FILE}" -j ${JOB_ID} -p ${NPROC} -c ${CMD} 
