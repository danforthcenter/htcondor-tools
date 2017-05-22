# htcondor-tools
A collection of scripts for building and running HTCondor jobs and workflows

## Current List of Tools and Descriptions
|           Tool            | Description |
| ------------------------- | ----------- |
| condor_tuxedo_pipeline.py | With provided reference file paths, and config for jobs, this builds and runs a DAG workflow to run the full Tuxedo Pipeline |
| pbsmrtpipe_condor_hooks   | Provides hooks to pbsmrtpipe to distribute jobs on ht_condor queuing system |
| condor_create_jobfile.sh  | Shell script to create condor job files to submit to the queue. It has getopts so feel free to -h for more information |
