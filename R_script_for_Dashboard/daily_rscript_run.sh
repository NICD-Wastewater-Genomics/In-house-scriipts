#!/bin/bash

# This is the script that runs the R script daily 
# Use cron to schedule the time and frequency of the runs

cd /path/to/R_script_for_Dashboard
Rscript server_dashboard_cleanup.R
