#!/bin/bash

fab -l
exit_code=$?
if [ $exit_code -ne 0 ]
then 
    echo "missing some prerequisites, run install_prerequisites.sh and then this script again"
    exit 1
fi

# FIXME: only a partial check is performed
echo 
echo "All prerequisites are ok"
echo 

fab install_devenv
