#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PY27_ENV="${HOME}/Sistemas/py27env"
AUTHOR='Juan Rodriguez Hortala <juan.rodriguez.hortala@gmail.com'
PROJECT_NAME='Phoenix OLAP'
DOC_TRG_DIR=${SCRIPT_DIR}'/python-doc'

source "${PY27_ENV}/bin/activate"
pushd ${SCRIPT_DIR}
rm -rf ${DOC_TRG_DIR}
# Generate sphinx project
sphinx-apidoc -A "${AUTHOR}" -H "${PROJECT_NAME}" -f -F -o ${DOC_TRG_DIR} ${SCRIPT_DIR}
# add the python code to the python path for sphinx
export PYTHONPATH=${PYTHONPATH}:${SCRIPT_DIR}
# generate html documentation
pushd ${DOC_TRG_DIR}
make clean
make html
popd 

popd