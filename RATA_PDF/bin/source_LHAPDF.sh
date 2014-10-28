#!/bin/bash
TEMPVAR=`pwd`

# PATH where LHAPDF should be installed (change if necessary)
cd $TOOLS3A/RATA_PDF

# Set the enviroment
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD/local/lib/
export LHAPDF_BASE=$PWD/local/

cd $TEMPVAR
