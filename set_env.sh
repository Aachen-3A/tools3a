#!/usr/bin/env bash
export TOOLS3A="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PATH=${TOOLS3A}/scripts:${PATH}
for dir in `find $TOOLS3A -name 'lib'`
  do
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${dir}
done
