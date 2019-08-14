#!/bin/bash

cd "$(dirname "$0")"

SCRIPT=${1:-example}

# Very naive script to find the matlab binary (Linux, Mac)
MATLAB="$(which matlab)"
if [[ $? != 0 ]]; then
  MATLAB='/Applications/MATLAB_R2018b.app/bin/matlab'
fi

FLAGS="-nodisplay -nosplash -nodesktop"

# NOTE: The fifoconn.py may or may not exist in the given directory.
exec ../py/fifoconn.py --resp_timeout=1 -- "$MATLAB" ${FLAGS[@]} -r "$SCRIPT"

# It is also possible to simply start your process and speak to it over agreed-upon FIFO locations, like this:
# ANALYTIC_FIFO_IN=/path/to/in/fifo ANALYTIC_FIFO_OUT=/path/to/out/fifo "$MATLAB" ${FLAGS[@]} -r "$SCRIPT"
#
# Then the caller process can use /path/to/in/fifo to send things to you, and /path/to/out/fifo to receive responses.
