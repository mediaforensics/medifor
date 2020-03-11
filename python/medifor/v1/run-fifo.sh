#!/bin/bash

# This bash script provides an outline for the steps required to run an
# analytic using fifoconn.py. It assumes you have installed Python 3 on your
# system. While Python 3 is the default for running fifoconn, it is not
# required for your own analytics.

# Create and activate a virtual environment
mkdir venv
python3 -m venv venv
source "venv/bin/activate"

# Install the medifor-proto library and dependencies
pip install 'git+https://github.com/mediaforensics/medifor'

# TODO: This is a placeholder for your analytic. It is only here to provide a working example.
cat > my_analytic.py <<EOF
#!/bin/env python

import analyticservice
import os
import sys

def process_image(req, resp):
    resp.score = 0.5

if __name__ == '__main__':
    svc = analyticservice.AnalyticServiceFIFO()
    svc.RegisterImageManipulation(process_image)
    sys.exit(svc.Run())
EOF

# Run fifoconn with your analytic. This starts up the gRPC service
# and your analytic as a child process that it proxies requests to.
python3 -m fifoconn -- python my_analytic.py

# TODO: Now you can poke at it using the medifor CLI tool.
