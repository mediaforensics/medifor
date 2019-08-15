#!/usr/bin/env python3

import logging
import os
import os.path
import sys
import time

from medifor.v1 import analytic_pb2, analyticservice

logging.basicConfig(level=logging.INFO)

def process_image(req, resp):
    logging.info("got request %s for image %s", req.request_id, req.image.uri)

    time.sleep(2)
    resp.score = 0.5
    logging.info("done")

if __name__ == '__main__':
    svc = analyticservice.AnalyticService()
    svc.RegisterImageManipulation(process_image)

    sys.exit(svc.Run())
