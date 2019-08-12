#!/usr/bin/env python3

import logging
import os
import os.path
import sys

import analytic_pb2
from analyticservice import AnalyticServiceFIFO

logging.basicConfig(level=logging.INFO)

def process_image(req, resp):
    logging.info("got request %s for image %s", req.request_id, req.image.uri)

    resp.score = 0.5
    resp.confidence = 0.2

    # Get image size for the explanation below. Just an example of how to
    # access actual image data.
    image_size = 0
    with open(req.image.uri, "rb") as img:
        data = img.read(1024)
        while data:
            image_size += len(data)
            data = img.read(1024)

    # Write out a useless "mask", which is really just a text file.
    # A real mask would be written in the same manner.
    os.makedirs(req.out_dir, exist_ok=True)
    out_mask = os.path.join(req.out_dir, "mask_test.txt")
    with open(out_mask, "wb") as f:
        f.write("hey there\n".encode('utf-8'))

    # Don't forget to specify where we put things.
    resp.localization.mask.uri = out_mask
    resp.localization.mask.type = 'text/plain'

    resp.explanation = 'faking a crop task for image {!r}, size {}'.format(
        req.image.uri, image_size)

    # Fake crop output. Can specify multiple manipulation types by
    # appending to the manipulation_type repeated field.
    resp.manipulation_type.append(analytic_pb2.MANIP_CROP)

    # Note that we don't specify an opt-out type. Any missing field in the
    # protocol will take the "zero" (or empty) value and can thus be omitted
    # if that's the right answer. In this case, the default is OPT_OUT_NONE.

    logging.info("done")

if __name__ == '__main__':
    print("Running Example Analytic Using FIFO For Communication")
    infile = os.environ.get("ANALYTIC_FIFO_IN")
    outfile = os.environ.get("ANALYTIC_FIFO_OUT")
    svc = AnalyticServiceFIFO(infile=infile, outfile=outfile)
    svc.RegisterImageManipulation(process_image)
    sys.exit(svc.Run())
