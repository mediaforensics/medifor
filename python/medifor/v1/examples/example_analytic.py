#!/usr/bin/env python3

import logging
import os
import os.path
import sys
import time
from PIL import Image

from medifor.v1 import analytic_pb2, analyticservice

logging.basicConfig(level=logging.INFO)

def process_image(req, resp):
    logging.info("got request %s for image %s", req.request_id, req.image.uri)

    time.sleep(2)
    resp.score = 0.5
    with Image.open(req.image.uri) as img:
        w, h = img.size

    # Just make a mask with a small rectangle in it.
    mask = Image.new('L', img.size)
    for x in range(20, 40):
        for y in range(20, 40):
            mask.putpixel((x % w, y % h), 255)
    omask = Image.new('L', img.size)
    for x in range(50, 60):
        for y in range(50, 60):
            omask.putpixel((x % w, y % h), 255)

    # Now write the two masks and specify where they are.
    mask_name = os.path.join(req.out_dir, "mask.png")
    omask_name = os.path.join(req.out_dir, "omask.png")

    resp.localization.mask.uri = mask_name
    resp.localization.mask.type = 'image/png'

    resp.localization.mask_optout.uri = omask_name
    resp.localization.mask_optout.type = 'image/png'

    mask.save(mask_name)
    omask.save(omask_name)


    logging.info("done")

def process_video(req, resp):
    logging.info("got request %s for video %s", req.request_id, req.video.uri)

    time.sleep(2)
    resp.score = 0.5
    logging.info("done")

if __name__ == '__main__':
    svc = analyticservice.AnalyticService()
    svc.RegisterImageManipulation(process_image)
    svc.RegisterVideoManipulation(process_video)

    sys.exit(svc.Run())
