#!/usr/bin/env python3

import logging
import os
import os.path
import sys

from fusionservice import FusionService

logging.basicConfig(level=logging.INFO)

def process_image(req, resp):
    logging.info("got request for fusion on image %s", req.img_manip_req.image.uri)
    resp.score, resp.confidence = max((x.data.score, x.data.confidence) for x in req.img_manip)

    if req.want_masks:
        os.makedirs(req.out_dir, exist_ok=True)
        out_mask = os.path.join(req.out_dir, "mask_test")
        with open(out_mask, "wb") as f:
            f.write("hey there\n".encode('utf-8'))

        resp.localization.mask.uri = out_mask
        resp.localization.mask.type = 'text/plain'

    logging.info("done")

def process_splice(req, resp):
    logging.info("got request for splice %s", req.img_splice_req)

    resp.link.score = 0.502
    resp.link.confidence = 0.202

    logging.info("done")

def process_video(req, resp):
    logging.info("got request for video %s", req.vid_manip_req)

    resp.score = 0.503
    resp.confidence = 0.203

    logging.info("done")

if __name__ == '__main__':
    svc = FusionService()
    svc.RegisterImageManipulation(process_image)
    svc.RegisterImageSplice(process_splice)
    svc.RegisterVideoManipulation(process_video)

    sys.exit(svc.Run())
