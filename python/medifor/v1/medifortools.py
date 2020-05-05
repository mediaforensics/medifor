import mimetypes
import os
import uuid

from medifor.v1 import analytic_pb2, pipeline_pb2

mimetypes.init()

mimetypes.add_type("image/x-adobe-dng", ".dng")
mimetypes.add_type("image/x-canon-cr2", ".cr2")
mimetypes.add_type("image/x-canon-crw", ".crw")
mimetypes.add_type("image/x-epson-erf", ".erf")
mimetypes.add_type("image/x-fuji-raf", ".raf")
mimetypes.add_type("image/x-kodak-dcr", ".dcr")
mimetypes.add_type("image/x-kodak-k25", ".k25")
mimetypes.add_type("image/x-kodak-kdc", ".kdc")
mimetypes.add_type("image/x-minolta-mrw", ".mrw")
mimetypes.add_type("image/x-nikon-nef", ".nef")
mimetypes.add_type("image/x-olympus-orf", ".orf")
mimetypes.add_type("image/x-panasonic-raw", ".raw")
mimetypes.add_type("image/x-pentax-pef", ".pef")
mimetypes.add_type("image/x-sigma-x3f", ".x3f")
mimetypes.add_type("image/x-sony-arw", ".arw")
mimetypes.add_type("image/x-sony-sr2", ".sr2")
mimetypes.add_type("image/x-sony-srf", ".srf")

mimetypes.add_type('video/avchd-stream', '.mts')
mimetypes.add_type("application/x-mpegURL", ".m3u8")
mimetypes.add_type("video/3gpp", ".3gp")
mimetypes.add_type("video/MP2T", ".ts")
mimetypes.add_type("video/mp4", ".mp4")
mimetypes.add_type("video/quicktime", ".mov")
mimetypes.add_type("video/x-flv", ".flv")
mimetypes.add_type("video/x-ms-wmv", ".wmv")
mimetypes.add_type("video/x-msvideo", ".avi")

additional_image_types = frozenset([
    "application/octet-stream",
    "application/pdf"
])

additional_video_types = frozenset([
    "application/x-mpegURL",
    "application/mxf"
])


analytic_req_map = {"image": analytic_pb2.ImageManipulationRequest(),
                    "video": analytic_pb2.VideoManipulationRequest()}


def get_media_type(uri):
    """
    'get_media_type' takes a filepath and returns the typestring and media type.
    If the mimetype is not discernable, the typestring returned will be
    "application/octet-stream", and the media type "application".
    """
    filename, ext = os.path.splitext(uri)
    typestring = mimetypes.types_map.get(ext.lower(), 'application/octet-stream')

    if typestring in additional_video_types:
        return typestring, 'video'

    if typestring in additional_image_types:
        return typestring, 'image'

    return typestring, typestring.split("/")[0]


def get_detection(media, output_dir, request_id=None):
    mime, mtype = get_media_type(media)
    det = analytic_pb2.Detection()
    if not request_id:
        request_id = str(uuid.uuid4())
    if mtype == "image":
        req = analytic_pb2.ImageManipulationRequest()
        req.image.uri = media
        req.image.type = mime
        req.request_id = request_id
        req.out_dir = output_dir
        det.img_manip_req.MergeFrom(req)
    elif mtype == "video":
        req = analytic_pb2.VideoManipulationRequest()
        req.video.uri = media
        req.video.type = mime
        req.request_id = str(uuid.uuid4())
        req.out_dir = output_dir
        det.vid_manip_req.MergeFrom(req)
    else:
        raise ValueError("Invalid media type. {!s} is not currently supported".format(mtype))

    return det


def get_detection_req(media):
    mime, mtype = get_media_type(media)
    return analytic_req_map[mtype]


def get_pipeline_req(media, detection_id="", analytic_ids=[], out_dir="", fuser_id=[], tags=[]):
    req = pipeline_pb2.DetectionRequest()
    mime, mtype = get_media_type(media)
    if mtype == "image":
        img_req = analytic_pb2.ImageManipulationRequest()
        img_req.image.uri = media
        img_req.image.type = mtype
        img_req.out_dir = out_dir
        req.request.img_manip_req.copyFrom(img_req)
    elif mtype == "video":
        vid_req = analytic_pb2.VideoManipulationRequest()
        vid_req.video.uri = media
        vid_req.video.type = mtype
        vid_req.out_dir = out_dir
        req.request.vid_manip_req.copyFrom(vid_req)
    else:
        raise ValueError("Unsupported media format.  Could not regocnize the mimetype for {!s}".format(media))

    req.id = detection_id
    req.analytic_id.extend(analytic_ids)
    req.tags.update(tags)
    req.fuser_id.extend(fuser_id)
    return req
