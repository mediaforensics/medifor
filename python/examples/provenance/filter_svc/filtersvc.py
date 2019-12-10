import requests
import numpy as np
from pprint import pprint

from medifor.v1.provenanceservice import ProvenanceService, query_index
from medifor.v1 import provenance_pb2
import PIL.Image

index_url = ""

def filter(req, resp, query_func):
    """This is the function that does the filtering.  It makes calls to the index(s) through
    a REST API using the requests library.  You can modify this to your own needs, or use the
    function call provided here.  The image is 'encoded' into a 100 dimensional numpy vector as
    a stand in for the feature vector.  You can encode this as you see fit since you control the
    decoding in the index."""

    def encode(img):
        """Dummy encoding function to take an image and turn it into a 100-D vector for index search"""
        seed = np.sum(img)
        np.random.seed(seed)
        return [[float(v) for v in q] for q in np.random.random((1,100)).astype("float32")]

    with PIL.Image.open(req.image.uri) as img:
        img = np.array(img.getdata()).reshape(img.size[0], img.size[1], 3)

    # index_results is a list of results across all index shards. Results contained in index_results["value"]
    index_results = query_func(encode(img), index_url, req.result_limit)

    matches = []
    # This section unpacks the results from the index service. Your implementation will likely be different.
    for img_matches in index_results:
        for i, img_id in enumerate(img_matches["value"]['ids']):
            new_match = provenance_pb2.ImageMatch()
            if not img_id:
                new_match.image_id = str(img_matches["value"]["fids"][i])
            else:
                new_match.image_id = str(img_id)
            new_match.score = img_matches["value"]["dists"][i]
            matches.append(new_match)

    resp.matches.extend(matches)


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    # Accepts a list of urls. Each url needs to be preceded by the flag.
    p.add_argument('--url', type=str, help='URL of host to talk to', action='append')
    args = p.parse_args()
    if not args.url:
        # Adds a default if left blank
        args.url = ['http://localhost:8080/search']
    index_url = args.url
    svc = ProvenanceService()
    svc.RegisterProvenanceFiltering(filter)
    svc.Run()

