import requests
import numpy as np
from pprint import pprint

from medifor.v1.provenanceservice import ProvenanceService, query_index
from medifor.v1 import provenance_pb2
import PIL.Image

index_url = ""

def encode(img):
    """Dummy encoding function to take an image and turn it into a 100-D vector for index search"""
    seed = np.sum(img)
    np.random.seed(seed)
    return [[float(v) for v in q] for q in np.random.random((1,100)).astype("float32")]


def filter(req, resp, query_func):
    """This is the function that does the filtering.  It makes calls to the index(s) through 
    a REST API using the requests library.  You can modify this to your own needs, or use the
    function call provided here.  The image is 'encoded' into a 100 dimensional numpy vector as 
    a stand in for the feature vector.  You can encode this as you see fit since you control the 
    decoding in the index."""

    print(index_url)

    with PIL.Image.open(req.image.uri) as img:
        img = np.array(img.getdata()).reshape(img.size[0], img.size[1], 3)

    matches = query_func(encode(img), index_url, req.result_limit)

    print (matches)

    resp.matches.extend(matches)


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--url', type=str, default=['http://localhost:8080/search'], help='URL of host to talk to', action='append')
    args = p.parse_args()
    index_url = args.url
    svc = ProvenanceService()
    svc.RegisterProvenanceFiltering(filter)
    svc.Run()
    
