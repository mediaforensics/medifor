#!/usr/bin/env python3

from medifor.v1.provenanceservice import IndexSvc
import faiss
import numpy as np

def index(img, limit):
    """ 
    Function for to retrieve matching images from index shard. This function can essentially be
    whatever you want as long as the returned value is json serializable. The provenanceservice
    library will take care of the REST calls. In your analytic function you will receive a list of
    whatever is returned by this function (because it assumes multiple shards).
    """

    # Standard faiss index search function
    D, I = idx.search(np.array(img).astype('float32'), limit)

    # TODO process and package the results as needed. The statement below is just meant to be illustrative.
    # Whatever is returned (e.g., results in this case) will be sent via REST to your filtering analytic as long as it is JSON serializable.
    results = [{
                'fids': [int(x) for x in ids],
                'ids': [id_map.get(int(x)) for x in ids],
                'dists': [float(x) for x in dists],
                } for ids, dists in zip(I,D)][0]

    return results

if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    # Arguments for the faiss index service.
    # Port: The port that the service will run on.
    # Index: The location of the faiss index file
    # Map: Used to map index IDs (integers) to actual image IDs (NIST provided MD5 hashes)
    p.add_argument('--port', type=int, default=8080, help='Port to listen on')
    p.add_argument('--index', type=str, default='', required=True, help="Location of FAISS index file.")
    p.add_argument('--map', type=str, default='', help='Location of file mapping index IDs to other IDs as needed.')
    args = p.parse_args()

    # Import the index
    idx = faiss.read_index(args.index)
    
    # Create the ID map
    id_map = {}
    if args.map:
        with open(args.map, 'rb') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#'): continue
                k, v = line.split(':')
                id_map[int(k.strip())] = v.strip()

    idxsvc = IndexSvc(__name__, host="::", port=args.port)
    idxsvc.RegisterQuery(index)
    idxsvc.set_map(id_map)
    idxsvc.run()
