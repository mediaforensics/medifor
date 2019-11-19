#!/usr/bin/env python3

from medifor.v1.provenanceservice import IndexSvc

from flask import  jsonify
import faiss
import numpy as np

id_map = {}

def index(img, limit):
    """ Function for to retrieve matching images from index shard """

    D, I = idx.search(np.array(img).astype('float32'), limit)

    # TODO process and package the results as needed. The statement below is just meant to be illustrative.
    # Whatever is returned (e.g., results in this case) will be sent via REST to your filtering analytic.
    results = [{
                'fids': [int(x) for x in ids],
                'ids': [id_map.get(int(x)) for x in ids],
                'dists': [float(x) for x in dists],
                } for ids, dists in zip(I,D)]

    return results

if __name__ == '__main__':
    import argparse
    
    p = argparse.ArgumentParser()
    p.add_argument('--port', type=int, default=8080, help='Port to listen on')
    p.add_argument('--index', type=str, default='', required=True, help="Location of FAISS index file.")
    p.add_argument('--map', type=str, default='', help='Location of file mapping index IDs to other IDs as needed.')

    args = p.parse_args()

    idx = faiss.read_index(args.index)
    
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
    # idxsvc.app.run(host='::', port=args.port, debug=True)
    idxsvc.run()
