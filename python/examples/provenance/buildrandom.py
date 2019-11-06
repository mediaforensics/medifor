#!/usr/bin/env python3
import faiss
import numpy as np

def main(outfile):
    d = 100
    nlist = 100
    k = 4

    nb = 100000
    np.random.seed(1234)

    xb = np.random.random((nb, d)).astype('float32')
    xb[:, 0] += np.arange(nb) / 1000.

    quantizer = faiss.IndexFlatL2(d)
    index = faiss.IndexIVFFlat(quantizer, d, nlist)
    index.train(xb)

    index.add(xb)

    faiss.write_index(index, outfile)


if __name__ == '__main__':
    import argparse

    a = argparse.ArgumentParser()
    a.add_argument('outfile', type=str, help='output file')

    args = a.parse_args()
    main(args.outfile)
