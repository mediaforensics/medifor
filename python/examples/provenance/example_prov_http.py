#!/usr/bin/env python3

from medifor.v1.provenanceservice import ProvenanceServiceHTTP

def on_filter(req, resp, unused_index_func):
    print("Got to our filter handler with request\n{}".format(req))

    # resp is a FilteringResult. Just set a simple field to show it's working.
    resp.probe.notes = 'this is a test'

def on_graph(req, resp):
    print("Got to our graph handler with request\n{}".format(req))

    # resp is a ProvenanceGraph. Just set a simple field to show it's working.
    resp.opt_out = True

def main():
    svc = ProvenanceServiceHTTP()

    svc.RegisterProvenanceFiltering(on_filter)
    svc.RegisterProvenanceGraphBuilding(on_graph)

    return svc.Run()

if __name__ == '__main__':
    import sys
    sys.exit(main() or 0)
