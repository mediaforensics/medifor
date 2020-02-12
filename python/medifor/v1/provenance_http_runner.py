#!/usr/bin/env python3

import logging
import subprocessing
import threading
import urllib.parse

import requests
from google.protobuf import json_format

from . import provenanceservice as prov


class ProxyService(prov.ProvenanceService):
    def __init__(self, backend):
        self.backend = backend
        self.RegisterProvenanceFiltering(self.prov_filter)
        self.RegisterProvenanceGraphBuilding(self.prov_graph)

    def backend_url(self, path):
        return urllib.parse.urlunparse('http', self.backend, path, '', '', '')

    def _forward_req(self, req, resp, path):
        data = json_format.MessageToDict(req)
        r = requests.post(self.backend_url(path), data=data)
        if not 200 <= r.status_code < 300:
            raise IOError("Error in request:\n{}".format(r.text))
        json_format.ParseDict(r.json(), resp)

    def prov_filter(self, req, resp, *unused_args):
        self._forward_req(req, resp, '/api/filter')

    def prov_graph(self, req, resp):
        self._forward_req(req, resp, '/api/graph')


def main(args):
    svc = ProxyService(args.backend)

    child = subprocess.Popen(args.child_args, stdin=subprocess.DEVNULL, stdout=None, stderr=None)

    try:
        server = svc.Start(analytic_port=args.port)
        while True:
            time.sleep(5)
            child_ret = child.poll()
            # Child exited, quit service.
            if child_ret is not None:
                raise StopIteration(child_ret)
    except StopIteration as e:
        logging.warn("Child process exited with code {}".format(e))
        server.stop(0)
    except KeyboardInterrupt:
        logging.warn("Server stopped")
        server.stop(0)
        logging.warn("Killing child {}".format(child.pid))
        child.kill()
        return 1
    except Exception:
        logging.exception("Caught exception in server")
        server.stop(0)
        logging.warn("Killing child {}".format(child.pid))
        child.kill()
        return -1


def parse_args():
    import argparse
    p = argparse.ArgumentParser(
        description=('Start a provenance service that spawns a child process, with given flags. It is '
                     'expected to speak the provenance HTTP language (proto-shaped JSON).'))

    p.add_argument('--port', type=int, default=50051, help='GRPC port to listen on.')
    p.add_argument('--backend', type=str, default='localhost:8765', help='Backend address to speak HTTP to.')
    p.add_argument('child_args', type=str, nargs='*', help='Child command line to run. Specify after flags using -- first.')

    return p.parse_args()


if __name__ == '__main__':
    sys.exit(main(parse_args()) or 0)
