#!/usr/bin/env python3

"""fifoconn.py

This can be imported as a library, to make use of FIFOConn, which allows for
simple sending of text or JSON messages to a FIFO service that expects
values as indicated below. To use the FIFOConn, use this file as an example,
or you can make your own:

    #!/usr/bin/env python

    input = vars().get('raw_input', input)

    with FIFOConn() as fconn:
        print("service input: use {!r}".format(fconn.thatRecv))
        print("service output: use {!r}".format(fconn.thatSend))

        input("start service now, using the above FIFOs, then hit Enter")

    print(fconn.communicate("hey there"))

If your service just echos what you send it, for example, then the above will
print "hey there".

You can use this to manually test your FIFO service (e.g., MATLAB, see below)
by calling, e.g.,

    req = WrapRequestProto(analytic_pb2.ImageManipulationRequest(
            request_id='id_foo',
            out_dir='/tmp/output'))
    print(fconn.communicateJSON(req))

If you just run this as a binary, this program starts an analytic service on
the given port, and then spawns a specified child process. It opens up two
named pipes with which it can communicate with the child. All analytic requests
that come into this service are forwarded to the child's "input" pipe, and all
responses are expected to come through the child's "output" pipe.

The protobuf messages are converted to JSON for the purposes of transmission,
and wrapped in a larger dict, like this for the request:

    request = {
      "type": "imgmanip",
      "value": actual_request_dict,
    }

Similarly, responses have the form:

    response = {
        "code": "OK",
        "value": actual_response_dict,
    }

The request and response values are both JSON representations of protocol
messages, as expected by the analytic protocol. The following types are allowed
in requests, with their corresponding protobuf names:

    imgmanip:    ImageManipulationRequest -> ImageManipulation
    vidmanip:    VideoManipulationRequest -> VideoManipulation
    imgsplice:   ImageSpliceRequest       -> ImageSplice
    imgcammatch: ImageCameraMatchRequest  -> ImageCameraMatch

If the protobuf name is not recognized, it is used as the type name in requests.

Note that proto fields are converted from snake case (e.g., 'out_dir') to camel
case ('outDir') when converting to JSON. The conversion happens automatically,
you just need to be aware of how it works when trying to produce JSON directly.

Thus, a JSON response would use the field name 'optOut' instead of the
proto-specified 'opt_out' as per the proto3 standard.

An Example with MATLAB:

One use for this is running MATLAB code. Given MATLAB code that uses a suitable
library for communicating via the named pipes (like AnalyticService.m), you can
start your MATLAB code as a child process with this service.

Assume your main script is located in "my_analytic.m". You can start this child
process with named pipes sent in environment variables as indicated above by
issuing the following command:

    ./fifoconn.py -- matlab -nodisplay -nosplash -nodesktop -r my_analytic

This starts up MATLAB with the instruction to run 'my_analytic.m', and removes
all of the cruft that you don't want on the command line. Since it doesn't
specify the in_key or out_key, the names of the FIFOs are set in environment
variables in the child process, as follows:

    input fifo: ANALYTIC_FIFO_IN
    output fifo: ANALYTIC_FIFO_OUT

These are *from the perspective of the child process*. You can also have them
sent on the child's command line as flags, using, for example

    ./fifoconn.py --in_key=-in_fifo --out_key=-out_fifo -- myproc

In this example, the child will be called thus:

    myproc -in_fifo <child_in_fifo_name> -out_fifo <child_out_fifo_name>

If an = suffix is specified, then flags are specified as, e.g., "--in_fifo=<name>".
In all cases, the prefix and suffix are preserved exactly. The '=' suffix
additionally consumes the space after the flag.
"""

import argparse
import json
import logging
import os.path
import select
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time

from google.protobuf import json_format

from analyticservice import AnalyticService


_DEFAULT_CHILD_IN_KEY = 'ANALYTIC_FIFO_IN'
_DEFAULT_CHILD_OUT_KEY = 'ANALYTIC_FIFO_OUT'
_PROTO_TYPE_MAP = {
    'ImageManipulationRequest': 'imgmanip',
    'VideoManipulationRequest': 'vidmanip',
    'ImageSpliceRequest': 'imgsplice',
    'ImageCameraMatchRequest': 'imgcammatch',
}


class TimeoutError(IOError):
    def __init__(self, op, timeout):
        return super(TimeoutError, self).__init__("timed out with op {!r} after {} seconds".format(op, timeout))


def RecvLine(f, timeout=0):
    selArgs = [[f], [], [f]]
    if timeout:
        selArgs.append(timeout)

    if not any(select.select(*selArgs)):
        raise TimeoutError("read", timeout)

    return f.readline()


def SendLine(f, data, timeout=0):
    selArgs = [[], [f], [f]]
    if timeout:
        selArgs.append(timeout)

    if not any(select.select(*selArgs)):
        raise TimeoutError("write", timeout)

    f.write(data + '\n')
    f.flush()


def WrapRequestProto(req):
    return {
        'type': _PROTO_TYPE_MAP.get(req.DESCRIPTOR.name, req.DESCRIPTOR.name),
        'value': json_format.MessageToDict(req),
    }

def UnwrapResponseProto(respDict, respProto):
    code = respDict.get('code', 'UNKNOWN')
    value = respDict.get('value')
    if code == 'OK':
        json_format.ParseDict(value, respProto)
    return code


class FIFOConn:
    def __init__(self, pipeNames=None, timeout=0):
        self.timeout = timeout
        self.lock = threading.Lock()

        self.fifoDir = None

        if not pipeNames:
            self.fifoDir = tempfile.mkdtemp()
            self.recvName = os.path.join(self.fifoDir, 'recv-child')
            self.sendName = os.path.join(self.fifoDir, 'send-child')

            os.mkfifo(self.recvName)
            os.mkfifo(self.sendName)
        else:
            self.recvName, self.sendName = pipeNames

        self.thatRecv = self.sendName
        self.thatSend = self.recvName

        self.sender = None
        self.receiver = None

    def _ensureOpen(self):
        if not (self.sender or self.receiver):
            # TODO: add os.O_NONBLOCK, loop and sleep on OSError until timeout.
            s = os.open(self.sendName, os.O_WRONLY)
            r = os.open(self.recvName, os.O_RDONLY)

            self.sender = os.fdopen(s, 'wt')
            self.receiver = os.fdopen(r, 'rt')

    def close(self):
        with self.lock:
            if self.sender:
                self.sender.close()
            if self.receiver:
                self.receiver.close()
            if self.fifoDir:
                shutil.rmtree(self.fifoDir)

    def __enter__(self):
        return self

    def __exit__(self, *unused_exc):
        self.close()

    def communicate(self, data):
        with self.lock:
            self._ensureOpen()
            SendLine(self.sender, data, timeout=self.timeout)
            return RecvLine(self.receiver, timeout=self.timeout)

    def communicateJSON(self, obj):
        return json.loads(self.communicate(json.dumps(obj)))


def spawnChild(fconn, args, inKey=None, outKey=None):
    if inKey is None:
        inKey = _DEFAULT_CHILD_IN_KEY
    if outKey is None:
        outKey = _DEFAULT_CHILD_OUT_KEY

    args = args[:]
    env = os.environ.copy()

    if inKey.startswith('-'):
        if inKey.endswith('='):
            args.append(inKey + shlex.quote(fconn.thatRecv))
        else:
            args.extend([inKey, fconn.thatRecv])
    else:
        env[inKey] = fconn.thatRecv

    if outKey.startswith('-'):
        if outKey.endswith('='):
            args.append(outKey + shlex.quote(fconn.thatSend))
        else:
            args.extend([outKey, fconn.thatSend])
    else:
        env[outKey] = fconn.thatSend

    # Never inherit stdin (not interactive!), always inherit stdout and stderr for logging.
    return subprocess.Popen(args, env=env, stdin=subprocess.DEVNULL, stdout=None, stderr=None)


def main(args):
    svc = AnalyticService()

    lock = threading.Lock()

    with FIFOConn(timeout=args.resp_timeout) as fconn:
        child = spawnChild(fconn,
                           args=args.child_args,
                           inKey=args.in_key,
                           outKey=args.out_key)
        print("Child process started with PID {}".format(child.pid))

        fatal = []
        def communicateProtos(req, resp):
            try:
                reqDict = WrapRequestProto(req)
                respDict = fconn.communicateJSON(reqDict)
                code = UnwrapResponseProto(respDict, resp)
                if code == 'OK':
                    return
                if code == 'UNIMPLEMENTED':
                    raise NotImplementedError(reqDict.get("type"))
                elif code in ('UNKNOWN', 'INTERNAL'):
                    raise RuntimeError(respDict.get("value"))
                else:
                    raise RuntimeError('unknown status code {!r}: {}'.format(code, respDict))
            except IOError as e:
                if e.errno == 32: # Broken Pipe
                    with lock:
                        fatal.append(e)
                    raise e
            except TimeoutError as e:
                # Timeouts are fatal because they put the fifo into an unknown
                # state for the next request.
                with lock:
                    fatal.append(e)
                raise e

        svc.RegisterImageManipulation(communicateProtos)
        svc.RegisterVideoManipulation(communicateProtos)
        svc.RegisterImageSplice(communicateProtos)
        svc.RegisterImageCameraMatch(communicateProtos)

        server = svc.Start(analytic_port=args.port)

        try:
            while True:
                with lock:
                    if fatal:
                        raise fatal[0]
                time.sleep(5)
                childRet = child.poll()
                # If the child exited, trigger server exit.
                if childRet is not None:
                    raise StopIteration(childRet)
        except StopIteration as e:
            print("Child exited with code {}".format(e))
            server.stop(0)
        except KeyboardInterrupt:
            print("Server stopped")
            server.stop(0)
            print("Killing child {}".format(child.pid))
            child.kill()
            return 0
        except Exception as e:
            print("Caught exception: {}".format(e))
            server.stop(0)
            print("Killing child {}".format(child.pid))
            child.kill()
            return -1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=('Start an analytic service that spawns a child process and communicates\n'
                     'with it via temporary named pipes.'))
    parser.add_argument('--port', dest='port', type=int, default=50051,
                        help='Port to listen on for gRPC requests.')
    parser.add_argument('--in_key', dest='in_key', type=str, default=_DEFAULT_CHILD_IN_KEY,
                        help=('Name of environment variable used by the child process to find its input pipe.\n'
                              'If it starts with "-", it will be sent as a flag. Trailing "=" forces the flag\n'
                              'to use the "=" convention. The prefix and suffix are preserved as is. Examples:\n'
                              '\n'
                              '--in_key=FOO    : the child gets its input pipe from env var FOO\n'
                              '--in_key=--foo  : the child gets its input pipe in args, as "--foo <name>"\n'
                              '--in_key=-foo= : the child gets its input pipe in args, as "-foo=<name>"\n'))
    parser.add_argument('--out_key', dest='out_key', type=str, default=_DEFAULT_CHILD_OUT_KEY,
                        help='Name child process uses to find its output named pipe. See --in_key for format')
    parser.add_argument('--resp_timeout', dest='resp_timeout', type=float, default=0,
                        help='Maximum time to wait for a FIFO response, in floating-point seconds.')
    parser.add_argument('child_args', type=str, nargs='+',
                        help='Child command line arguments, including binary. Specify after flags using "--".')
    sys.exit(main(parser.parse_args()))
