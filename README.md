# MediaForensics Protocol Buffer and Analytic Service Code

This repository is intended to provide all of the tools necessary to build and test
media forensic analytics for use as microservices within a distributed system.
Currently the repository contains the analytic wrapper which is used to provide a
consistent api for communicating with analytics.

Currently provided are the python libraries and proto files required to develop
and wrap analytics in python.  Future commits will include client code for testing
analytics and support for additional languages.

## Changelog

 - v0.1.0:
    * Setup medifor.v1 package
    * Refactored mediforclient and cli to be included in the medifor.v1 package
    * Added streaming service to analyticservice and `streamdetect` command to
    the cli.

-  v0.3.0   
    * Refactored CLI to be included in the medifor package (i.e. `python -m medifor --help`)
    * Added provenance filtering and graph building client


## Quickstart
  (Note: Before starting you may wish to create a python virtual environment (e.g., `python venv env`) and then activate that environment (`source env/bin/activate`)
  
  1) Install the medifor library using `python3 -m pip install git+https://github.com/mediaforensics/medifor.git`


  2) Wrap your analytic using the `analyticservice` library.  The two easiest ways
 to do this are:

   - Create a new python script and import the required modules:
      `from medifor.v1 import analytic_pb2` and 
      `from medifor.v1.analyticservice import AnalyticService`
    as well as your current analytic functions. Create a new function that takes
    as input a request and response object.  E.g.,

    def process_image_manip(req, resp):
      # Call your analytic function(s) here

      # Fill out the resp object with the results

      # No return necessary


   - Modify your existing analytic by adding a function which takes as input a
    request and response as shown above to act as the entrypoint of your function.

  3) In the main section of the script (in either case) create the analytic service,
  register your function(s), and start the service.

  ```
  if __name__ == '__main__':
    # You can do one-time start up and initialization tasks here
    svc = AnalyticService()
    svc.RegisterImageManipulation(process_image_manip)

    sys.exit(svc.Run())
  ```

  4) Test the analytic using the medifor cli:

  At this point you may wish to test your analytic prior to building the container. To do this simply run your analytic in another window (e.g., `python analytic.py`) and then, while the analytic is running, use the medifor CLI to send test images/videos to the analytic. 


    python -m medifor [args] <command> [file] -o <path to output directory>


   Where the command is `imgmanip` for image manipulation detection or `vidmanip`
   for video manipulation detection.

  Testing the analytic this way can be useful since it does not require mounting volumes and path translation, which is often required when running analytic containers.

  5) Build an analytic container.

      - Create the Dockerfile in your analytic directory.  An example is provided below.

      ```
      FROM datamachines/grpc-python:1.15.0

      # Install the medifor library
      RUN pip install git+https://github.com/mediaforensics/medifor.git

      # Additional installation for your analytic
      COPY . /app

      WORKDIR /app

      EXPOSE 50051

      CMD ["python", "example_analytic.py"]
      ```
      - Build the container (here we are calling our container image `example_analytic`)
      ```
      docker build -t example_analytic .
      ```
      - Run the docker container with the port exposed and any file locations mounted.
      In this example we have mount `/tmp/input` as `/input` and `/tmp/output` as
      `/output` in the container.

      ```
      docker run -p 50051:50051 -v /tmp/input:/input -v /tmp/output:/output example_analytic
      ```

  6) Use the medifor cli to run media through the analytic as shown above.

## Developing an Analytic in Python

#### Overview
For the v1 medifor library (used for the DARPA MediFor program), the primary media forensic
tasks supported are:
 - Image Manipulation Detection and Localization: The analytic receives an
 image and provides an indicator score on the interval [0,1], with higher scores
 indicating that the image is more likely top have been manipulated.  Analytics
 may also provide a grayscale mask of the image where pixel values closer to 0
 represent regions that were more likely to have been manipulated, and pixel
 values closer to 255 represent regions which were less likely to have been manipulated.

 - Video Detection and Localization: The analytic receives a video and provides
 an indicator score (same as in the image manipulation detection task).  The analytic
 may also attempt to localize detected manipulations either temporally (which frames
 were manipulated), spatially (where in the frames do the manipulations take place),
 or both.

The medifor library provides a gRPC wrapper which provides an endpoint for each of
these media forensic tasks, and allows analytic developers to register a function
to one or more of these endpoints.  The following sections will provide a
step-by-step guide for developing an analytic using the medifor library.

#### Installation
Clone the medifor repository and run the install script.

```
$ git clone https://github.com/mediaforensics/medifor.git
$ cd medifor
$ pip install .
```

This will install the `medifor.v1` module, which can be used to build a media
forensic analytic.

#### Analytic API
The medifor api is defined in the protobuf files located at [`proto/medifor/v1`](proto/medifor/v1).  
Of interest to analytic developers is the [`analytic.proto`](proto/medifor/v1/analytic.proto)
file.  This file defines the services as well as the request and response objects.
To better understand the `analytic.proto` file we'll walk through the different
components of interest in the file.

The analytic services are defined at the bottom of the file.
```
service Analytic {
  rpc DetectImageManipulation(ImageManipulationRequest) returns (ImageManipulation);
  rpc DetectImageSplice(ImageSpliceRequest) returns (ImageSplice);
  rpc DetectVideoManipulation(VideoManipulationRequest) returns (VideoManipulation);
  rpc DetectImageCameraMatch(ImageCameraMatchRequest) returns (ImageCameraMatch);
  rpc Kill(Empty) returns (Empty);
}
```
Of interest to analytic developers are the first four endpoints, which define
the endpoints for the media forensic tasks.  If a given analytic performs image
manipulation detection and localization then it would need to register a function
to the `DetectImageManipulation` endpoint, which takes as input an `ImageManipulationRequest`
and returns an `ImageManipulation` object. [Note: When implementing your analytic
function in python both the request and response will be passed into the function
as arguments.  Setting fields in the response object is sufficient, and the
function need not actually return anything.  This is covered further below].

The request and response objects are also defined in the proto file.  For example
here is the definition of the `ImageManipulationRequest` object:
```
// ImageManipulationRequest is used to ask an analytic indicator whether
// a particular image is likely to have been manipulated after capture.
// NextID: 5
message ImageManipulationRequest {
  // A unique ID for each request. Usually a UUID4 is used here.
  string request_id = 1;

  // The image to check for manipulation.
  Resource image = 2;

  // The location on the local file system where output masks and supplemental
  // files should be written. The locations of these must be referenced in the
  // response, or they will be lost. Similarly, any files written outside of
  // this directory are not guaranteed to survive the return trip: this
  // directory is an indicator of what the *caller* is able to pull from (but
  // the path is from the service's perspective), so writing outside of it may
  // render the files inaccessible.
  string out_dir = 3;

  // The high-provenance device ID, if known, of the device that captured the
  // image.
  string hp_device_id = 4;
}
```
Each object has a number of fields (which are numbered) with explicit types.  Some
fields, such as the `image` field above, have nonstandard types.  These types are
defined elsewhere in the file.  For example the `Resource` type:
```
// Resource holds information about, typically, a blob of data. It references this
// data by URI, which might be a file path, for example.
message Resource {
  // The location of the media. For local files, this just looks like a file path.
  string uri = 1;

  // The mime type of this resource (file).
  string type = 2;

  // Free-form notes about this resource.
  string notes = 3;
}
```
It's also important to mention enum types such as `DetectionStatus`
```enum DetectionStatus {
  DETECTION_STATUS_NONE = 0;
  DETECTION_STATUS_SUCCESS = 1;
  DETECTION_STATUS_FAILURE = 2;
}
```
As you would expect, fields of this type can only have one of the values listed
in the type definition, and should be set by referencing the enum value, e.g.,
`status = analytic_pb2.DETECTION_STATUS_SUCCESS`.

One final note is that all fields have default values.  For example, the default
value of a float is `0.0`, while the default value of a string is `""`.  If a field
in the response is left unset, the default value will be returned.  For enum fields
the default value is the entry with value `0` (i.e. in the `DetectionStatus` example
the default value is `DETECTION_STATUS_NONE`).  This usually won't be impactful to
analytic developers, however it may be helpful with debugging, as a default value
may mean that a field was not set in the response.

#### Creating an analytic
The basic steps for creating an analytic are 1) Create a new python script which
imports `medifor` modules. 2) Create a function in that script which accepts as
arguments a request protocol buffer and a response protocol buffer.  This function
will perform the actual medifor task, or more likely, will parse the request protobuf
to load the image, call your library functions to perform the analysis, and then
update the response protobuf with the results, 3) In the
`__main__` of your script start an analytic service, perform any necessary
initialization steps, register your function from (1), and start the service.
The service will listen for a request of the registered type and call your
registered function when one is received.

###### 1) Create the Python Script
Create a new python script with the following imports:
```
import os
import os.path
import sys
import time

from medifor.v1 import analytic_pb2
from medifor.v1.analyticservice import AnalyticService

# additional modules which required to run your algorithm should be imported as well.
# It may be preferable to do all of your actual analysis in library functions and import those
# For the purposes of this example we will do that (as shown below)

from foo import detect_img_manip
```

The most significant import is `from medifor.v1 import analytic_pb2, analyticservice`.  
The `analyticservice` module allows you to register your function(s) and start a
service, while `analytic_pb2` allows you to work with protobuf objects.  The other
modules are not strictly required but will be helpful in most analytics.  The final
import, as explained in the comment, is there to provide an example of an analytic
which imports from a library of media forensics functions.  This is also not a
requirement but is meant to provide an example for developers who have media
forensic analytics written and merely wish to wrap them using the medifor library.

###### 2) Create the Function to Register
Create a function which has two input arguments, the request object and the
response object.  The primary purpose of this function is to parse the request
object and set the fields in the response object.The response object contains the
image uri, metadata, and the output directory (which is used to by the
analytic to save mask files).  The most important fields in the response object
are the `score` and `localization` fields, however the `opt_out` field can also
be used to indicate that a given image/video was not processed at all by the
analytic.  This is usually done in cases where an analytic works on a specific
subset of image/video types and should not be used to ignore errors.  Errors are
handled by the wrapper and a full stack trace will be returned in the response.  
Allowing these errors to be returned can often simplify the debugging process.

An example function might look something like this:
```
def process_image_manip(req, resp):

  with PIL.Image.Open(req.image.uri) as img:
    score, mask, mask_filename = detect_img_manip(req.image.uri)
    resp.score = score
    mask_filename = os.path.join(req.out_dir, mask_filename)
    resp.localization.mask.uri = mask_filename
    resp.localization.mask.type = 'image/png'
    mask.save(mask_filename)
```
In this instance the function is only used to load the image using the uri
provided in the request and pass that image to the function imported from the
analytic developer's module.  The score, mask image, and a filename for the mask
are returned and then used to fill out the response object in the function defined
above.  It is worth noting that the output directory provided in the request
(`req.out_dir`) is used to provide a path when writing the mask.  This is
important when mounting shared storage to the container and can help mitigate the
risk of mask name conflicts.

###### 3) Register the Function and Run the Service
Lastly, in the `__main__` portion of the script you will need to create an
analytic service, register your function with the appropriate endpoint, and then
start the service.  Example:
```
if __name__ == '__main__':
  # You can do one-time start up and initialization tasks here
  svc = AnalyticService()
  svc.RegisterImageManipulation(process_image_manip)

  sys.exit(svc.Run())
```

#### Example Analytic
Combining the three steps above when creating our analytic yields the following:
```
import os
import os.path
import sys
import time

from medifor.v1 import analytic_pb2
from medifor.v1.analyticservice import AnalyticService

# additional modules which required to run your algorithm should be imported as well.
# It may be preferable to do all of your actual analysis in library functions and import those
# For the purposes of this example we will do that (as shown below)

from foo import detect_img_manip


def process_image_manip(req, resp):

  with PIL.Image.Open(req.image.uri) as img:
    score, mask, mask_filename = detect_img_manip(req.image.uri)
    resp.score = score
    mask_filename = os.path.join(req.out_dir, mask_filename)
    resp.localization.mask.uri = mask_filename
    resp.localization.mask.type = 'image/png'
    mask.save(mask_filename)

if __name__ == '__main__':
  # You can do one-time start up and initialization tasks here
  svc = AnalyticService()
  svc.RegisterImageManipulation(process_image_manip)

  sys.exit(svc.Run())
```

A very short and simple script.  Now you can of course choose to include more
of the actual analysis in this script or create and register additional functions,
but hopefully this illustrates how quickly and easily an analytic can be wrapped
using the medifor library.

#### Using the MediFor Client
A client library and CLI have been provided for communicating with media forensic analytics.

The medifor client cli can be used to run or test media forensic analytics.  It
currently has  3 primary commands:
 1)`detect` - used for image/video manipulation detection
 2) `provenance` - used for provenance filtering & graph building tasks
 3) `pipeline` - used to talk to an existing medifor pipeline

The `detect` command is the primary use of the medifor cli and has subcommands: 
 1) `imgmanip` - Used to run the analytic over a single image
 2) `vidmanip` - Used to run the analytic over a single video
 3) `detectbatch` - Used to run the analytic over every image/video in a specified directory.
 4) `streamdetect` - Used to run the analytic over a single image or video which
 is streamed to the analytic.  NOTE: The `analyticservice` library handles the streaming
 of input and output files and input/output files are written to temporary directories
 in the analytic container.  As a result analytics wrapped using the `analyticservice`
 library (v1.0.0 or higher) can be run using the `streamdetect` command.

Usage for the cli:
```
$ python -m medifor [flags] <command> [options]
```

Each of command has its own set of flags and arguments in addition to the global
flags provided to the client.  The global flags include the host and port of the
analytic as well as flags for path translation for mapping image/video uri and
output  directories from the client perspective to the analytic container
perspective.  The client defaults to not using any path translation and looking
for the analytic at `localhost:50051`. For more information use:
```
$ python -m medifor.v1.cli --help
```

The `imgmanip` and `vidmanip` commands operate in the same manner and take the
media file uri as a postitional argument and the output directory as a required
flag.  Example usage:
```
$ python medifor.v1.cli imgmanip /path/to/image.jpg -o /output
```
The `detectbatch` command has two required flags, the input directory and the
output directory.  The input directory is the path to the folder containing the
media files.  The input directory should contain only image or media files, and
currently the client will not traverse any subdirectories.  The output directory
is used as the parent directory for the output folders for each file.  Each request
is given a UUID before being sent to the analytic.  This UUID is used to name the
output folder (which is a subdirectory for the output directory provided.  The
UUID is also used as the key for the results which are output as JSON.  Example
usage:
```
$ python medifor.v1.cli detectbatch -d /path/to/image_folder/ -o /output
```

The `streamdetect`command has 3 required flags:
 `--probe`: The image or video to be streamed to the analytic.
 `--container_out`: The output directory path for writing files in the container.
 `--local_out`:  The output directory path on the client machine.  Used to write
 mask and other output files that are streamed back to the client from the analytic
 container.


The library can be used to incorporate client calls into your own code.  To use
the client library import the `mediforclient` class:

```
from medifor.v1 import mediforclient
```

The medifor class takes as arguments the host and port of the analytic (defaulted to
  localhost:50051) as well as source and target paths for mapping the paths of input
  and output locations between the client file system and the container.  For example,
  if the folder `/tmp/input/media` is mounted in the container as `/input`, and
  `/tmp/output/` as `/output`, then you would instantiate the class as

  ```
  client = MediforClient(host=host, port=port, src="/tmp/input/media", targ="/input", osrc="/tmp/output", otarg="/output")
  ```

  where `host` and `port` are the host and port of the analytic container to communicate with.

The client can be used to send requests for image and video manipulation detection,
as well as batch detection, which creates requests for all media files in a directory.
Each of these functions provide media uri to the analytic and require that the analytic
have access to the media location.  There is also a `stream_detection` function which can
be used to stream the contents of a media file to the analytic.  Both the stream detection
and batch detection functions use the mimetype library to identify the media type from the
file extension.  The MediforClient class can also be used as a superclass to your own
client class if you with to add additional capabilities.


#### Building an Analytic Container
This is meant to provide instruction and a simple example for those unfamiliar with docker
and how to incorporate the medifor library. To create a docker container for your analytic
you will need to create a Dockerfile and install the medifor library.  For convenience,
Data Machines has provided abase image, but it is not required.  Using a different image
may require additional modifications to the Dockerfile.  An example Dockerfile is provided below.


    FROM datamachines/grpc-python:1.15.0

    # Install the medifor library
    RUN pip install git+https://github.com/mediaforensics/medifor.git

    # Additional installation for your analytic
    COPY . /app

    WORKDIR /app

    EXPOSE 50051

    CMD ["python", "example_analytic.py"]


Build and tag the container (In the below example we are calling our container image `example_analytic`
and tagging it `v1`):


    docker build -t example_analytic:v1 .


Run the docker container with the port exposed and any file locations mounted.
To expose the port add the `-p` followed by the port mapping
`<container-port>:<host-port>`.  In order for the
analytic to read files from the host filesystem the relevant directories need to
be mounted inside the container.  This can be done by passing the `-v` flag with
the path mapping with the format `-v host_path:container_path`. If you are not
able to mount the media files inside the container, the streaming option may
still be used to to communicate with the analytic.  To run out previous example,
use the command:


    docker run -p 50051:50051 -v /tmp/input:/input -v /tmp/output:/output example_analytic
