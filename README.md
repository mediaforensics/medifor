# MediaForensics Protocol Buffer and Analytic Service Code

This repository is intended to provide all of the tools necessary to build and test
media forensic analytics for use as microservices within a distributed system.
Currently the repository contains the analytic wrapper which is used to provide a
consistent api for communicating with analytics.

Currently provided are the python libraries and proto files required to develop
and wrap analytics in python.  Future commits will include client code for testing
analytics and support for additional languages.

## Developing an Analytic in Python

#### Overview
For the v1 medifor library (used for the DARPA MediFor program), four media forensic
tasks are supported. These tasks are:
 * Image Manipulation Detection and Localization: The analytic receives an
 image and provides an indicator score on the interval [0,1], with higher scores
 indicating that the image is more likely top have been manipulated.  Analytics
 may also provide a grayscale mask of the image where pixel values closer to 0
 represent regions that were more likely to have been manipulated, and pixel
 values closer to 255 represent regions which were less likely to have been manipulated.
 * Image Splice Detection and Localization: The Analytic receives a probe image
 and a donor image and provides an indicator score on the interval [0,1] which
 indicates how likely it is that the donor image was spliced into the probe image,
 and may provide a mask (as in the image manipulation detection task) indicating
 which regions of the probe image are likely to have come from the donor. [Note:
 This task will likely be removed for v2]
 * Video Detection and Localization: The analytic receives a video and provides
 an indicator score (same as in the image manipulation detection task).  The analytic
 may also attempt to localize detected manipulations either temporally (which frames
 were manipulated), spatially (where in the frames do the manipulations take place),
 or both.
 * Image Camera Verification: Where an analytic receives an image and an HP camera
 device ID and must provide a score indicating how likely the probe image is to have
 come from that camera. [Note: This task will likely be removed for v2]

The medifor library provides a gRPC wrapper which provides an endpoint for each of
these media forensic tasks, and allows analytic developers to register a function
to one or more of these endpoints.  The following sections will provide a
step-by-step guide for developing an analytic using the medifor library.

#### Installation
Clone the medifor repository and run the install script.
```
$ git clone https://github.com/mediaforensics/medifor.git`
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
/ ImageManipulationRequest is used to ask an analytic indicator whether
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

from medifor.v1 import analytic_pb2, analyticservice

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

from medifor.v1 import analytic_pb2, analyticservice

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
Once an analytic has been wrapped using the medifor library, it can be tested
using the [medifor client](python/medifor/medifor-client).  The medifor client
can be used in two primary ways:
 1) To run the analytic over a single image/video
 2) To run the analytic over every image/video in a specified directory.

Usage fo the client:
```
$ python mediforclient [flags] <command> [options]
```
The client as three different commands, `imgmanip`, `vidmanip`, and `detectbatch`.
Each of command has it's own set of flags and arguments in addition to the global
flags provided to the client.  The global flags include the host and port of the
analytic as well as flags for path translation for mapping image/video uri and
output  directories from the client perspective to the analytic container
perspective.  The client defaults to not using any path translation and looking
for the analytic at `localhost:50051`. For more information use:
```
$ python mediforclient --help
```

The `imgmanip` and `vidmanip` commands operate in the same manner and take the
media file uri as a postitional argument and the output directory as a required
flag.  Example usage:
```
$ python mediforclient imgmanip /path/to/image.jpg -o /output
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
$ pyton mediforeclient detectbatch -d /path/to/image_folder/ -o /output
```
