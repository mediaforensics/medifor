# MediFor command-line utility.

The `medifor` utility is useful for sending requests to analytics; it
packages up images or videos into a format that is understood by the gRPC
interface and asks the analytic to do the detection.

## Basic Use

Run the utility with the `help` command to get help about usage.

Many of the commands attempt to connect to an analytic server. The default host
and port for that server is `:50051`, which means "port 50051 on the local
host, visible to the local network". You can change it using the `--host` flag
if needed.

If you want to check that you can talk to your analytic service at all, use the
`health` command to see if you can connect to it and it returns a "serving"
status. That can be really helpful for debugging.

If you are running your service inside of a container and the health check is
not coming back properly, try running the container *without detaching* so you
can see its output logs easily, and run the health check against it from a
different terminal. That can help you find bugs.

## Running Single-File Detections

The `medifor detect` command has several subcommands, including `imgmanip`,
`vidmanip`, and others. These do essentially what you think they do: `medifor
detect imgmanip` sends an image manipuation detection request to your analytic,
and outputs the response (which will point to a mask file if you write one).

The output can be formatted as JSON or as a text-serialized protocol buffer. Both are readable formats.

### Defaults Are Not Shown

If a value in your output is the default "zero" value, it *will not be
displayed*. That is fine and expected. Defaults are never sent over the wire
because the absence of a value means, in proto3, that it has its default value.
Thus, the absence of an int will be seen as that int having the value 0. The
absence of a string means it has the value "", etc.

*If you do not use a value, leave it as its default.* Resist the temptation to
specify something like `-1` to mean "I don't set this." Just leave things as
their defaults when unused. The protocol is set up to make it easy, for
example, to specify that you are not providing a score. That's what
`OPT_OUT_DETECTION` means. Use those fields that indicate inaction to specify
inaction, rather than producing magic values.

## Running Batch Detections

To run a set of detections, e.g., image manipulations, we have provided the
`medifor detect batch` command. This accepts a single `.csv` file as input,
using the NIST input CSV specification, where the following columns are
present (for the image manipulation "MDL" task, at least---see NIST
documentation for more details):

- TaskID
- ProbeFileID
- ProbeFileName
- ProbeWidth
- ProbeHeight
- HPDeviceID (usually not present)
- HPSensorID (usually not present)

The `detect batch` command will read the rows from a CSV of this format, and
attempt to run requests for each row against your analytic. The output is a
JSON-serialized `Detection` protocol buffer, which contains information like
when the request was made, what status it returned, the contents of the
request, and the contents of the response.

In normal use, this output should be written to a file, but if you do not
specify an output file, it is written to STDOUT. If you only redirect STDOUT to
a file (not STDERR!), you will capture the structured log output.

To run the batch detection command, you might choose to use the sample assets
in this project, e.g.,

```bash
$ medifor detect batch assets/manipulations.csv output.json
```

This instructs the tool to read its tasks from the given CSV file and output
its results to the specified `output.json` file.

### Packaging Batch Results for Validation/Scoring

To package up the results of your batch run into a format accepted by NIST
(which we use for validating take-home results, for example), you can use the
`nist package` command, like so:

```bash
$ medifor nist package output.json my-batch-output.tar.gz
```

This also takes two arguments, namely the JSON log from a `detect batch`
command, and the name of a new tarball that it will create. This command looks
through all of the entries in `output.json`, finds all of the corresponding
output mask files, and packages them all into a NIST tarball containing a CSV
file representing those rows, and a `mask` directory containing the mask files.

When submitting results, we strongly suggest that you *keep your original
files, including the JSON log* for debugging purposes, and send the tarball to
the program-specified submission location.

## Sending Requests to an Analytic Service Container

Note that any request you send will necessarily reference files. Those file
paths need to be either visible from the analytic's perspective, or
translatable into a path that is thus visible.

In other words, if you run something like

```bash
$ medifor detect imgmanip /path/to/images/img.png
```

The analytic server needs to be able to see `/path/to/images/img.png`. If it's
running in a container, you might have mounted that directory somewhere else,
in which case you need to do path translation.

Let's suppose that you have set up a mount point in your container like this (host -> container):

- Host: `/path/to/images`
- Container: `/data/images`

You can still call `medifor detect imgmanip /path/to/images`, but you will need to supply a flag telling it how the mount point is mapped:

```bash
$ medifor -s /path/to/images -t /data/images detect imgmanip /path/to/images
```

There are similar flags for mapping the *output* directory, as well. So, for
example, if you want the output mask images written to `/path/to/outputs`, but
the container has that location mapped to `/data/outputs`, you would specify
additional flags like this:

```bash
$ medifor -s /path/to/images -t /data/images \
          -S /path/to/outputs -T /data/outputs \
          detect imgmanip /path/to/images/img.png
```

This all assumes that you have run your analytic inside of a docker container
with appropriate volume mounts. Here's how you might go about doing that:

```bash
docker run \
  --mount type=bind,writable=false,source=/path/to/images,target=/data/images \
  --mount type=bind,writable=true,source=/path/to/outputs,target=/data/outputs \
  --publish 50051:50051 \
  --detach my_analytic_image
```

That will start up `my_analytic_image` in a container that can see files in
`/data/images` and can write to `/data/outputs`, as well as publishing port
`50051` to the host network.

Then you can run `medifor` on the host, provided that you only reference images
in `/path/to/images` and its subdirectories.

File mappings can get people turned around, so just remember this: they mirror your mounts *exactly*. Thus, if you have mounts like the following from your host to your container:

- `/host/inputs` -> `/container/inputs`
- `/host/outputs` -> `/container/outputs`

Then you can specify those mappings in the `medifor` command with these flags:

`-s  /host/inputs -t /container/inputs -S /host/outputs -T /container/outputs`

If you get tired of typing that out all the time, you can use a config YAML file, like so:

```yaml
source: /host/inputs
target: /container/inputs
out_source: /host/outputs
out_target: /container/outputs
```

and then you can refer to the config file using the `--config` flag, or drop it in `$HOME/.config/medifor.yml`.

## Building

This utility can be built from the root of this repository by issuing

```
$ go build ./...
```

This creates a release binary for your platform inside of the `release` directory.
