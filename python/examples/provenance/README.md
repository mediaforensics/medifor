# Example Provenance Filtering Analytic
This directory contains an example provenance analytic which uses the front end gRPC API as well as a simple REST API to make calls to the FAISS
index.  The example code can be used to demonstrate the general workflow of a provenance filtering analytic, though you will need to make changes to
have it fit your implementation.
## Running the Test Analytic
 - Build the FAISS container `docker build -t faiss-example .`
 - Generate a dummy index `docker run -v ${PWD}:/data -i --entrypoint '/bin/bash' faiss-example -c "python3 buildrandom.py /data/index.faiss"`
 - Run the FAISS container `docker run -dv ${PWD}:/data -p 8080:8080 faiss-example`
 - Run the front end service `python testsvc.py`
 - Send requests via medifor tool:

 ```
 $ cd ../../cmd/medifor
 $ go build
 $ ./medifor provenance --limit 5 filter "path/to/image.ext"```