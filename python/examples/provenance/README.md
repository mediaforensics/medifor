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

Test Command
 ```
 # Terminal 1
 docker-compose up

 # Terminal 2
 python -m medifor -s $PWD -t /input  provenance filter $PWD/python/examples/provenance/assets/Koala.jpg
 ```

Modify the mount path for `filtersvc.py` in the `docker-compose.yml` to point to your own images to run additional images through the example (It's a toy example though, so the index is jsut random).