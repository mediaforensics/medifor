# Example Provenance Filtering Analytic

This directory contains an example provenance analytic which uses the front end gRPC API as well as a simple REST API to make calls to the FAISS
index.  The example code can be used to demonstrate the general workflow of a provenance filtering analytic, though you will need to make changes to
have it fit your implementation.

###Install Medifor package in your local machine
Install Medifor package in you local conda env.

 ```bash
 pip install git+https://github.com/mediaforensics/medifor.git@develop
 ```
###Build filtering image and index image
Filtering image:

 ```bash
 cd filter_svc
 docker build -t mediaforensics/samples:prov-filter-svc ./
 ```

Index image:

 ```bash
 cd index_svc
 docker build -t mediaforensics/samples:prov-index-svc ./
 ```

###Test Command

In terminal 1, run command from this directory
 
 ```bash
 docker-compose up
 ```

In terminal 2, run command from this directory. Note the image file may be corrupted by git. You may need to download the image file from git manually.
 
 ```bash
 python -m medifor -s $PWD -t /input  provenance filter $PWD/assets/Koala.jpg
 ```

Modify the mount path for `filtersvc.py` in the `docker-compose.yml` to point to your own images to run additional images through the example (It's a toy example though, so the index is just random).
