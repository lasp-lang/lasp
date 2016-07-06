#!/bin/bash

echo "Building Docker image."
docker build -f Dockerfiles/lasp-evaluation -t cmeiklejohn/lasp-evaluation .

echo "Launching docker instance."
docker run --dns=4.2.2.1 -e "EVALUATION_PRIVATE_KEY=$EVALUATION_PRIVATE_KEY" -t -i cmeiklejohn/lasp-evaluation
