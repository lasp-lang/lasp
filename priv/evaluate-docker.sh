#!/bin/bash

if [ ! -z "$BUILD" ]; then
  echo "Building Docker image."
  docker build -f Dockerfiles/lasp-evaluation -t cmeiklejohn/lasp-evaluation .
fi

echo "Launching docker instance."
docker run --dns=4.2.2.1 -e "DCOS=true" -e "EVALUATION_PASSPHRASE=$EVALUATION_PASSPHRASE" -t -i cmeiklejohn/lasp-evaluation
