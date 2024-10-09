#!/bin/bash

A3M_DOCKER_IMAGE="ghcr.io/artefactual-labs/a3m:v0.7.9"
PROCESSING_DIRECTORY='/tmp/curate/preservation'

# Create processing directory if it doesn't exist
mkdir -p "$PROCESSING_DIRECTORY"

# Create a3m-network if it doesn't exist
if ! docker network inspect a3m-network >/dev/null 2>&1; then
    docker network create a3m-network
fi

# Start a3md container
if [ "$(docker ps -aq -f name=a3md)" ]; then
    # Start the existing container
    docker start a3md
else
    # Start a new a3md container
    docker run -d --name a3md --user 1000 -v "$PROCESSING_DIRECTORY:$PROCESSING_DIRECTORY" --network a3m-network -p 7000:7000 --restart on-failure -e A3M_DEBUG=yes "$A3M_DOCKER_IMAGE"
fi
