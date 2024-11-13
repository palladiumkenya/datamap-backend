#!/bin/bash

# Define variables
CONTAINER_NAME="datamap-backend"

# Pull the latest code
git pull origin master

# Build the new Docker image
docker compose build datamap

# Stop the running container if it's running
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
fi

# Start the new container
docker compose up -d datamap

echo "Datamap Backend Container updated successfully!"
