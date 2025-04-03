#!/bin/bash
set -e

# Build the Docker image and tag it as "etl-tool-test"
docker build -t etl-tool-test .

# Run the tests in the container and remove the container after running
docker run --rm etl-tool-test

