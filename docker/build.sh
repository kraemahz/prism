#! /bin/bash
docker build --network host -f docker/Dockerfile -t prism:latest .
