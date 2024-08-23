#!/bin/bash
# Build script
set -eo pipefail

build_tag=$1
name=$2
node=$3
org=$4

docker build -t ${imageName}:${imageTag} .
echo {\"image_name\" : \"${imageName}\", \"image_tag\" : \"${build_tag}\", \"node_name\" : \"$node\"} > metadata.json
