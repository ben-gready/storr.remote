#!/usr/bin/env bash
set -ex

PORT=10022
NAME_IMAGE=richfitz/alpine-sshd:latest
NAME_CONTAINER=storr-remote-sshd

HERE=$(dirname $0)
docker build --rm -t $NAME_IMAGE $HERE
docker run -d --rm -p $PORT:22 --name $NAME_CONTAINER $NAME_IMAGE
rm -rf $HERE/keys
docker cp $NAME_CONTAINER:/root/.ssh keys
