#!/usr/bin/env bash

rm -rf lib
docker run --rm -v `pwd`:/var/task:z lambci/lambda:build-python3.6 python3.6 -m pip --isolated install -t lib -r requirements.txt
