#!/bin/bash

export GOPATH=/usr/share/go-1.9/src/
export GOROOT=/usr/lib/go-1.9/
export PATH=$GOROOT/bin:$PATH

make build

