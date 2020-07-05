#!/bin/bash

export GOPATH=$HOME/src/go/
export GOROOT=/usr/lib/go-1.9/
export PATH=$GOROOT/bin:$PATH

make build

