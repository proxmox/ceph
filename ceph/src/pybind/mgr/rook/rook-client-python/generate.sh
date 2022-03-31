#!/bin/bash

set -ex

if [ -z "$1" ] ; then
    rook_base="${GOPATH:-$HOME/go}/src/github.com/rook/rook"
else
    rook_base="$1"
fi
crd_base="$rook_base/cluster/examples/kubernetes"

cd "$(dirname "$0")"

if ! [ -x "$(command -v python3)" ]; then
  echo 'Error: python3 is not installed.' >&2
  exit 1
fi

if [ ! -d venv ]
then
  python3 -m venv venv
  . venv/bin/activate
  pip install -r requirements.txt
else
  . venv/bin/activate 
fi

python generate_model_classes.py "$crd_base/ceph/crds.yaml" "rook_client/ceph"
#python generate_model_classes.py "$crd_base/cassandra/operator.yaml" "rook_client/cassandra"

python setup.py develop

tox --skip-missing-interpreters=true -- --crd_base="$crd_base"

deactivate
