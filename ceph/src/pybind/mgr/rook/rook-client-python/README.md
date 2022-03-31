# `rook-client` Python API Classes  

The Rook CRDs are evolving over time and to verify that clients make use of the correct definitions, 
this project provides generated API classes to let users write correct clients easily.

Right now, it supports three operators:

* Ceph
* Edgefs
* Cassandra

It is used to type check client code against the Rook API using [mypy](mypy-lang.org).

Inspired by [kubernetes-client/python](https://github.com/kubernetes-client/python/tree/master/kubernetes/client/models)

Main uses case is the mgr/rook orchestrator module of the Ceph MGR

## Code Generation

This project contains a `generate_model_classes.py` which can generate 
model classes from Kubernetes CRDs and is **independent** of Rook. 

Usage:

```
python generate_model_classes.py <crds.yaml> <output-folder>
```

## Installing `rook-client`

To install `rook-client`, run:

```bash
pip install -e 'git+https://github.com/ceph/rook-client-python#egg=rook-client'
```


## Usage

```python
def objectstore(api_name, name, namespace, instances):
    from rook_client.ceph import cephobjectstore as cos
    rook_os = cos.CephObjectStore(
        apiVersion=api_name,
        metadata=dict(
            name=name,
            namespace=namespace
        ),
        spec=cos.Spec(
            metadataPool=cos.MetadataPool(
                failureDomain='host',
                replicated=cos.Replicated(
                    size=1
                )
            ),
            dataPool=cos.DataPool(
                failureDomain='osd',
                replicated=cos.Replicated(
                    size=1
                )
            ),
            gateway=cos.Gateway(
                port=80,
                instances=instances
            )
        )
    )
    return rook_os.to_json()
```

## Demo

![](rook-python-client-demo.gif)

## Regenerate

To re-generate the python files, run 

```bash
./generate.sh
```
