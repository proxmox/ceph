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
