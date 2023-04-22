import io
import logging
import oci
import sys
from oci.object_storage import ObjectStorageClient

rps = oci.auth.signers.get_resource_principals_signer()
oci_client = ObjectStorageClient({}, signer=rps)


def handler(ctx, data: io.BytesIO = None):
    try:
        main()
    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))


def main():
    bucket_name = 'bucket-name' # update OCI object storage bucket name which you want to clean up.
    namespace = oci_client.get_namespace().data
    listfiles = oci_client.list_objects(namespace, bucket_name)

    if not listfiles.data.objects:
        print('No files found to be deleted')
        sys.exit()
    else:
        for filenames in listfiles.data.objects:
            oci_client.delete_object(namespace, bucket_name, filenames.name)

if __name__ == "__main__":
    main()
