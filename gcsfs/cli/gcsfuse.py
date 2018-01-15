import click
from fuse import FUSE

from gcsfs.gcsfuse import GCSFS


@click.command()
@click.argument('bucket', type=str, required=True)
@click.argument('mount_point', type=str, required=True)
@click.option('--token', type=str, required=False, default=None,
              help="Token to use for authentication")
@click.option('--project-id', type=str, required=False, default='',
              help="Billing Project ID")
@click.option('--foreground/--background', default=True,
              help="Run in the foreground or as a background process")
def main(bucket, mount_point, token, project_id, foreground):
    """ Mount a Google Cloud Storage (GCS) bucket to a local directory """
    print("Mounting bucket %s to directory %s" % (bucket, mount_point))
    FUSE(GCSFS(bucket, token=token, project=project_id),
         mount_point, nothreads=True, foreground=foreground)


if __name__ == '__main__':
    main()
