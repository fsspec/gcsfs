gcsfs is a community maintained project. We welcome contributions in the form of bug reports, documentation, code, design proposals, and more.

## Project specific notes

For testing remote API calls this project uses the docker image `fsouza/fake-gcs-server`.
See the docs for more information https://gcsfs.readthedocs.io/en/latest/developer.html and
the invocation in `gcsfs.tests.conftest.docker_gcs`.
