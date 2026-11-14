gcsfs is a community maintained project. We welcome contributions in the form of bug reports, documentation, code, design proposals, and more.

## Project specific notes

For testing remote API calls this project uses [VCR](https://vcrpy.readthedocs.io/en/latest/). See the docs for more information https://gcsfs.readthedocs.io/en/latest/developer.html.

## Dependency on fsspec

Raise the `fsspec` lower bound only when gcsfs code requires a feature or fix first released in that `fsspec` version, and name it in the PR (e.g. #993 needed the `adaptive` cache from `2026.9.0`). Do not raise it just to match the latest `fsspec` or as part of a release PR. Do not add an upper bound. See the [release docs](https://gcsfs.readthedocs.io/en/latest/release.html).
