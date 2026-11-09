"""Prepare the objects of a Rapid Cache Ultra (RCU) run from inside the cluster.

    stage-rapid SRC DST  Copy every object under SRC to the same relative path
                         under DST. gcsfs writes RAPID objects on an RCU
                         bucket; gcloud cannot.
    warm PATH...         Read the first MiB of every object under each PATH.
                         An admit-on-first-miss cache then ingests the whole
                         object, as long as the read comes from the cache zone.

scripts/rcu_stage.sh and scripts/rcu_prepare_reads.sh run this in a Job on the
workload node pool, which is in the cache zone. Keep it self-contained (stdlib
and gcsfs): the gcsfs wheel does not ship gcsfs.tests.
"""

import argparse
import shutil
from concurrent.futures import ThreadPoolExecutor

import gcsfs

MiB = 1 << 20
THREADS = 32


def _sizes(fs, path):
    """Size of every object at or under path, keyed by its bucket/key name."""
    return {name: info["size"] for name, info in fs.find(path, detail=True).items()}


def stage_rapid(fs, src, dst):
    src_root = src.removeprefix("gs://").rstrip("/")
    dst_root = dst.rstrip("/")

    def copy(name):
        target = f"{dst_root}/{name[len(src_root) + 1 :]}"
        with fs.open(name, "rb") as f, fs.open(target, "wb") as g:
            shutil.copyfileobj(f, g, 16 * MiB)

    names = list(_sizes(fs, src))
    with ThreadPoolExecutor(THREADS) as pool:
        list(pool.map(copy, names))
    print(f"Staged {len(names)} objects from {src} to {dst}.")


def warm(fs, paths):
    sizes = {}
    for path in paths:
        sizes.update(_sizes(fs, path))

    def read(name):
        if sizes[name]:
            fs.cat_file(name, start=0, end=min(sizes[name], MiB))

    with ThreadPoolExecutor(THREADS) as pool:
        list(pool.map(read, sizes))
    print(f"Read the first MiB of {len(sizes)} objects ({sum(sizes.values())} bytes).")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    stage = commands.add_parser("stage-rapid")
    stage.add_argument("src")
    stage.add_argument("dst")
    commands.add_parser("warm").add_argument("paths", nargs="+")
    args = parser.parse_args(argv)

    fs = gcsfs.GCSFileSystem()
    if args.command == "stage-rapid":
        stage_rapid(fs, args.src, args.dst)
    else:
        warm(fs, args.paths)


if __name__ == "__main__":
    main()
