"""Allow running gcsfs.iotracer via `python -m gcsfs.iotracer`."""

import sys
from gcsfs.cli.iotracer import main

if __name__ == "__main__":
    sys.exit(main())
