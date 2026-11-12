# Generated file - DO NOT EDIT.
from __future__ import annotations

import proto

__protobuf__ = proto.module(
    package="gcsfs.telemetry",
    manifest={"GcsfsMethod", "GcsfsFramework", "GcsfsEnvironment", "GcsfsFeatures"},
)


class GcsfsMethod(proto.Enum):
    GCSFS_METHOD_UNSPECIFIED = 0
    GCSFS_METHOD_LS = 1
    GCSFS_METHOD_LIST_OBJECTS = 2
    GCSFS_METHOD_INFO = 3
    GCSFS_METHOD_FIND = 4
    GCSFS_METHOD_OPEN = 5
    GCSFS_METHOD_CAT = 6
    GCSFS_METHOD_CAT_FILE = 7
    GCSFS_METHOD_GET = 8
    GCSFS_METHOD_GET_FILE = 9
    GCSFS_METHOD_PUT = 10
    GCSFS_METHOD_PUT_FILE = 11
    GCSFS_METHOD_PIPE = 12
    GCSFS_METHOD_PIPE_FILE = 13
    GCSFS_METHOD_MKDIR = 14
    GCSFS_METHOD_RMDIR = 15
    GCSFS_METHOD_RM = 16
    GCSFS_METHOD_MV = 17
    GCSFS_METHOD_MOVE = 18
    GCSFS_METHOD_CP = 19
    GCSFS_METHOD_COPY = 20
    GCSFS_METHOD_TOUCH = 21
    GCSFS_METHOD_EXISTS = 22
    GCSFS_METHOD_ISDIR = 23
    GCSFS_METHOD_ISFILE = 24
    GCSFS_METHOD_CREATED = 25
    GCSFS_METHOD_MODIFIED = 26
    GCSFS_METHOD_SIGN = 27
    GCSFS_METHOD_GLOB = 28
    GCSFS_METHOD_WALK = 29
    GCSFS_METHOD_DU = 30
    GCSFS_METHOD_DISK_USAGE = 31
    GCSFS_METHOD_HEAD = 32
    GCSFS_METHOD_TAIL = 33
    GCSFS_METHOD_READ_TEXT = 34
    GCSFS_METHOD_WRITE_TEXT = 35
    GCSFS_METHOD_CAT_RANGES = 36
    GCSFS_METHOD_CP_FILE = 37
    GCSFS_METHOD_EXPAND_PATH = 38
    GCSFS_METHOD_RM_FILE = 39
    GCSFS_METHOD_OPEN_ASYNC = 40
    GCSFS_METHOD_MV_FILE = 41
    GCSFS_METHOD_PROCESS_LIMITS = 42
    GCSFS_METHOD_GET_OBJECT = 43
    GCSFS_METHOD_MERGE = 44
    GCSFS_METHOD_RM_FILES = 45
    GCSFS_METHOD_MAKEDIRS = 46
    GCSFS_METHOD_MKDIRS = 47
    GCSFS_METHOD_TREE = 48
    GCSFS_METHOD_SIZE = 49
    GCSFS_METHOD_SIZES = 50


class GcsfsFramework(proto.Enum):
    GCSFS_FRAMEWORK_UNSPECIFIED = 0
    GCSFS_FRAMEWORK_PANDAS = 1
    GCSFS_FRAMEWORK_DUCKDB = 2
    GCSFS_FRAMEWORK_SPARK = 3
    GCSFS_FRAMEWORK_DASK = 4
    GCSFS_FRAMEWORK_RAY = 5
    GCSFS_FRAMEWORK_PYARROW = 6
    GCSFS_FRAMEWORK_XARRAY = 7
    GCSFS_FRAMEWORK_FASTPARQUET = 8
    GCSFS_FRAMEWORK_POLARS = 9
    GCSFS_FRAMEWORK_TORCH = 10
    GCSFS_FRAMEWORK_TORCHDATA = 11
    GCSFS_FRAMEWORK_LIGHTNING = 12
    GCSFS_FRAMEWORK_DATASETS = 13
    GCSFS_FRAMEWORK_TRANSFORMERS = 14
    GCSFS_FRAMEWORK_ORBAX = 15
    GCSFS_FRAMEWORK_TENSORSTORE = 16
    GCSFS_FRAMEWORK_TENSORFLOW = 17
    GCSFS_FRAMEWORK_JAX = 18


class GcsfsEnvironment(proto.Enum):
    GCSFS_ENVIRONMENT_UNSPECIFIED = 0
    GCSFS_ENVIRONMENT_GCE = 1
    GCSFS_ENVIRONMENT_GKE = 2
    GCSFS_ENVIRONMENT_OTHER = 3


class GcsfsFeatures(proto.Message):
    class MethodParams(proto.Message):
        recursive = proto.Field(proto.BOOL, number=1, optional=True)
        detail = proto.Field(proto.BOOL, number=2, optional=True)
        withdirs = proto.Field(proto.BOOL, number=3, optional=True)
        prefix = proto.Field(proto.BOOL, number=4, optional=True)
        cache_options = proto.Field(proto.BOOL, number=5, optional=True)

    method = proto.Field(proto.ENUM, number=1, enum=GcsfsMethod, optional=True)
    framework = proto.Field(proto.ENUM, number=2, enum=GcsfsFramework, optional=True)
    environment = proto.Field(
        proto.ENUM, number=3, enum=GcsfsEnvironment, optional=True
    )
    method_params = proto.Field(MethodParams, number=4, optional=True)
