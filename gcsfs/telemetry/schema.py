"""Pure protobuf wire encoding for gcsfs telemetry feature headers."""

from __future__ import annotations

import base64
import functools
from collections.abc import Mapping

import proto

from gcsfs.telemetry.context import Dimension
from gcsfs.telemetry.features_proto import (
    GcsfsEnvironment,
    GcsfsFeatures,
    GcsfsFramework,
    GcsfsMethod,
)


def _build_enum_map(enum_cls: type[proto.Enum], prefix: str) -> dict[str, int]:
    return {
        member.name.removeprefix(prefix).lower(): int(member)
        for member in enum_cls
        if not member.name.endswith("_UNSPECIFIED")
    }


_METHOD_MAP = _build_enum_map(GcsfsMethod, "GCSFS_METHOD_")
_FRAMEWORK_MAP = _build_enum_map(GcsfsFramework, "GCSFS_FRAMEWORK_")
_ENVIRONMENT_MAP = {
    **_build_enum_map(GcsfsEnvironment, "GCSFS_ENVIRONMENT_"),
    "oth": int(GcsfsEnvironment.GCSFS_ENVIRONMENT_OTHER),
}
_METHOD_PARAM_FIELDS = frozenset(
    name
    for name, field in GcsfsFeatures.MethodParams.meta.fields.items()
    if field.proto_type == proto.BOOL
)


def _norm_token(value: object) -> str:
    return value.strip().lower() if isinstance(value, str) else ""


@functools.lru_cache(maxsize=256)
def _serialize_features(
    method_id: int | None,
    framework_id: int | None,
    env_id: int | None,
    method_params: frozenset[str],
) -> str | None:
    features = GcsfsFeatures()
    if method_id is not None:
        features.method = method_id
    if framework_id is not None:
        features.framework = framework_id
    if env_id is not None:
        features.environment = env_id
    if method_params:
        # Omit False flags so optional proto3 bool fields are only serialized when True.
        params_msg = GcsfsFeatures.MethodParams()
        for field_name in method_params:
            setattr(params_msg, field_name, True)
        features.method_params = params_msg
    serialized = GcsfsFeatures.serialize(features)
    if not serialized:
        return None
    return base64.urlsafe_b64encode(serialized).decode("ascii")


def encode_features(tokens_map: Mapping[str, str]) -> str | None:
    """Encode active telemetry dimensions into a web-safe base64 GcsfsFeatures string."""
    if not tokens_map:
        return None

    method_id = _METHOD_MAP.get(_norm_token(tokens_map.get(Dimension.METHOD.value)))
    framework_id = _FRAMEWORK_MAP.get(
        _norm_token(tokens_map.get(Dimension.FRAMEWORK.value))
    )
    env_id = _ENVIRONMENT_MAP.get(
        _norm_token(tokens_map.get(Dimension.ENVIRONMENT.value))
    )

    params_raw = tokens_map.get(Dimension.METHOD_PARAMS.value)
    method_params = (
        frozenset(
            part.strip().lower()
            for chunk in params_raw.split(",")
            for part in chunk.split(".")
            if part.strip()
        )
        & _METHOD_PARAM_FIELDS
        if isinstance(params_raw, str) and params_raw
        else frozenset()
    )

    # Short-circuit before the cache so unrecognized or empty tokens never fill slots.
    if (
        method_id is None
        and framework_id is None
        and env_id is None
        and not method_params
    ):
        return None

    return _serialize_features(method_id, framework_id, env_id, method_params)
