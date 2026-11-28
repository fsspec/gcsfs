"""gRPC client interceptors for gcsfs usage telemetry."""

from __future__ import annotations

import grpc.aio

from gcsfs.telemetry import manager as telemetry_manager
from gcsfs.telemetry.context import GRPC_METADATA_KEY


class ControlPlaneTelemetryInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """Attach gcsfs feature metadata to Storage Control unary RPCs."""

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        existing = (
            tuple(client_call_details.metadata)
            if client_call_details.metadata is not None
            else ()
        )
        filtered = tuple((k, v) for k, v in existing if k.lower() != GRPC_METADATA_KEY)
        had_feature_key = len(filtered) != len(existing)
        encoded = telemetry_manager.default_usage_tracker.encode_features()
        if not encoded:
            if not had_feature_key:
                return await continuation(client_call_details, request)
            updated_details = client_call_details._replace(
                metadata=grpc.aio.Metadata(*filtered)
            )
            return await continuation(updated_details, request)

        updated_details = client_call_details._replace(
            metadata=grpc.aio.Metadata(*filtered, (GRPC_METADATA_KEY, encoded))
        )
        return await continuation(updated_details, request)
