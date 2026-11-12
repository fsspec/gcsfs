"""gRPC client interceptors for gcsfs usage telemetry."""

from __future__ import annotations

import grpc.aio

from gcsfs.telemetry import manager as telemetry_manager
from gcsfs.telemetry.context import GRPC_METADATA_KEY


class ControlPlaneTelemetryInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """Attach gcsfs feature metadata to Storage Control unary RPCs."""

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        if not telemetry_manager.default_usage_tracker.is_enabled():
            return await continuation(client_call_details, request)

        existing = (
            tuple(client_call_details.metadata)
            if client_call_details.metadata is not None
            else ()
        )
        if any(k.lower() == GRPC_METADATA_KEY for k, _ in existing):
            return await continuation(client_call_details, request)

        encoded = telemetry_manager.default_usage_tracker.encode_features()
        if not encoded:
            return await continuation(client_call_details, request)

        updated_details = client_call_details._replace(
            metadata=grpc.aio.Metadata(*existing, (GRPC_METADATA_KEY, encoded))
        )
        return await continuation(updated_details, request)
