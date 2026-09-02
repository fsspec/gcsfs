"""Cloud Logging helpers shared by macrobenchmark metric parsers."""

from dataclasses import dataclass
from typing import Iterable


@dataclass
class LogEntry:
    timestamp: float  # epoch seconds
    message: str


_MAX_PAGE_SIZE = 1000


def _default_read_retry():
    from google.api_core import exceptions as gexc
    from google.api_core import retry as retries

    return retries.Retry(
        predicate=retries.if_exception_type(gexc.ResourceExhausted),
        initial=2.0,
        maximum=60.0,
        multiplier=2.0,
        deadline=180.0,
    )


def _to_log_entry(entry) -> LogEntry:
    payload = entry.text_payload or (
        dict(entry.json_payload) if entry.json_payload else ""
    )
    message = (
        payload
        if isinstance(payload, str)
        else (payload.get("message", "") if payload else "")
    )
    return LogEntry(timestamp=entry.timestamp.timestamp(), message=message)


def iter_log_entries(
    client,
    project: str,
    filter_string: str,
    *,
    page_size: int = _MAX_PAGE_SIZE,
    retry=None,
) -> Iterable[LogEntry]:
    if retry is None:
        retry = _default_read_retry()

    def _fetch(token):
        request = {
            "resource_names": [f"projects/{project}"],
            "filter": filter_string,
            "order_by": "timestamp asc",
            "page_size": page_size,
            "page_token": token,
        }
        pager = client.list_log_entries(request=request)
        page = next(pager.pages, None)
        if page is None:
            return None, None
        return page.entries, page.next_page_token

    page_token = None
    while True:
        page, page_token = retry(_fetch)(page_token)
        if page is None:
            return
        for entry in page:
            yield _to_log_entry(entry)
        if not page_token:
            return
