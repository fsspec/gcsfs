---
title: Place Fixtures in the Nearest conftest.py
impact: HIGH
impactDescription: Keeps fixture origins discoverable as suites grow
tags: [fixtures, conftest, organization]
---

# Place Fixtures in the Nearest conftest.py [HIGH]

## Description
Define a fixture at the narrowest level where all its users live: in the test file itself if only that file uses it, in the nearest `conftest.py` if a directory shares it, and in the root `conftest.py` only for genuinely suite-wide resources. Dumping everything into the root conftest creates a grab-bag where nobody can tell which fixtures are safe to change, and every test pays the cognitive cost of hundreds of irrelevant fixtures.

## Bad Example
```python
# tests/conftest.py — 800-line root conftest used by everything
@pytest.fixture
def stripe_webhook_payload() -> dict:  # only used by tests/api/test_webhooks.py
    ...

@pytest.fixture
def parsed_invoice_pdf() -> Invoice:   # only used by tests/pdf/test_parser.py
    ...

@pytest.fixture
def admin_user() -> User: ...
# ... 40 more fixtures with unknown consumers
```

## Good Example
```
tests/
├── conftest.py            # suite-wide only: db_session, app config
├── api/
│   ├── conftest.py        # api-wide: client, auth_headers
│   └── test_webhooks.py   # stripe_webhook_payload defined here, its only user
└── pdf/
    ├── conftest.py        # pdf-wide: sample_pdf_dir
    └── test_parser.py
```

```python
# tests/api/conftest.py — shared by API tests only
@pytest.fixture
def client(db_session: Session) -> TestClient:
    return TestClient(create_app(db_session))


# tests/api/test_webhooks.py — single-file fixture stays in the file
@pytest.fixture
def stripe_webhook_payload() -> dict:
    return {"type": "invoice.paid", "data": {...}}
```

## Notes
- conftest.py fixtures are auto-discovered by every test at or below that directory — no imports needed or allowed
- Promote a fixture upward only when a second location actually needs it (YAGNI applies to fixtures)
- A lower conftest can override a same-named fixture from a higher one; use sparingly — it surprises readers
- `pytest --fixtures tests/api/` lists every fixture visible to a directory, with origin file

## References
- [pytest - conftest.py: sharing fixtures across multiple files](https://docs.pytest.org/en/stable/how-to/fixtures.html#scope-sharing-fixtures-across-classes-modules-packages-or-session)
