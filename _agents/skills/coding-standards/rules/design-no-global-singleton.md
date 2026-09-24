---
title: Avoid Global Singletons and Module-Level Shared Instances
impact: HIGH
impactDescription: Testable, swappable dependencies without hidden shared state
tags: [design, singleton, global-state, dependency-injection, testing]
---

# Avoid Global Singletons and Module-Level Shared Instances [HIGH]

## Description
Do not expose services, clients, or mutable configuration as module-level singletons that any code can import and use. Generative models often default to lazy `get_*()` globals or `__new__`-based singletons "for convenience," but these hide coupling, make tests order-dependent, and block alternate implementations (fakes, multi-tenant configs, multiple connections).

Prefer constructing dependencies once at a composition root (`main`, app factory, lifespan) and passing them in via constructors or parameters. Related: [design-dependency-injection](design-dependency-injection.md).

## Bad Example
```python
# Module-level shared instance: hard to test and reconfigure
_db: Database | None = None

def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database(os.environ["DATABASE_URL"])
    return _db


class OrderService:
    def place(self, order: Order) -> None:
        # Hidden dependency: callers cannot inject a fake
        get_db().save(order)


# Classic singleton metaclass / __new__ pattern
class Config:
    _instance: "Config | None" = None

    def __new__(cls) -> "Config":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.api_key = os.environ["API_KEY"]
        return cls._instance


# Eager module global used as a service locator
email_client = SmtpEmailClient(os.environ["SMTP_HOST"])
```

## Good Example
```python
from dataclasses import dataclass
from typing import Protocol


class Database(Protocol):
    def save(self, order: Order) -> None: ...


class EmailClient(Protocol):
    def send(self, to: str, body: str) -> None: ...


@dataclass
class Settings:
    database_url: str
    smtp_host: str


class OrderService:
    def __init__(self, db: Database, email: EmailClient) -> None:
        self._db = db
        self._email = email

    def place(self, order: Order) -> None:
        self._db.save(order)
        self._email.send(order.customer_email, "Order confirmed")


# Composition root: build the graph once at the process boundary
def create_app(settings: Settings) -> OrderService:
    db = PostgresDatabase(settings.database_url)
    email = SmtpEmailClient(settings.smtp_host)
    return OrderService(db=db, email=email)


def main() -> None:
    settings = Settings(
        database_url=os.environ["DATABASE_URL"],
        smtp_host=os.environ["SMTP_HOST"],
    )
    service = create_app(settings)
    service.place(load_order())


# Tests inject fakes without patching module globals
def test_place_order_saves_and_notifies() -> None:
    db = FakeDatabase()
    email = FakeEmailClient()
    service = OrderService(db=db, email=email)

    service.place(sample_order)

    assert db.saved == [sample_order]
    assert email.sent == [(sample_order.customer_email, "Order confirmed")]
```

## Notes
- Bad smells: module-level `client = ...`, lazy `get_x()` with `global`, Borg/`__new__` singletons, service locators that reach into other modules' state
- Acceptable when scoped to the process boundary and not imported as a service API:
  - Read-only settings loaded once and passed in (or a thin loader called only from `main`)
  - Logging configuration applied once at startup
  - Framework wiring that registers a single app instance you still inject into handlers
- If a resource must be shared (DB pool, HTTP session), create it in the composition root or lifespan and inject the shared object—do not re-fetch it through a global accessor inside business logic
- Prefer explicit teardown (`close()`, context managers, async lifespan) over immortal process-wide objects that tests cannot reset
- Monkeypatching module globals in tests is a signal the design should use injection instead

## References
- [Dependency Injection](https://en.wikipedia.org/wiki/Dependency_injection)
- [Singleton considered harmful (patterns of coupling)](https://www.yegor256.com/2016/06/27/singletons.html)
- [design-dependency-injection](design-dependency-injection.md)
