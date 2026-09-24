---
title: Loose Coupling with Dependency Injection
impact: HIGH
impactDescription: Improved testability and flexibility
tags: [design, dependency-injection, testing, decoupling]
---

# Loose Coupling with Dependency Injection [HIGH]

## Description
Instead of creating dependencies inside a class, inject them from outside. This decouples components, makes testing easier with mocks, and allows swapping implementations without changing the dependent code.

## Bad Example
```python
class OrderProcessor:
    def __init__(self) -> None:
        # Hard-coded dependencies: tight coupling
        self.db = PostgresDatabase("connection_string")
        self.email = SmtpEmailService("smtp.server.com")
        self.payment = StripePaymentGateway("api_key")

    def process(self, order: Order) -> None:
        self.payment.charge(order.total)
        self.db.save(order)
        self.email.send_confirmation(order.customer_email)
```

## Good Example
```python
from typing import Protocol

class Database(Protocol):
    def save(self, order: Order) -> None: ...

class EmailService(Protocol):
    def send_confirmation(self, email: str) -> None: ...

class PaymentGateway(Protocol):
    def charge(self, amount: Decimal) -> None: ...

class OrderProcessor:
    def __init__(
        self,
        db: Database,
        email: EmailService,
        payment: PaymentGateway,
    ) -> None:
        self.db = db
        self.email = email
        self.payment = payment

    def process(self, order: Order) -> None:
        self.payment.charge(order.total)
        self.db.save(order)
        self.email.send_confirmation(order.customer_email)

# Usage
processor = OrderProcessor(
    db=PostgresDatabase("connection_string"),
    email=SmtpEmailService("smtp.server.com"),
    payment=StripePaymentGateway("api_key"),
)

# Testing with mocks
def test_order_processing() -> None:
    mock_db = Mock(spec=Database)
    mock_email = Mock(spec=EmailService)
    mock_payment = Mock(spec=PaymentGateway)

    processor = OrderProcessor(mock_db, mock_email, mock_payment)
    processor.process(test_order)

    mock_payment.charge.assert_called_once_with(test_order.total)
```

## Notes
- Use `Protocol` to define interfaces without forcing inheritance
- Constructor injection is the most common and recommended approach
- Consider using DI containers (e.g., `dependency-injector`) for complex applications
- Factory functions can also serve as simple dependency injection
- Do not replace constructor injection with module-level singletons or lazy `get_*()` globals; see [design-no-global-singleton](design-no-global-singleton.md)

## References
- [Dependency Injection Principles](https://en.wikipedia.org/wiki/Dependency_injection)
- [design-no-global-singleton](design-no-global-singleton.md)
