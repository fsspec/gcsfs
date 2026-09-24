---
title: Document Public APIs with Google Style Docstrings
impact: HIGH
impactDescription: Clear API contracts for humans, agents, and generated documentation
tags: [documentation, docstring, public-api, google-style]
---

# Document Public APIs with Google Style Docstrings [HIGH]

## Description
Public modules, classes, functions, methods, and exceptions should include concise Google style docstrings. A docstring is part of the API contract: it explains intent, parameters, return values, raised exceptions, and important side effects that are not obvious from the signature.

## Bad Example
```python
def charge(customer, amount, currency, capture=True):
    result = gateway.create_charge(customer.id, amount, currency, capture)
    audit_log.write("charge_created", result.id)
    return result


class InvoiceService:
    def void(self, invoice_id):
        invoice = self.repository.get(invoice_id)
        invoice.status = "void"
        self.repository.save(invoice)
```

## Good Example
```python
from decimal import Decimal


def charge(
    customer: Customer,
    amount: Decimal,
    currency: str,
    capture: bool = True,
) -> Charge:
    """Create a payment charge for a customer.

    Args:
        customer: Customer to charge.
        amount: Positive amount in the smallest supported unit for the currency.
        currency: ISO 4217 currency code, such as "USD" or "JPY".
        capture: Whether to capture funds immediately.

    Returns:
        The charge created by the payment gateway.

    Raises:
        PaymentDeclinedError: If the gateway rejects the charge.
        PaymentGatewayError: If the gateway request fails.
    """
    result = gateway.create_charge(customer.id, amount, currency, capture)
    audit_log.write("charge_created", result.id)
    return result


class InvoiceService:
    """Coordinates invoice lifecycle operations."""

    def void(self, invoice_id: InvoiceId) -> Invoice:
        """Mark an invoice as void.

        Args:
            invoice_id: Identifier of the invoice to void.

        Returns:
            The updated invoice.

        Raises:
            InvoiceNotFoundError: If the invoice does not exist.
            InvalidInvoiceStateError: If the invoice cannot be voided.
        """
        invoice = self.repository.get(invoice_id)
        invoice.status = "void"
        self.repository.save(invoice)
        return invoice
```

## Notes
- Focus on public API behavior, invariants, side effects, and failure modes.
- Do not restate every type annotation in prose; explain meaning and constraints.
- Private helpers can omit docstrings when their purpose is obvious from the name and local context.
- Keep docstrings current when behavior, parameters, or exceptions change.

## References
- [Python Docs - Documentation Strings](https://docs.python.org/3/tutorial/controlflow.html#documentation-strings)
- [Google Python Style Guide - Comments and Docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
