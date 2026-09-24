---
title: Open/Closed Principle
impact: HIGH
impactDescription: Extend behavior without modifying existing code
tags: [design, solid, ocp, extensibility]
---

# Open/Closed Principle [HIGH]

## Description
Software entities should be open for extension but closed for modification. Add new behavior by introducing new types or strategies, not by editing and retesting stable code paths. In Python, `Protocol`-based abstractions and composition make this practical without heavyweight inheritance hierarchies.

## Bad Example
```python
class InvoiceExporter:
    def export(self, invoice: Invoice, format: str) -> str:
        # Every new format requires editing this method
        if format == "json":
            return json.dumps(invoice.to_dict())
        if format == "csv":
            return ",".join(invoice.to_dict().values())
        if format == "xml":
            return f"<invoice>{invoice.id}</invoice>"
        raise ValueError(f"Unsupported format: {format}")
```

## Good Example
```python
from typing import Protocol

class InvoiceFormat(Protocol):
    def export(self, invoice: Invoice) -> str: ...

class JsonInvoiceFormat:
    def export(self, invoice: Invoice) -> str:
        return json.dumps(invoice.to_dict())

class CsvInvoiceFormat:
    def export(self, invoice: Invoice) -> str:
        return ",".join(invoice.to_dict().values())

class InvoiceExporter:
    def __init__(self, formatter: InvoiceFormat) -> None:
        self.formatter = formatter

    def export(self, invoice: Invoice) -> str:
        return self.formatter.export(invoice)

# Add XML support by creating a new class—InvoiceExporter stays unchanged
class XmlInvoiceFormat:
    def export(self, invoice: Invoice) -> str:
        return f"<invoice>{invoice.id}</invoice>"
```

## Notes
- OCP pairs naturally with dependency injection: inject the extension point instead of branching inside a method
- Prefer small `Protocol` interfaces over deep inheritance trees
- A growing `if/elif` chain or `match` on type names is a common OCP violation signal
- Not every change warrants abstraction—apply OCP when the code path is stable and new variants are expected

## References
- [SOLID - Open/Closed Principle](https://en.wikipedia.org/wiki/Open%E2%80%93closed_principle)
- [Clean Architecture by Robert C. Martin](https://www.oreilly.com/library/view/clean-architecture-a/9780134494272/)