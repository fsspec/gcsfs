---
title: Single Responsibility Principle
impact: HIGH
impactDescription: Improved maintainability and testability
tags: [design, solid, srp, maintainability]
---

# Single Responsibility Principle [HIGH]

## Description
A class or function should have only one reason to change. When a unit of code handles multiple responsibilities, changes to one responsibility may inadvertently affect others, making the code harder to maintain and test.

## Bad Example
```python
class UserManager:
    def __init__(self, db_connection) -> None:
        self.db = db_connection

    def create_user(self, email: str, password: str) -> User:
        # Responsibility 1: Validation
        if not self._validate_email(email):
            raise ValueError("Invalid email")

        # Responsibility 2: Password hashing
        # (Note: SHA-256 is NOT suitable for passwords—see Good Example.)
        hashed = hashlib.sha256(password.encode()).hexdigest()

        # Responsibility 3: Database operations
        self.db.execute("INSERT INTO users ...", (email, hashed))

        # Responsibility 4: Email notification
        self._send_welcome_email(email)

        return User(email=email)

    def _validate_email(self, email: str) -> bool: ...
    def _send_welcome_email(self, email: str) -> None: ...
```

## Good Example
```python
from dataclasses import dataclass

# Stateless collaborators: plain classes (no fields, so no @dataclass needed)
class UserValidator:
    def validate_email(self, email: str) -> bool:
        return "@" in email and "." in email.split("@")[1]

class PasswordHasher:
    def hash(self, password: str) -> str:
        # Use a slow, salted password hash—NEVER a plain digest like SHA-256/MD5.
        # e.g. argon2-cffi or bcrypt:
        #   from argon2 import PasswordHasher as Argon2
        #   return Argon2().hash(password)
        ...

# Collaborators that carry dependencies: @dataclass removes __init__ boilerplate
@dataclass
class UserRepository:
    db: Connection

    def save(self, user: User) -> None:
        self.db.execute("INSERT INTO users ...", (user.email, user.password_hash))

@dataclass
class EmailService:
    smtp_host: str

    def send_welcome(self, email: str) -> None: ...

@dataclass
class UserService:
    validator: UserValidator
    hasher: PasswordHasher
    repository: UserRepository
    email_service: EmailService

    def create_user(self, email: str, password: str) -> User:
        if not self.validator.validate_email(email):
            raise ValueError("Invalid email")

        user = User(email=email, password_hash=self.hasher.hash(password))
        self.repository.save(user)
        self.email_service.send_welcome(email)
        return user
```

## Notes
- Signs of SRP violation: "and" in class descriptions, multiple unrelated imports, large classes
- Each component can now be tested in isolation
- Changes to email logic won't affect password hashing
- Enables easier dependency injection and mocking
- Use `@dataclass` only for classes that hold fields; stateless collaborators are clearer as plain classes
- Hash passwords with a dedicated slow algorithm (argon2, bcrypt, scrypt), never a bare `hashlib` digest—the example above flags this

## References
- [Clean Code by Robert C. Martin](https://www.oreilly.com/library/view/clean-code-a/9780136083238/)
