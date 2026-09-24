---
title: Use Pydantic for Boundary Data Validation
impact: HIGH
impactDescription: Catch invalid input at trust boundaries before it reaches business logic
tags: [validation, pydantic, input-validation, api]
---

# Use Pydantic for Boundary Data Validation [HIGH]

## Description
Data crossing trust boundaries—HTTP requests, CLI arguments, environment variables, config files, and external API payloads—must be validated and coerced before use. Pydantic models centralize that logic with declarative constraints, clear error messages, and type coercion. Use `@dataclass` for internal domain objects; use Pydantic where untrusted input enters the system.

## Bad Example
```python
def create_user(payload: dict) -> User:
    # Validation scattered, easy to forget fields, no coercion
    if "email" not in payload:
        raise ValueError("email required")
    if "@" not in payload["email"]:
        raise ValueError("invalid email")
    if "age" not in payload:
        raise ValueError("age required")
    age = payload["age"]
    if not isinstance(age, int) or age < 0:
        raise ValueError("invalid age")
    return user_repository.create(payload["email"], age)


@app.post("/users")
def create_user_endpoint(body: dict) -> dict:
    user = create_user(body)  # dict passes through unchecked
    return {"id": user.id}
```

## Good Example
```python
from pydantic import BaseModel, EmailStr, Field, field_validator


class CreateUserRequest(BaseModel):
    email: EmailStr
    age: int = Field(ge=0, le=150)
    name: str = Field(min_length=1, max_length=100)

    @field_validator("name", mode="before")
    @classmethod
    def strip_name(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


@app.post("/users")
def create_user_endpoint(body: CreateUserRequest) -> dict:
    # body is already validated and coerced
    user = user_repository.create(body.email, body.age, body.name)
    return {"id": user.id}


# Config and env vars at startup
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str
    api_key: str = Field(min_length=32)
    debug: bool = False
```

## Notes
- **Dataclass vs Pydantic**: use `@dataclass` for internal data structures your code owns (domain entities, value objects, service DTOs between layers). Use Pydantic at boundaries where data is untrusted or loosely typed (JSON, form data, env vars, third-party API responses).
- Validate once at the boundary, then pass typed objects inward—do not re-validate the same fields in every service method.
- Prefer `model_validate()` / `model_validate_json()` over manual dict unpacking so coercion and constraints run consistently.
- Use `Field()` constraints (`ge`, `le`, `pattern`, `min_length`) instead of hand-written `if` checks.
- `field_validator` defaults to `mode="after"`, so `Field()` constraints run first. Use `mode="before"` when normalizing input (e.g., stripping whitespace) must happen before length or pattern checks.
- For FastAPI, declare Pydantic models as route parameters or `response_model` to get automatic validation and OpenAPI schemas.
- Keep Pydantic models thin: validation and serialization only. Move business rules to domain functions or services.

## References
- [Pydantic Documentation](https://docs.pydantic.dev/latest/)
- [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)