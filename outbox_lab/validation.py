"""Bounded validation and deterministic JSON serialization."""

from __future__ import annotations

import json
import math
import re
import unicodedata
from collections.abc import Mapping
from itertools import islice
from typing import Any

from outbox_lab.errors import OutboxValidationError

INT64_MAX = (1 << 63) - 1
MAX_PAYLOAD_BYTES = 16_384
MAX_JSON_DEPTH = 16
MAX_CONTAINER_ITEMS = 256
MAX_STRING_CHARS = 4_096
MAX_ATTEMPTS_LIMIT = 100
MAX_BATCH_SIZE = 500
MAX_LEASE_DURATION_MS = 86_400_000 * 365
MAX_AUDIT_ROWS_LIMIT = 10_000
MAX_ERROR_REASON_CHARS = 160

_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9_.:/-]{1,128}")
_ERROR_CODE_RE = re.compile(r"[a-z0-9_.-]{1,64}")


def is_strict_int(value: Any) -> bool:
    return type(value) is int


def validate_timestamp_ms(value: Any, field_name: str = "now_ms") -> int:
    return validate_positive_int(value, field_name, minimum=0)


def validate_positive_int(
    value: Any,
    field_name: str,
    *,
    minimum: int = 1,
    maximum: int = INT64_MAX,
) -> int:
    if not is_strict_int(value) or not minimum <= value <= maximum:
        raise OutboxValidationError(
            f"{field_name} must be an integer in {minimum}..{maximum}"
        )
    return value


def validate_identifier(value: Any, field_name: str, *, max_len: int = 128) -> str:
    if (
        type(value) is not str
        or len(value) > max_len
        or _IDENTIFIER_RE.fullmatch(value) is None
    ):
        raise OutboxValidationError(
            f"{field_name} must be a nonempty ASCII identifier of at most {max_len} characters"
        )
    return value


def validate_error_code(value: Any, field_name: str = "error_code") -> str:
    if type(value) is not str or _ERROR_CODE_RE.fullmatch(value) is None:
        raise OutboxValidationError(
            f"{field_name} must contain 1..64 lowercase ASCII letters, digits, dots, underscores or hyphens"
        )
    return value


def sanitize_error_reason(value: Any, field_name: str = "error_reason") -> str:
    if type(value) is not str or len(value) > 1024:
        raise OutboxValidationError(f"{field_name} must be a string of at most 1024 input characters")
    cleaned = " ".join(value.split())
    if not cleaned or len(cleaned) > MAX_ERROR_REASON_CHARS:
        raise OutboxValidationError(f"{field_name} must normalize to 1..160 characters")
    if any(unicodedata.category(ch) in {"Cc", "Cf", "Cs"} for ch in cleaned):
        raise OutboxValidationError(f"{field_name} contains forbidden control or surrogate characters")
    return cleaned


def freeze_canonical_json(mapping: Any, *, field_name: str = "payload") -> str:
    """Serialize a mapping while enforcing the byte budget during traversal.

    This is a Python JSON representation, not a cross-language canonicalization
    standard. Shared subtrees are permitted; cycles are not.
    """
    if not isinstance(mapping, Mapping):
        raise OutboxValidationError(f"{field_name} must be a mapping")
    parts: list[str] = []
    remaining = MAX_PAYLOAD_BYTES
    active: set[int] = set()

    def emit(fragment: str) -> None:
        nonlocal remaining
        try:
            size = len(fragment.encode("utf-8"))
        except UnicodeEncodeError:
            raise OutboxValidationError(f"{field_name} contains an unpaired surrogate") from None
        remaining -= size
        if remaining < 0:
            raise OutboxValidationError(f"{field_name} exceeds {MAX_PAYLOAD_BYTES} UTF-8 bytes")
        parts.append(fragment)

    def string(value: str, maximum: int) -> None:
        if len(value) > maximum:
            raise OutboxValidationError(f"{field_name} contains an oversized string")
        emit(json.dumps(value, ensure_ascii=False, allow_nan=False))

    def visit(node: Any, depth: int) -> None:
        if depth > MAX_JSON_DEPTH:
            raise OutboxValidationError(f"{field_name} exceeds depth {MAX_JSON_DEPTH}")
        if node is None:
            emit("null")
        elif type(node) is bool:
            emit("true" if node else "false")
        elif type(node) is int:
            if not -(1 << 63) <= node <= INT64_MAX:
                raise OutboxValidationError(f"{field_name} contains an integer outside signed 64-bit bounds")
            emit(str(node))
        elif type(node) is float:
            if not math.isfinite(node):
                raise OutboxValidationError(f"{field_name} contains a non-finite float")
            emit(json.dumps(node, allow_nan=False))
        elif type(node) is str:
            string(node, MAX_STRING_CHARS)
        elif isinstance(node, (Mapping, list, tuple)):
            identity = id(node)
            if identity in active:
                raise OutboxValidationError(f"{field_name} contains a cycle")
            if len(node) > MAX_CONTAINER_ITEMS:
                raise OutboxValidationError(f"{field_name} exceeds the container item limit")
            active.add(identity)
            try:
                if isinstance(node, Mapping):
                    items = list(islice(node.items(), MAX_CONTAINER_ITEMS + 1))
                    if len(items) > MAX_CONTAINER_ITEMS:
                        raise OutboxValidationError(f"{field_name} exceeds the object item limit")
                    for key, _ in items:
                        if type(key) is not str or not key or len(key) > 128:
                            raise OutboxValidationError(f"{field_name} requires nonempty string keys of at most 128 characters")
                    emit("{")
                    for index, (key, value) in enumerate(sorted(items, key=lambda item: item[0])):
                        if index:
                            emit(",")
                        string(key, 128)
                        emit(":")
                        visit(value, depth + 1)
                    emit("}")
                else:
                    emit("[")
                    for index, value in enumerate(islice(node, MAX_CONTAINER_ITEMS + 1)):
                        if index == MAX_CONTAINER_ITEMS:
                            raise OutboxValidationError(f"{field_name} exceeds the array item limit")
                        if index:
                            emit(",")
                        visit(value, depth + 1)
                    emit("]")
            finally:
                active.remove(identity)
        else:
            raise OutboxValidationError(f"{field_name} contains an unsupported value type")

    visit(mapping, 1)
    return "".join(parts)


def compute_retry_backoff_ms(
    attempt_number: int,
    *,
    base_backoff_ms: int = 1_000,
    max_backoff_ms: int = 60_000,
) -> int:
    validate_positive_int(attempt_number, "attempt_number", maximum=MAX_ATTEMPTS_LIMIT)
    validate_positive_int(base_backoff_ms, "base_backoff_ms", minimum=0, maximum=MAX_LEASE_DURATION_MS)
    validate_positive_int(max_backoff_ms, "max_backoff_ms", minimum=0, maximum=MAX_LEASE_DURATION_MS)
    if max_backoff_ms < base_backoff_ms:
        raise OutboxValidationError("max_backoff_ms must be >= base_backoff_ms")
    return min(max_backoff_ms, base_backoff_ms * (1 << (attempt_number - 1)))
