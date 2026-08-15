"""Webhook signature verification (Part B).

The mock API signs every webhook with HMAC-SHA256 of the *raw request body*,
using the API key as the secret, and sends the result in the header:

    X-PseudoGram-Signature: sha256=<hex>

We recompute the digest over the exact bytes we received and compare in constant
time, so a forged or tampered request is rejected before any work is done.
"""

import hashlib
import hmac

_PREFIX = "sha256="


def verify(body: bytes, header: str | None, secret: str) -> bool:
    """Return True only if `header` is a valid signature of `body` for `secret`."""
    if not header or not secret:
        return False

    # Accept any casing of the prefix, then normalize the hex for comparison.
    lowered = header.strip().lower()
    if not lowered.startswith(_PREFIX):
        return False

    received_hex = lowered[len(_PREFIX):]
    expected_hex = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

    # hmac.compare_digest is constant-time: it does not leak how many bytes match.
    return hmac.compare_digest(received_hex, expected_hex)
