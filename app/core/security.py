import hashlib
import secrets

KEY_PREFIX = "sk-gw-"


def generate_key() -> tuple[str, str, str]:
    """Returns (plaintext, display_prefix, sha256_hex). Plaintext is shown once, never stored."""
    plaintext = KEY_PREFIX + secrets.token_urlsafe(32)
    return plaintext, plaintext[:12], hash_key(plaintext)


def hash_key(plaintext: str) -> str:
    # SHA-256 (not bcrypt): keys are 256-bit random, so a fast hash is safe and keeps
    # per-request auth cheap.
    return hashlib.sha256(plaintext.encode()).hexdigest()
