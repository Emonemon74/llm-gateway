from app.core.security import KEY_PREFIX, generate_key, hash_key


def test_generate_key_shape_and_hash():
    plaintext, prefix, digest = generate_key()
    assert plaintext.startswith(KEY_PREFIX) and plaintext.startswith(prefix)
    assert digest == hash_key(plaintext) and len(digest) == 64


def test_keys_are_unique():
    assert len({generate_key()[0] for _ in range(100)}) == 100
