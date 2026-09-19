import base64
import hashlib
import hmac


def test_native_scram_verifier(postgres_node_factory):
    node = postgres_node_factory("scram_verifier")
    password = "generated machine password"
    rows = node.execute(f"""
        SET scram_iterations = 8192;
        SELECT pgwrh_fdw_scram_verifier('{password}')
        FROM generate_series(1, 2)
    """)
    assert rows[0][0] != rows[1][0]
    for (verifier,) in rows:
        mechanism, parameters, keys = verifier.split("$")
        iterations, salt = parameters.split(":")
        stored_key, server_key = keys.split(":")
        assert mechanism == "SCRAM-SHA-256"
        assert int(iterations) == 8192
        salted = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), base64.b64decode(salt), int(iterations)
        )
        client = hmac.digest(salted, b"Client Key", "sha256")
        assert base64.b64decode(stored_key) == hashlib.sha256(client).digest()
        assert base64.b64decode(server_key) == hmac.digest(salted, b"Server Key", "sha256")
    assert node.execute("SELECT pgwrh_fdw_scram_verifier(NULL)") == [(None,)]
