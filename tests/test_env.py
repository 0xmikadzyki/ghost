from rhflow import env


def test_dotenv_and_redact(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text('ENVIO_TOKEN="secret-token-123"\n# c\nX=1\n')
    monkeypatch.delenv("ENVIO_TOKEN", raising=False)
    monkeypatch.setattr(env, "_loaded", False)
    env.load_dotenv(tmp_path / ".env")
    assert env.env("ENVIO_TOKEN") == "secret-token-123"
    assert env.redact("Bearer secret-token-123 failed") == "Bearer *** failed"
