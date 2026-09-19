"""Minimal .env loader. Values are never printed or logged."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_loaded = False


def load_dotenv(path=None):
    global _loaded
    if _loaded:
        return
    p = path or (ROOT / ".env")
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    _loaded = True


def env(name, default=None):
    load_dotenv()
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def redact(text):
    """Strip the HyperSync token from anything that could reach a log or an error message."""
    tok = os.environ.get("ENVIO_TOKEN")
    return text.replace(tok, "***") if tok and len(tok) >= 8 else text
