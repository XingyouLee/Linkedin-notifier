import sys
import types
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


psycopg_stub = types.ModuleType("psycopg")
psycopg_stub.connect = None
psycopg_stub.Connection = object

psycopg_rows_stub = types.ModuleType("psycopg.rows")
psycopg_rows_stub.dict_row = object()

playwright_stub = types.ModuleType("playwright")
playwright_async_api_stub = types.ModuleType("playwright.async_api")
playwright_async_api_stub.async_playwright = None

sys.modules.setdefault("psycopg", psycopg_stub)
sys.modules.setdefault("psycopg.rows", psycopg_rows_stub)
sys.modules.setdefault("playwright", playwright_stub)
sys.modules.setdefault("playwright.async_api", playwright_async_api_stub)
