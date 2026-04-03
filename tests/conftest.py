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

airflow_stub = types.ModuleType("airflow")
airflow_sdk_stub = types.ModuleType("airflow.sdk")


def _dag_stub(*args, **kwargs):
    def decorator(fn):
        def _factory(*factory_args, **factory_kwargs):
            return {
                "dag_id": fn.__name__,
                "args": factory_args,
                "kwargs": factory_kwargs,
            }

        _factory.__wrapped__ = fn
        return _factory

    return decorator


class _TaskStub:
    def __call__(self, fn=None, **kwargs):
        if fn is None:
            return lambda inner: inner
        return fn

    def branch(self, fn=None, **kwargs):
        if fn is None:
            return lambda inner: inner
        return fn


airflow_sdk_stub.dag = _dag_stub
airflow_sdk_stub.task = _TaskStub()

sys.modules.setdefault("psycopg", psycopg_stub)
sys.modules.setdefault("psycopg.rows", psycopg_rows_stub)
sys.modules.setdefault("playwright", playwright_stub)
sys.modules.setdefault("playwright.async_api", playwright_async_api_stub)
sys.modules.setdefault("airflow", airflow_stub)
sys.modules.setdefault("airflow.sdk", airflow_sdk_stub)
