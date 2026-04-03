import ast
from pathlib import Path


def _find_function(tree: ast.AST, name: str) -> ast.FunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"Function {name} not found")


def test_scan_and_save_jobs_retries_transient_executor_failures():
    source_path = Path(__file__).resolve().parents[1] / "dags" / "process.py"
    tree = ast.parse(source_path.read_text())
    scan_task = _find_function(tree, "scan_and_save_jobs")

    task_decorator = next(
        (
            decorator
            for decorator in scan_task.decorator_list
            if isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Name)
            and decorator.func.id == "task"
        ),
        None,
    )

    assert task_decorator is not None

    retries_keyword = next(
        (keyword for keyword in task_decorator.keywords if keyword.arg == "retries"),
        None,
    )
    retry_delay_keyword = next(
        (
            keyword
            for keyword in task_decorator.keywords
            if keyword.arg == "retry_delay"
        ),
        None,
    )

    assert retries_keyword is not None
    assert ast.literal_eval(retries_keyword.value) >= 1
    assert retry_delay_keyword is not None
    assert isinstance(retry_delay_keyword.value, ast.Call)
    assert isinstance(retry_delay_keyword.value.func, ast.Name)
    assert retry_delay_keyword.value.func.id == "timedelta"
    assert any(keyword.arg == "minutes" for keyword in retry_delay_keyword.value.keywords)
