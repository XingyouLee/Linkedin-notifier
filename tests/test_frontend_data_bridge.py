import importlib


def test_frontend_data_module_can_be_imported():
    module = importlib.import_module("dags.frontend_data")

    assert module is not None
    assert hasattr(module, "main")

