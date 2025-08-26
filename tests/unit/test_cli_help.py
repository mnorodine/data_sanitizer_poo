# tests/unit/test_cli_help.py
from typer.testing import CliRunner
from data_sanitizer.cli.__main__ import app

runner = CliRunner()

def test_cli_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "update-prices" in result.stdout
