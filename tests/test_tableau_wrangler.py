from datetime import datetime

from tableau_wrangler import cli
from click.testing import CliRunner

from .conftest import FAKE_WORKBOOK_IDS


def test_tableau_workbook_server_initialise(tableau_workbook_server):
    """Sample pytest test function with the pytest fixture as an argument."""
    tws = tableau_workbook_server
    tws.initialise(
        checkpoint=datetime.now(), limit=5
    )
    assert tableau_workbook_server.all_workbook_ids == FAKE_WORKBOOK_IDS


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert 'tableau_wrangler.cli.main' in result.output
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0
    assert '--help  Show this message and exit.' in help_result.output
