import pytest
from tableau_wrangler import TableauWorkbookServer, BaseTableauServerClient


FAKE_WORKBOOK_IDS = [
    'a', 'bunch', 'of', 'fake', 'ids'
]


class FakeTableauServerClient(BaseTableauServerClient):

    def list_all_workbook_ids(self):
        return FAKE_WORKBOOK_IDS


@pytest.fixture
def tableau_server_client():
    return FakeTableauServerClient()


@pytest.fixture
def tableau_workbook_server(tableau_server_client):
    """An example TablueaWorkbookServer
    """
    return TableauWorkbookServer(client=tableau_server_client)
