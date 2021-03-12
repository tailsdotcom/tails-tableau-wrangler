from .client import BaseTableauServerClient
from .local_workbook_extractor import LocalWorkbookExtractor


class TableauWorkbookService:
    """ Abstraction on top of Tableau Server Client and Workbook files, to
    make it simple to extract metadata embedded in Workbooks.
    """

    def __init__(self, client: BaseTableauServerClient):
        self.client = client
        # Storage Attrs
        self.all_workbook_ids = []
        self.workbooks = []
        self.datasources = []
        self.connections = []
        self.relations = []
        self.table_references = []

    def initialise(self, checkpoint=None, limit=None, relation_types_exclude=[]):
        """ **Warning:** this is an expensive operation. Initialising
        a TableauWorkbookService does the work to download Workbooks and
        extract child entities into an in-memory data structure. The
        `checkpoint` and `limit` args are used to limit the number of Workbooks
        fetched at any given time.
        """
        self.all_workbook_ids = self.client.list_all_workbook_ids()
        # Get filtered list of workbook ids
        filtered_workbook_ids = self.client.list_workbook_ids(
            checkpoint=checkpoint, limit=limit
        )
        lwx = LocalWorkbookExtractor(
            relation_types_exclude=relation_types_exclude
        )
        for lwb in self.client.iterate_server_workbooks(filtered_workbook_ids):
            extraction_result = lwx.extract(lwb)
            # Store Extraction Results
            self.workbooks.extend(extraction_result.workbooks)
            self.datasources.extend(extraction_result.datasources)
            self.connections.extend(extraction_result.connections)
            self.relations.extend(extraction_result.relations)
            self.table_references.extend(extraction_result.table_references)
