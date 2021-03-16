from typing import List
from datetime import datetime

from .client import BaseTableauServerClient
from .local_workbook_extractor import LocalWorkbookExtractor


class TableauWorkbookService:
    """ Abstraction on top of Tableau Server Client and Workbook files, to
    make it simple to extract metadata embedded in Workbooks.
    """

    def __init__(
        self, client: BaseTableauServerClient,
        checkpoint: str = None,
        limit: int = None,
        relation_types_exclude: List = []
    ):
        self.client = client
        self.checkpoint = checkpoint
        self.limit = limit
        self.relation_types_exclude = relation_types_exclude
        # Storage Attrs
        self._is_initialised = False
        self._remote_workbook_ids = []
        self._workbooks = []
        self._datasources = []
        self._connections = []
        self._relations = []
        self._table_references = []
        self._observed_at = None

    def _initialise(self):
        """ **Warning:** this is an expensive operation. Initialising
        a TableauWorkbookService does the work to download Workbooks and
        extract child entities into an in-memory data structure. The
        `checkpoint` and `limit` args are used to limit the number of Workbooks
        fetched at any given time.
        """
        if not self._is_initialised:
            self._observed_at = datetime.utcnow().isoformat()
            self._remote_workbook_ids = self.client.list_all_workbook_ids()
            # Get filtered list of workbook ids
            filtered_workbook_ids = self.client.list_workbook_ids(
                checkpoint=self.checkpoint, limit=self.limit
            )
            lwx = LocalWorkbookExtractor(
                relation_types_exclude=self.relation_types_exclude
            )
            for lwb in self.client.iterate_server_workbooks(filtered_workbook_ids):
                extraction_result = lwx.extract(lwb)
                # Store Extraction Results
                self._workbooks.extend(extraction_result.workbooks)
                self._datasources.extend(extraction_result.datasources)
                self._connections.extend(extraction_result.connections)
                self._relations.extend(extraction_result.relations)
                self._table_references.extend(extraction_result.table_references)
            # Mark service as initialised
            self._is_initialised = True
        return self._is_initialised

    @property
    def observed_at(self):
        self._initialise()
        return self._observed_at

    @property
    def remote_workbook_ids(self):
        self._initialise()
        return self._remote_workbook_ids

    @property
    def workbooks(self):
        self._initialise()
        return self._workbooks

    @property
    def workbooks(self):
        self._initialise()
        return self._workbooks

    @property
    def workbook_datasources(self):
        self._initialise()
        return self._datasources

    @property
    def workbook_connections(self):
        self._initialise()
        return self._connections

    @property
    def workbook_relations(self):
        self._initialise()
        return self._relations

    @property
    def workbook_table_references(self):
        self._initialise()
        return self._table_references

    def get_deleted_workbooks(self, bookmarked_workbook_ids):
        """ Compare current workbook id's to previously bookmarked ones,
        and return an updated bookmark and deletion records.
        """
        self._initialise()
        deleted_ids = list(
            set(bookmarked_workbook_ids) - set(self.remote_workbook_ids)
        )
        return self.remote_workbook_ids, [
            {'wb_id': wb_id, 'deleted_at': self.observed_at}
            for wb_id in deleted_ids
        ]
