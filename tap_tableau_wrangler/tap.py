"""TailsTableauWrangler tap class."""

import click
from pathlib import Path
from typing import List
from singer_sdk import Tap, Stream
from singer_sdk.helpers.state import get_stream_state_dict
from singer_sdk.helpers.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_tableau_wrangler.streams import (
    Workbook, WorkbookIds, WorkbookDatasource, WorkbookConnection,
    WorkbookRelation, WorkbookTableReference
)
from tableau_wrangler import TableauServerClient, TableauWorkbookService


PLUGIN_NAME = "tap-tableau-wrangler"
WORKBOOK_FILE_STREAM_TYPES = [
    Workbook, WorkbookDatasource, WorkbookConnection,
    WorkbookRelation, WorkbookTableReference, WorkbookIds
]
OTHER_STREAM_TYPES = []
STREAM_TYPES = WORKBOOK_FILE_STREAM_TYPES + OTHER_STREAM_TYPES

class TapTailsTableauWrangler(Tap):
    """TailsTableauWrangler tap class."""

    name = "tap-tableau-wrangler"
    config_jsonschema = PropertiesList(
        Property("host", StringType, required=True),
        Property("username", StringType, required=True),
        Property("password", StringType, required=True),
        Property("batch_size", IntegerType, default=50)
    ).to_dict()
    _tablea_service = None

    @property
    def tableau_service(self):
        if self._tablea_service is None:
            checkpoint = (
                self.state.get('bookmarks', {})
                .get('workbook', {})
                .get('updated_at')
            )
            client = TableauServerClient(
                host=self.config['host'],
                username=self.config['username'],
                password=self.config['password']
            )
            self._tablea_service = TableauWorkbookService(
                client=client, checkpoint=checkpoint, limit=self.config['batch_size'],
                relation_types_exclude=['join']
            )
        return self._tablea_service

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = []
        streams.extend(
            [
                stream_class(tap=self, tableau_service=self.tableau_service)
                for stream_class in WORKBOOK_FILE_STREAM_TYPES
            ]
        )
        streams.extend(
            [stream_class(tap=self) for stream_class in OTHER_STREAM_TYPES]
        )
        return streams

# CLI Execution:
cli = TapTailsTableauWrangler.cli
