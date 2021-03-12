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
    Workbook, WorkbookDatasource, WorkbookConnection, WorkbookRelation,
    WorkbookTableReference
)
from tableau_wrangler import TableauServerClient, TableauWorkbookService


PLUGIN_NAME = "tap-tableau-wrangler"

STREAM_TYPES = [
    Workbook, WorkbookDatasource, WorkbookConnection, WorkbookRelation,
    WorkbookTableReference
]

WORKBOOK_FILE_STREAMS = [
    'workbook', 'workbook_datasource', 'workbook_connection',
    'workbook_relation', 'workbook_table_reference'
]


class TapTailsTableauWrangler(Tap):
    """TailsTableauWrangler tap class."""

    name = "tap-tableau-wrangler"
    config_jsonschema = PropertiesList(
        Property("host", StringType, required=True),
        Property("username", StringType, required=True),
        Property("password", StringType, required=True),
        Property("batch_size", IntegerType),  # TODO: add default of -1
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    def load_streams(self) -> List[Stream]:
        return self.discover_streams()

    def sync_all(self):
        """Sync all streams."""
        stream_names = self.streams.keys()
        requires_workbook_service = any(
            stream in stream_names for stream in WORKBOOK_FILE_STREAMS
        )
        if requires_workbook_service:
            checkpoint = None
            if 'workbook' in stream_names:
                wb_state = get_stream_state_dict(self.state, 'workbook')
                checkpoint = wb_state.get('bookmark')
            client = TableauServerClient(
                host=self.config['host'],
                username=self.config['username'],
                password=self.config['password']
            )
            # This actually does the work to download Workbooks
            service = TableauWorkbookService(client=client)
            service.initialise(
                checkpoint=checkpoint, limit=self.config.get('batch_size', 50)
            )
        # Sync Streams
        for stream in self.streams.values():
            if stream.name in WORKBOOK_FILE_STREAMS:
                stream.service = service
            stream.sync()

# CLI Execution:
cli = TapTailsTableauWrangler.cli
