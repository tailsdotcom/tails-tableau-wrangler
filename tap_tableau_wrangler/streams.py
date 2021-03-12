"""Stream class for tap-tableau-wrangler."""

import requests

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional, Union, Iterable

from singer.schema import Schema
from singer_sdk.streams import Stream
from singer_sdk.plugin_base import PluginBase as TapBaseClass
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

from tableau_wrangler import TableauWorkbookService, TableauServerClient


SCHEMAS_DIR = Path("./tap_tableau_wrangler/schemas")


class TableauWranglerStream(Stream):
    """ Base Stream.
    """

    def __init__(
        self,
        tap: TapBaseClass,
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None
    ):
        """Initialize stream."""
        super().__init__(name=name, schema=schema, tap=tap)
        self._service = None

    @property
    def service(self):
        if self._service:
            return self._service
        else:
            raise ValueError(
                "Service not set. Please set TableauWranglerStream.service "
                "to an initialised TableauWorkbookService instance."
            )

    @service.setter
    def service(self, service: TableauWorkbookService):
        self._service = service


class Workbook(TableauWranglerStream):
    """ Tableau Workbooks
    """

    name = 'workbook'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook.json'

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for row in self.service.workbooks:
            yield row


class WorkbookDatasource(TableauWranglerStream):
    """ Tableau Workbook Datasources
    """

    name = 'workbook_datasource'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook_datasource.json'

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for row in self.service.datasources:
            yield row


class WorkbookConnection(TableauWranglerStream):
    """ Tableau Workbook Connections
    """

    name = 'workbook_connection'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook_connection.json'

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for row in self.service.connections:
            yield row


class WorkbookRelation(TableauWranglerStream):
    """ Tableau Workbook Relations
    """

    name = 'workbook_relation'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook_relation.json'

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for row in self.service.relations:
            yield row


class WorkbookTableReference(TableauWranglerStream):
    """ Tableau Workbook Table References
    """

    name = 'workbook_table_reference'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook_table_reference.json'

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for row in self.service.table_references:
            yield row
