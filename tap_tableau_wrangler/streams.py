"""Stream class for tap-tableau-wrangler."""
import requests
import pkg_resources
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


SCHEMAS_DIR = Path(pkg_resources.resource_filename('tap_tableau_wrangler', 'schemas/'))


class TableauWorkbookFile(Stream):
    """ Base Stream.
    """

    def __init__(
        self,
        tap: TapBaseClass,
        tableau_service: TableauWorkbookService,
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None
    ):
        """Initialize stream."""
        super().__init__(name=name, schema=schema, tap=tap)
        self._tablea_service = tableau_service

    @property
    def tableau_service(self):
        return self._tablea_service

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        attr = self.service_attr or self.name
        for row in getattr(self.tableau_service, attr, []):
            yield row


class Workbook(TableauWorkbookFile):
    """ Tableau Workbooks
    """

    name = 'workbook'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook.json'
    service_attr = 'workbooks'


class WorkbookIds(TableauWorkbookFile):
    """ All workbook id's.
    """

    name = 'workbook_ids'
    schema_filepath = SCHEMAS_DIR / 'workbook_ids.json'

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        yield {
            'observed_at': self.tableau_service.observed_at,
            'workbook_ids': self.tableau_service.remote_workbook_ids
        }


class WorkbookDatasource(TableauWorkbookFile):
    """ Tableau Workbook Datasources
    """

    name = 'workbook_datasource'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook_datasource.json'
    service_attr = 'workbook_datasources'


class WorkbookConnection(TableauWorkbookFile):
    """ Tableau Workbook Connections
    """

    name = 'workbook_connection'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook_connection.json'
    service_attr = 'workbook_connections'


class WorkbookRelation(TableauWorkbookFile):
    """ Tableau Workbook Relations
    """

    name = 'workbook_relation'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook_relation.json'
    service_attr = 'workbook_relations'


class WorkbookTableReference(TableauWorkbookFile):
    """ Tableau Workbook Table References
    """

    name = 'workbook_table_reference'
    primary_keys = ['id']
    replication_key = 'updated_at'
    schema_filepath = SCHEMAS_DIR / 'workbook_table_reference.json'
    service_attr = 'workbook_table_references'
