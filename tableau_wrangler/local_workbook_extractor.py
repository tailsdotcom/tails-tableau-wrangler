import uuid
import logging
import sqlfluff
import collections
from datetime import datetime, date

logging.getLogger("sqlfluff").setLevel(logging.WARNING)
logger = logging.getLogger('tableau_wrangler.local_workbook_extractor')

# Workbook
WORKBOOK_ATTRS = ['source_platform', 'source_build', 'worksheets']
# Datasource
DATASOURCE_ATTRS = ['name', 'caption', 'version']
# Connection
BASE_CONNECTION_ATTRS = [
    'class_', 'dbname', 'server', 'username',
    'authentication', 'port', 'channel', 'dataserver_permissions',
    'directory', 'server_oauth', 'workgroup_auth_mode',
    'query_band', 'initial_sql'
]
SQL_PROXY_ATTRS = []
NAMED_CONNECTION_ATTRS = ['name', 'caption']
CONNECTION_ATTRS = (
    BASE_CONNECTION_ATTRS + SQL_PROXY_ATTRS + NAMED_CONNECTION_ATTRS
)
# Relation
RELATION_ATTRS = ['type', 'name', 'connection', 'table', 'text']


def infer_dialect(relation):
    supported_dialects = [dialect.label for dialect in sqlfluff.dialects()]
    conn = relation.get('connection')
    if isinstance(conn, str) and conn:
        dialect = conn.split('.')[0].lower()
        if dialect in supported_dialects:
            return dialect
    return "ansi"


def clean_tableau_sql(sql):
    sql = sql.replace('<<', '<')
    sql = sql.replace('>>', '>')
    return sql


ExtractionResult = collections.namedtuple(
        'ExtractionResult',
        'workbooks, datasources, connections, relations, table_references'
)


class LocalWorkbookExtractor:
    """Class for extracting detail from a LocalWorkbook.
    """

    def __init__(self, relation_types_include=[], relation_types_exclude=[]):
        self.include = relation_types_include
        self.exclude = relation_types_exclude

    def _build_dict(self, obj, attrs):
        this = {}
        for k in attrs:
            if hasattr(obj, k):
                value = getattr(obj, k)
                if value:
                    this[k] = value
                else:
                    this[k] = None
            else:
                this[k] = None
        return this

    def _recurse_relations(
        self, relations, relation_types_include, relation_types_exclude
    ):
        found_relations = []
        for relation in relations:
            if (
                (relation.type in relation_types_include)
                or (relation.type not in relation_types_exclude)
            ):
                found_relations.append(relation)
                # Unpack nested Relations
                if relation.relation:
                    found_relations.extend(
                        self._recurse_relations(
                            relation.relation, relation_types_include, relation_types_exclude
                        )
                    )
        return found_relations

    def _sqlfluff_extract_table_refs(self, relations, updated_at):
        table_refs = []
        for relation in relations:
            if relation['type'] == 'text':
                if relation['text']:
                    dialect = infer_dialect(relation)
                    query = clean_tableau_sql(relation['text'])
                    # Parse SQL using SQLFluff
                    try:
                        parsed = sqlfluff.parse(query)
                        external_tables = parsed.tree.get_table_references()
                        for ref in external_tables:
                            table_refs.append(
                                self.jsonify_dict({
                                    'wb_id': relation['wb_id'],
                                    'ds_id': relation['ds_id'],
                                    'conn_id': relation['conn_id'],
                                    'rel_id': relation['id'],
                                    'id': f"{relation['wb_id']}:{relation['ds_id']}:{relation['conn_id']}:{relation['id']}:{uuid.uuid4()}",
                                    'ref': ref,
                                    'updated_at': updated_at
                                })
                            )
                    except sqlfluff.core.SQLBaseError as e:
                        logger.error(f"Falied to parse table refs: {e}")
                        pass
        return table_refs

    def _extract_relation(self, wb_id, ds_id, updated_at, relation):
        rel = {
            'wb_id': wb_id,
            'ds_id': ds_id,
            **self._build_dict(relation, RELATION_ATTRS)
        }
        conn_name = rel['connection'] or 'sqlproxy'
        conn_id = f"{ds_id}:{conn_name}"
        rel['conn_id'] = conn_id
        rel['id'] = f"{conn_id}:{rel['name']}"
        rel['updated_at'] = updated_at
        return self.jsonify_dict(rel)

    def _extract_relations(self, wb_id, ds_id, updated_at, relations):
        for rel in relations:
            if rel.relation:
                flat_relations = self._recurse_relations(
                    rel.relation, relation_types_include=self.include,
                    relation_types_exclude=self.exclude
                )
                return [
                    self._extract_relation(
                        wb_id, ds_id, updated_at, r
                    )
                    for r in flat_relations
                ]
            else:
                return [
                    self._extract_relation(
                        wb_id, ds_id, updated_at, rel
                    )
                ]

    def _extract_connections(self, wb_id, ds_id, updated_at, connection):
        connections = []
        relations = []
        if connection.class_ == 'sqlproxy':
            conn = {
                'wb_id': wb_id,
                'ds_id': ds_id,
                **self._build_dict(connection, CONNECTION_ATTRS)
            }
            conn['id'] = f"{ds_id}:sqlproxy"
            conn['updated_at'] = updated_at
            connections = [self.jsonify_dict(conn)]
        elif connection.class_ == 'federated':
            named_connections = []
            if connection.named_connections:
                for nc in connection.named_connections.values():
                    this_nc = {
                        'wb_id': wb_id,
                        'ds_id': ds_id,
                        **self._build_dict(nc, CONNECTION_ATTRS)
                    }
                    this_nc['id'] = f"{ds_id}:{this_nc['name']}"
                    this_nc['updated_at'] = updated_at
                    named_connections.append(self.jsonify_dict(this_nc))
                connections = named_connections
        if getattr(connection, 'relation'):
            relations.extend(
                self._extract_relations(
                    wb_id, ds_id, updated_at, connection.relation
                )
            )
        table_refs = self._sqlfluff_extract_table_refs(
            relations, updated_at=updated_at
        )
        return connections, relations, table_refs

    def _extract_datasource(self, wb_id, updated_at, datasource):
        ds = {
            'wb_id': wb_id,
            **self._build_dict(datasource, DATASOURCE_ATTRS)
        }
        ds['id'] = f"{wb_id}:{ds['name']}"  # add primary key
        ds['updated_at'] = updated_at
        return self.jsonify_dict(ds)

    def _extract_workbook(self, workbook):
        return self._build_dict(workbook, WORKBOOK_ATTRS)

    def _extract(self, local_workbook):
        wb = local_workbook.wb
        wb_id = local_workbook.wbi.id
        updated_at = local_workbook.wbi.updated_at

        datasources, connections, relations, table_refs = ([], [], [], [])
        if wb.datasources:
            for datasource in wb.datasources.values():
                ds = self._extract_datasource(wb_id, updated_at, datasource)
                datasources.append(ds)
                if datasource.connections:
                    ds_id = ds['id']
                    for connection in datasource.connections:
                        conns, relts, tbl_rfs = self._extract_connections(
                            wb_id, ds_id, updated_at, connection
                        )
                        connections.extend(conns)
                        relations.extend(relts)
                        table_refs.extend(tbl_rfs)

        return datasources, connections, relations, table_refs

    def jsonify_dict(self, adict):
        return {
            k: self._make_json_friendly(v)
            for k, v in adict.items()
        }

    def _make_json_friendly(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, set):
            return list(obj)
        else:
            return obj

    def extract(self, local_workbook):
        workbook = self.jsonify_dict({
            **local_workbook.to_dict()['workbook_item'],
            **self._extract_workbook(local_workbook.wb)
        })
        datasources, connections, relations, table_refs = \
            self._extract(local_workbook)
        return ExtractionResult(
            [workbook], datasources, connections, relations, table_refs
        )
