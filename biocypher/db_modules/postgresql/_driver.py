"""
A wrapper around the Neo4j driver which handles the DBMS connection and
provides basic management methods.
"""
import itertools
from collections.abc import Iterable
from typing import Optional
from ..._create import BioCypherEdge, BioCypherNode
from ._writer import PostgresBatchWriter
from ..._logger import logger
from ..._driver import Driver
from ..._config import config as _config
from ... import _misc
import psycopg2

logger.debug(f'Loading module {__name__}.')


class PostgresDriver(Driver):
    """
    Manages a connection to a biocypher database.

    The connection can be defined in three ways:
        * Providing a ready ``neo4j.Driver`` instance
        * By URI and authentication data
        * By a YAML config file

    Args:
        driver:
            A ``neo4j.Driver`` instance, created by, for example,
            ``neo4j.GraphDatabase.driver``.
        db_name:
            Name of the database (Neo4j graph) to use.
        db_uri:
            Protocol, host and port to access the Neo4j server.
        db_user:
            Neo4j user name.
        db_passwd:
            Password of the Neo4j user.
        fetch_size:
            Optional; the fetch size to use in database transactions.
        wipe:
            Wipe the database after connection, ensuring the data is
            loaded into an empty database.
        offline:
            Do not connect to the database, but use the provided
            schema to create a graph representation and write CSVs for
            admin import.
        output_directory:
            Directory to write CSV files to.
        increment_version:
            Whether to increase version number automatically and create a
            new BioCypher version node in the graph.
        user_schema_config_path:
            Path to the graph database schema configuration file.
        clear_cache:
            Whether to clear the ontological hierarchy cache at driver
            instantiation. The cache is used to speed up the translation
            of Biolink classes to the database schema.
        delimiter:
            Delimiter for CSV export.
        array_delimiter:
            Array delimiter for CSV exported contents.
        quote_char:
            String quotation character for CSV export.
        skip_bad_relationships:
            Whether to skip relationships with missing source or target
            nodes in the admin import shell command.
        skip_duplicate_nodes:
            Whether to skip duplicate nodes in the admin import shell
            command.
        tail_ontology_url:
            URL of the ontology to hybridise to the head ontology.
        head_join_node:
            Biolink class of the node to join the tail ontology to.
        tail_join_node:
            Ontology class of the node to join the head ontology to.
    """

    def __init__(
            self,
            db_name: Optional[str] = None,
            db_uri: Optional[str] = None,
            db_user: Optional[str] = None,
            db_passwd: Optional[str] = None,
            delimiter: Optional[str] = None,
            array_delimiter: Optional[str] = None,
            quote_char: Optional[str] = None,
            *args,
            **kwargs) -> None:

        # Neo4j options
        self.db_name = db_name or _config('postgres_db')
        db_uri = db_uri or _config('postgres_uri')
        db_user = db_user or _config('postgres_user')
        db_passwd = db_passwd or _config('postgres_pw')
        delimiter = delimiter or _config('postgres_delimiter')
        array_delimiter = array_delimiter or _config('postgres_array_delimiter')
        quote_char = quote_char or _config('postgres_quote_char')

        # pass arguments to the driver
        self = super(PostgresDriver, self).__init__(
            db_name=db_name,
            db_uri=db_uri,
            db_user=db_user,
            db_passwd=db_passwd,
            delimiter=delimiter,
            array_delimiter=array_delimiter,
            quote_char=quote_char,
            *args,
            **kwargs,)

    def init_db(self):
        """
        Used to initialise a property graph database by deleting
        contents and constraints and setting up new constraints.

        Todo:
            - set up constraint creation interactively depending on the
                need of the database
        """
        # self.wipe_db()
        self._create_constraints()
        logger.info('Initialising database.')

    def _create_constraints(self):
        """
        Creates constraints on node types in the graph. Used for
        initial setup.

        Grabs leaves of the ``schema_config.yaml`` file and creates
        constraints on the id of all entities represented as nodes.
        """

        logger.info('Creating constraints for node types in config.')

        # get structure
        for leaf in self.db_meta.leaves.items():
            label = leaf[0]
            if leaf[1]['represented_as'] == 'node':
                print('constraint for label')
                print(label)
                s = (
                    f'CREATE CONSTRAINT `{label}_id` '
                    f'IF NOT EXISTS ON (n:`{label}`) '
                    'ASSERT n.id IS UNIQUE'
                )
                # self.query(s)

    def update_meta_graph(self):
        return

        if self.offline:
            return

        logger.info('Updating Neo4j meta graph.')
        # add version node
        self.add_biocypher_nodes(self.db_meta)

        # find current version node
        db_version = self.query(
            'MATCH (v:BioCypher) '
            'WHERE NOT (v)-[:PRECEDES]->() '
            'RETURN v',
        )
        # connect version node to previous
        if db_version[0]:
            e_meta = BioCypherEdge(
                self.db_meta.graph_state['id'],
                self.db_meta.node_id,
                'PRECEDES',
            )
            self.add_biocypher_edges(e_meta)

        # add structure nodes
        no_l = []
        # leaves of the hierarchy specified in schema yaml
        for entity, params in self.db_meta.leaves.items():
            no_l.append(
                BioCypherNode(
                    node_id=entity,
                    node_label='MetaNode',
                    properties=params,
                ),
            )
        self.add_biocypher_nodes(no_l)

        # remove connection of structure nodes from previous version
        # node(s)
        self.query('MATCH ()-[r:CONTAINS]-()'
                   'DELETE r', )

        # connect structure nodes to version node
        ed_v = []
        current_version = self.db_meta.get_id()
        for entity in self.db_meta.leaves.keys():
            ed_v.append(
                BioCypherEdge(
                    source_id=current_version,
                    target_id=entity,
                    relationship_label='CONTAINS',
                ),
            )
        self.add_biocypher_edges(ed_v)

        # add graph structure between MetaNodes
        ed = []
        for no in no_l:
            id = no.get_id()
            src = no.get_properties().get('source')
            tar = no.get_properties().get('target')
            if None not in [id, src, tar]:
                ed.append(BioCypherEdge(id, src, 'IS_SOURCE_OF'))
                ed.append(BioCypherEdge(id, tar, 'IS_TARGET_OF'))
        self.add_biocypher_edges(ed)

    def add_biocypher_nodes(
        self,
        nodes: Iterable[BioCypherNode],
        explain: bool = False,
        profile: bool = False,
    ) -> bool:
        """
        Accepts a node type handoff class
        (:class:`biocypher.create.BioCypherNode`) with id,
        label, and a dict of properties (passing on the type of
        property, ie, ``int``, ``str``, ...).

        The dict retrieved by the
        :meth:`biocypher.create.BioCypherNode.get_dict()` method is
        passed into Neo4j as a map of maps, explicitly encoding node id
        and label, and adding all other properties from the 'properties'
        key of the dict. The merge is performed via APOC, matching only
        on node id to prevent duplicates. The same properties are set on
        match and on create, irrespective of the actual event.

        Args:
            nodes:
                An iterable of :class:`biocypher.create.BioCypherNode` objects.
            explain:
                Call ``EXPLAIN`` on the CYPHER query.
            profile:
                Do profiling on the CYPHER query.

        Returns:
            True for success, False otherwise.
        """

        try:

            entities = [
                node.get_dict() for node in _misc.ensure_iterable(nodes)
            ]

        except AttributeError:

            msg = 'Nodes must have a `get_dict` method.'
            logger.error(msg)

            raise ValueError(msg)

        logger.info(f'Merging {len(entities)} nodes.')

        entity_query = (
            'UNWIND $entities AS ent '
            'CALL apoc.merge.node([ent.node_label], '
            '{id: ent.node_id}, ent.properties, ent.properties) '
            'YIELD node '
            'RETURN node'
        )

        # result = getattr(self, 'query')(
        #     entity_query,
        #     parameters={
        #         'entities': entities,
        #     },
        # )
        print('Skipping merging node, tbi')
        result = True

        logger.info('Finished merging nodes.')

        return result

    def add_biocypher_edges(
        self,
        edges: Iterable[BioCypherEdge],
        explain: bool = False,
        profile: bool = False,
    ) -> bool:
        """
        Accepts an edge type handoff class
        (:class:`biocypher.create.BioCypherEdge`) with source
        and target ids, label, and a dict of properties (passing on the
        type of property, ie, int, string ...).

        The individual edge is either passed as a singleton, in the case
        of representation as an edge in the graph, or as a 4-tuple, in
        the case of representation as a node (with two edges connecting
        to interaction partners).

        The dict retrieved by the
        :meth:`biocypher.create.BioCypherEdge.get_dict()` method is
        passed into Neo4j as a map of maps, explicitly encoding source
        and target ids and the relationship label, and adding all edge
        properties from the 'properties' key of the dict. The merge is
        performed via APOC, matching only on source and target id to
        prevent duplicates. The same properties are set on match and on
        create, irrespective of the actual event.

        Args:
            edges:
                An iterable of :class:`biocypher.create.BioCypherEdge` objects.
            explain:
                Call ``EXPLAIN`` on the CYPHER query.
            profile:
                Do profiling on the CYPHER query.

        Returns:
            `True` for success, `False` otherwise.
        """

        edges = _misc.ensure_iterable(edges)
        edges = itertools.chain(*(_misc.ensure_iterable(i) for i in edges))

        nodes = []
        rels = []

        try:

            for e in edges:

                if hasattr(e, 'get_node'):

                    nodes.append(e.get_node())
                    rels.append(e.get_source_edge().get_dict())
                    rels.append(e.get_target_edge().get_dict())

                else:

                    rels.append(e.get_dict())

        except AttributeError:

            msg = 'Edges and nodes must have a `get_dict` method.'
            logger.error(msg)

            raise ValueError(msg)

        self.add_biocypher_nodes(nodes)
        logger.info(f'Merging {len(rels)} edges.')

        # merging only on the ids of the entities, passing the
        # properties on match and on create;
        # TODO add node labels?
        node_query = (
            'UNWIND $rels AS r '
            'MERGE (src {id: r.source_id}) '
            'MERGE (tar {id: r.target_id}) '
        )

        # self.query(node_query, parameters={'rels': rels})

        edge_query = (
            'UNWIND $rels AS r '
            'MATCH (src {id: r.source_id}) '
            'MATCH (tar {id: r.target_id}) '
            'WITH src, tar, r '
            'CALL apoc.merge.relationship'
            '(src, r.relationship_label, NULL, '
            'r.properties, tar, r.properties) '
            'YIELD rel '
            'RETURN rel'
        )

        print('Skipping merging node, tbi')
        result = True

        # result = getattr(self, 'query')(edge_query, parameters={'rels': rels})

        logger.info('Finished merging edges.')

        return result

    def start_batch_writer(self) -> None:
        print('self.ontology_adapter', self.ontology_adapter)
        if not self.batch_writer:
            self.batch_writer = PostgresBatchWriter(
                leaves=self.db_meta.leaves,
                ontology_adapter=self.ontology_adapter,
                translator=self.translator,
                delimiter=self.db_delim,
                array_delimiter=self.db_adelim,
                quote=self.db_quote,
                dirname=self.output_directory,
                db_name=self.db_name,
                skip_bad_relationships=self.skip_bad_relationships,
                skip_duplicate_nodes=self.skip_duplicate_nodes,
                wipe=self.wipe,
                strict_mode=self.strict_mode,
            )

    def __repr__(self): 
        return f'<BioCypher Postgres>'
