#!/usr/bin/env python

#
# Copyright 2021, Heidelberg University Clinic
#
# File author(s): Sebastian Lobentanzer
#                 ...
#
# Distributed under GPLv3 license, see the file `LICENSE`.
#
"""
A abstract driver class intended to be used as a parent to database-specific drivers.
It handles the DBMS connection and provides basic management methods.
"""
from ._translate import Translator, BiolinkAdapter, OntologyAdapter
from ._create import VersionNode, BioCypherEdge, BioCypherNode
from ._config import config as _config
from more_itertools import peekable
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional
from collections.abc import Iterable

from ._logger import logger

logger.debug(f'Loading module {__name__}.')


if TYPE_CHECKING:

    import neo4j


__all__ = ['Driver']


class Driver(ABC):
    """
    Abstract parent class for BioCypher database drivers. 
        * Provides basic functions needed by all drivers to 
            implement the BioCyher features. 
        * Child classes can add database-specific functionalities.

    Args:
        db_name:
            Name of the database to use.
        db_uri:
            Protocol, host and port to access the database server.
        db_user:
            database user name.
        db_passwd:
            Password of the database user.
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
        # multi_db: Optional[bool] = None,
        fetch_size: int = 1000,
        skip_bad_relationships: bool = False,
        skip_duplicate_nodes: bool = False,
        wipe: bool = False,
        strict_mode: Optional[bool] = None,
        offline: Optional[bool] = None,
        output_directory: Optional[str] = None,
        increment_version: bool = True,
        clear_cache: Optional[bool] = None,
        user_schema_config_path: Optional[str] = None,
        delimiter: Optional[str] = None,
        array_delimiter: Optional[str] = None,
        quote_char: Optional[str] = None,
        tail_ontology_url: Optional[str] = None,
        head_join_node: Optional[str] = None,
        tail_join_node: Optional[str] = None,
    ):

        self.db_delim = delimiter
        self.db_adelim = array_delimiter
        self.db_quote = quote_char

        self.skip_bad_relationships = skip_bad_relationships
        self.skip_duplicate_nodes = skip_duplicate_nodes
        self.wipe = wipe

        if offline is None:
            self.offline = _config('offline')
        else:
            self.offline = offline

        # BioCypher options
        self.user_schema_config_path = user_schema_config_path or _config(
            'user_schema_config_path'
        )
        self.strict_mode = strict_mode or _config('strict_mode')
        self.output_directory = output_directory or _config('output_directory')
        self.clear_cache = clear_cache or _config('clear_cache')

        self.tail_ontology_url = tail_ontology_url or _config(
            'tail_ontology_url'
        )
        self.head_join_node = head_join_node or _config('head_join_node')
        self.tail_join_node = tail_join_node or _config('tail_join_node')

        if self.offline:

            if not self.user_schema_config_path:
                raise ValueError(
                    'Offline mode requires a user schema config file.'
                    ' Please provide one with the `user_schema_config_path`'
                    ' argument or set the `user_schema_config_path`'
                    ' configuration variable.'
                )

            logger.info('Offline mode: no connection to Neo4j.')

            self.db_meta = VersionNode(
                from_config=True,
                config_file=self.user_schema_config_path,
                offline=True,
                bcy_driver=self,
            )

            self._db_config = {
                'uri': db_uri,
                'user': db_user,
                'passwd': db_passwd,
                'db': db_name,
                'fetch_size': fetch_size,
            }

            self.driver = None
            
        else:

            # if db representation node does not exist or explicitly
            # asked for wipe, create new graph representation: default
            # yaml, interactive?
            if wipe:

                # get database version node ('check' module) immutable
                # variable of each instance (ie, each call from the
                # adapter to BioCypher); checks for existence of graph
                # representation and returns if found, else creates new
                # one
                self.db_meta = VersionNode(
                    from_config=offline or wipe,
                    config_file=self.user_schema_config_path,
                    bcy_driver=self,
                )

                # init requires db_meta to be set
                self.init_db()

            else:

                self.db_meta = VersionNode(self)

        if increment_version:

            # set new current version node
            self.update_meta_graph()

        self.ontology_adapter = None
        self.batch_writer = None
        self._update_translator()

        return self

        # TODO: implement passing a driver instance
        # I am not sure, but seems like it should work from driver

    @abstractmethod
    def update_meta_graph(self):
        raise NotImplementedError("Database driver must override 'update_meta_graph'")

    def _update_translator(self):

        self.translator = Translator(
            leaves=self.db_meta.leaves,
            strict_mode=self.strict_mode,
        )

    @abstractmethod
    def init_db(self):
        """
        Placeholder for db initialisation for the database drivers.
        Tasks performed are:
            * clear database from old content
            * clear old constraints
            * set up new constraints
        """
        raise NotImplementedError("Database driver must override 'init_db'")

    def add_nodes(self, id_type_tuples: Iterable[tuple]) -> tuple:
        """
        Generic node adder method to add any kind of input to the graph via the
        :class:`biocypher.create.BioCypherNode` class. Employs translation
        functionality and calls the :meth:`add_biocypher_nodes()` method.

        Args:
            id_type_tuples (iterable of 3-tuple): for each node to add to
                the biocypher graph, a 3-tuple with the following layout:
                first, the (unique if constrained) ID of the node; second, the
                type of the node, capitalised or PascalCase and in noun form
                (Neo4j primary label, eg `:Protein`); and third, a dictionary
                of arbitrary properties the node should possess (can be empty).

        Returns:
            2-tuple: the query result of :meth:`add_biocypher_nodes()`
                - first entry: data
                - second entry: Neo4j summary.
        """

        bn = self.translator.translate_nodes(id_type_tuples)
        return self.add_biocypher_nodes(bn)

    def add_edges(self, id_src_tar_type_tuples: Iterable[tuple]) -> tuple:
        """
        Generic edge adder method to add any kind of input to the graph
        via the :class:`biocypher.create.BioCypherEdge` class. Employs
        translation functionality and calls the
        :meth:`add_biocypher_edges()` method.

        Args:

            id_src_tar_type_tuples (iterable of 5-tuple):

                for each edge to add to the biocypher graph, a 5-tuple
                with the following layout: first, the optional unique ID
                of the interaction. This can be `None` if there is no
                systematic identifier (which for many interactions is
                the case). Second and third, the (unique if constrained)
                IDs of the source and target nodes of the relationship;
                fourth, the type of the relationship; and fifth, a
                dictionary of arbitrary properties the edge should
                possess (can be empty).

        Returns:

            2-tuple: the query result of :meth:`add_biocypher_edges()`

                - first entry: data
                - second entry: Neo4j summary.
        """

        bn = self.translator.translate_edges(id_src_tar_type_tuples)
        return self.add_biocypher_edges(bn)

    @abstractmethod
    def add_biocypher_nodes(self):
        raise NotImplementedError("Database driver must override 'add_biocypher_nodes'")

    @abstractmethod
    def add_biocypher_edges(self):
        raise NotImplementedError("Database driver must override 'add_biocypher_edges'")

    def write_nodes(self, nodes):
        """
        Write BioCypher nodes to disk, formatting the CSV to 
        enable database-specific import from the target directory.

        Args:
            nodes (iterable): collection of nodes to be written in
                BioCypher-compatible CSV format; can be any compatible
                (ie, translatable) input format or already as
                :class:`biocypher.create.BioCypherNode`.
        """

        # instantiate adapter on demand because it takes time to load
        # the biolink model toolkit
        self.start_ontology_adapter()

        self.start_batch_writer()

        nodes = peekable(nodes)
        if not isinstance(nodes.peek(), BioCypherNode):
            tnodes = self.translator.translate_nodes(nodes)
        else:
            tnodes = nodes
        # write node files
        return self.batch_writer.write_nodes(tnodes)

    @abstractmethod
    def start_batch_writer(self, ) -> None:
        """
        Abstract mehtod to instantiate the batch writer if it does not exist. Writer should be implement on a database level.

        Args:
            dirname (str): the directory to write the files to
            db_name (str): the name of the database to write the files to
        """
        raise NotImplementedError("Database driver must override 'add_biocypher_edges'")

    def start_ontology_adapter(self) -> None:
        """
        Instantiate the :class:`biocypher._translate.OntologyAdapter` if not
        existing.
        """
        if not self.ontology_adapter:
            biolink_adapter = BiolinkAdapter(
                leaves=self.db_meta.leaves,
                translator=self.translator,
                clear_cache=self.clear_cache,
            )
            # only simple one-hybrid case; TODO generalise
            self.ontology_adapter = OntologyAdapter(
                tail_ontology_url=self.tail_ontology_url,
                head_join_node=self.head_join_node,
                tail_join_node=self.tail_join_node,
                biolink_adapter=biolink_adapter,
            )

    def write_edges(
        self,
        edges,
    ) -> None:
        """
        Write BioCypher edges to disk using the :mod:`write` module,
        formatting the CSV to enable Neo4j admin import from the target
        directory.

        Args:
            edges (iterable): collection of edges to be written in
                BioCypher-compatible CSV format; can be any compatible
                (ie, translatable) input format or already as
                :class:`biocypher.create.BioCypherEdge`.
        """

        # instantiate adapter on demand because it takes time to load
        # the biolink model toolkit
        self.start_ontology_adapter()

        self.start_batch_writer()

        edges = peekable(edges)
        if not isinstance(edges.peek(), BioCypherEdge):
            tedges = self.translator.translate_edges(edges)
        else:
            tedges = edges
        # write edge files
        self.batch_writer.write_edges(tedges)

    def get_import_call(self):
        """
        Upon using the batch writer for writing admin import CSV files,
        return a string containing the neo4j admin import call with
        delimiters, database name, and paths of node and edge files.

        Returns:
            str: a database-specific import call
        """
        return self.batch_writer.get_import_call()

    def write_import_call(self):
        """
        Upon using the batch writer for writing admin import CSV files,
        write a string containing the neo4j admin import call with
        delimiters, database name, and paths of node and edge files, to
        the export directory.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        return self.batch_writer.write_import_call()

    def log_missing_bl_types(self):
        """
        Get the set of Biolink types encountered without an entry in
        the `schema_config.yaml` and print them to the logger.

        Returns:
            set: a set of missing Biolink types
        """

        mt = self.translator.get_missing_biolink_types()

        if mt:
            msg = (
                'Input entities not accounted for due to them not being '
                'present in the `schema_config.yaml` configuration file '
                '(this is not necessarily a problem, if you did not intend '
                'to include them in the database; see the log for details): \n'
            )
            for k, v in mt.items():
                msg += f'    {k}: {v} \n'

            logger.info(msg)
            return mt

        else:
            logger.info('No missing Biolink types in input.')
            return None

    def log_duplicates(self):
        """
        Get the set of duplicate nodes and edges encountered and print them to
        the logger.
        """

        dn = self.batch_writer.get_duplicate_nodes()

        if dn:

            ntypes = dn[0]
            nids = dn[1]

            msg = ('Duplicate node types encountered (IDs in log): \n')
            for typ in ntypes:
                msg += f'    {typ}\n'

            logger.info(msg)

            idmsg = ('Duplicate node IDs encountered: \n')
            for _id in nids:
                idmsg += f'    {_id}\n'

            logger.debug(idmsg)

        else:
            logger.info('No duplicate nodes in input.')

        de = self.batch_writer.get_duplicate_edges()

        if de:

            etypes = de[0]
            eids = de[1]

            msg = ('Duplicate edge types encountered (IDs in log): \n')
            for typ in etypes:
                msg += f'    {typ}\n'

            logger.info(msg)

            idmsg = ('Duplicate edge IDs encountered: \n')
            for _id in eids:
                idmsg += f'    {_id}\n'

            logger.debug(idmsg)

        else:
            logger.info('No duplicate edges in input.')

    def show_ontology_structure(self) -> None:
        """
        Show the ontology structure of the database using the Biolink schema and
        treelib.
        """

        self.start_ontology_adapter()

        self.ontology_adapter.show_ontology_structure()

    # TRANSLATION METHODS ###

    def translate_term(self, term: str) -> str:
        """
        Translate a term to its BioCypher equivalent.
        """

        # instantiate adapter if not exists
        self.start_ontology_adapter()

        return self.translator.translate_term(term)

    def reverse_translate_term(self, term: str) -> str:
        """
        Reverse translate a term from its BioCypher equivalent.
        """

        # instantiate adapter if not exists
        self.start_ontology_adapter()

        return self.translator.reverse_translate_term(term)

    def translate_query(self, query: str) -> str:
        """
        Translate a query to its BioCypher equivalent.
        """

        # instantiate adapter if not exists
        self.start_ontology_adapter()

        return self.translator.translate(query)

    def reverse_translate_query(self, query: str) -> str:
        """
        Reverse translate a query from its BioCypher equivalent.
        """

        # instantiate adapter if not exists
        self.start_ontology_adapter()

        return self.translator.reverse_translate(query)

    @abstractmethod
    def __repr__(self):
        raise NotImplementedError("Database driver must override '__repr__'")
