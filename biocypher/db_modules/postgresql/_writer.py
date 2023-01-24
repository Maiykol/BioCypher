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
Export of CSV files for the Neo4J admin import. The admin import is able
to quickly transfer large amounts of content into an unused database. For more
explanation, see https://neo4j.com/docs/operations-manual/current/tuto\
rial/neo4j-admin-import/.

Import like that:
https://community.neo4j.com/t/how-can-i-use-a-database-created-with-neo4j-\
admin-import-in-neo4j-desktop/40594

    - Can properties the node/relationship does not own be left blank?

Formatting: --delimiter=";"
            --array-delimiter="|"
            --quote="'"

The header contains information for each field, for ID and properties
in the format <name>: <field_type>. E.g.:
`UniProtKB:ID;genesymbol;entrez_id:int;:LABEL`. Multiple labels can
be given by separating with the array delimiter.

There are three mandatory fields for relationship data:
:START_ID — ID referring to a node.
:END_ID — ID referring to a node.
:TYPE — The relationship type.

E.g.: `:START_ID;relationship_id;residue;:END_ID;:TYPE`.

Headers would best be separate files, data files with similar name but
different ending. Example from Neo4j documentation:

.. code-block:: bash

   bin/neo4j-admin import --database=neo4j
   --nodes=import/entities-header.csv,import/entities-part1.csv,
    import/entities-part2.csv
   --nodes=import/interactions-header.csv,import/interactions-part1.csv,
    import/interaction-part2.csv
   --relationships=import/rels-header.csv,import/rels-part1.csv,
    import/rels-part2.csv

Can use regex, e.g., [..] import/rels-part*. In this case, use padding
for ordering of the earlier part files ("01, 02").

# How to import:

1. stop the db

2. shell command:

.. code-block:: bash

   bin/neo4j-admin import --database=neo4j
   # nodes per type, separate header, regex for parts:
   --nodes="<path>/<node_type>-header.csv,<path>/<node_type>-part.*"
   # edges per type, separate header, regex for parts:
   --relationships="<path>/<edge_type>-header.csv,<path>/<edge_type>-part.*"

3. start db, test for consistency
"""

from ..._logger import logger

logger.debug(f'Loading module {__name__}.')

import os

from ..._write import BatchWriter

__all__ = ['PostgresBatchWriter']


DATA_TYPE_LOOKUP = {
    'str': 'VARCHAR',
    'int': 'INTEGER',
    'long': 'BIGINT',
    'float': 'NUMERIC',
    'double': 'NUMERIC',
    'dbl': 'NUMERIC',
    'boolean': 'BOOLEAN',
    'str[]': 'TEXT[]',
    'string[]': 'TEXT[]'
}
def _get_data_type(string):
    try:
        return DATA_TYPE_LOOKUP[string]
    except KeyError:
        logger.info('Could not determine data type {string}. Using default "VARCHAR"')
        return "VARCHAR"


class PostgresBatchWriter(BatchWriter):
    """
    Class for writing node and edge representations to disk using the
    format specified by Neo4j for the use of admin import. Each batch
    writer instance has a fixed representation that needs to be passed
    at instantiation via the :py:attr:`schema` argument. The instance
    also expects an ontology adapter via :py:attr:`ontology_adapter` to be able
    to convert and extend the hierarchy.

    Args:
        leaves:
            The BioCypher graph schema leaves (ontology classes).

        ontology_adapter:
            Instance of :py:class:`OntologyAdapter` to enable translation and
            ontology queries

        delimiter:
            The delimiter to use for the CSV files.

        array_delimiter:
            The delimiter to use for array properties.

        quote:
            The quote character to use for the CSV files.

        dirname:
            Path for exporting CSV files.

        db_name:
            Name of the Neo4j database that will be used in the generated
            commands.

        skip_bad_relationships:
            Whether to skip relationships that do not have a valid
            start and end node. (In admin import call.)

        skip_duplicate_nodes:
            Whether to skip duplicate nodes. (In admin import call.)

        wipe:
            Whether to force import (removing existing DB content). (In
            admin import call.)

        strict_mode:
            Whether to enforce source, version, and license properties.
    """
    def __init__(
        self,
        db_name: str = 'neo4j',
        *args, 
        **kwargs,
    ):
        # init BatchWriter
        super(PostgresBatchWriter, self).__init__(db_name=db_name, *args, **kwargs)

    def _write_node_headers(self):
        """
        Writes single CSV file for a graph entity that is represented
        as a node as per the definition in the `schema_config.yaml`,
        containing only the header for this type of node.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        # load headers from data parse
        if not self.node_property_dict:
            logger.error(
                'Header information not found. Was the data parsed first?',
            )
            return False


        print('node_property_dict', self.node_property_dict)

        for label, props in self.node_property_dict.items():
            # create header CSV with ID, properties, labels

            # to programmatically define properties to be written, the
            # data would have to be parsed before writing the header.
            # alternatively, desired properties can also be provided
            # via the schema_config.yaml.

            # translate label to PascalCase
            pascal_label = self.translator.name_sentence_to_pascal(label)

            header_path = os.path.join(
                self.outdir,
                f'{pascal_label}-create_table.sql',
            )
            parts_path = os.path.join(self.outdir, f'{pascal_label}-part.*')

            # check if file already exists
            if not os.path.exists(header_path):

                # concatenate key:value in props
                for table_name, values in props.items():
                    columns = [] # 'ID VARCHAR'
                    for col_name, col_type in values.items():
                        columns.append(f'{col_name} {_get_data_type(col_type)}')

                with open(header_path, 'w', encoding='utf-8') as f:

                    # concatenate with delimiter
                    command = f'CREATE TABLE {table_name}({self.delim.join(columns)});'
                    f.write(command)

                # add file path to import statement
                self.import_call_nodes.append([header_path, parts_path])

        return True

    def _write_edge_headers(self):
        """
        Writes single CSV file for a graph entity that is represented
        as an edge as per the definition in the `schema_config.yaml`,
        containing only the header for this type of edge.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        # load headers from data parse
        if not self.edge_property_dict:
            logger.error(
                'Header information not found. Was the data parsed first?',
            )
            return False

        for label, props in self.edge_property_dict.items():
            # create header CSV with :START_ID, (optional) properties,
            # :END_ID, :TYPE

            # translate label to PascalCase
            pascal_label = self.translator.name_sentence_to_pascal(label)

            # paths
            header_path = os.path.join(
                self.outdir,
                f'{pascal_label}-header.csv',
            )
            parts_path = os.path.join(self.outdir, f'{pascal_label}-part.*')

            # check for file exists
            if not os.path.exists(header_path):

                # concatenate key:value in props
                props_list = []
                for k, v in props.items():
                    if v in ['int', 'long']:
                        props_list.append(f'{k}:long')
                    elif v in ['float', 'double']:
                        props_list.append(f'{k}:double')
                    elif v in [
                        'bool',
                        'boolean',
                    ]:  # TODO does Neo4j support bool?
                        props_list.append(f'{k}:boolean')
                    else:
                        props_list.append(f'{k}')

                # create list of lists and flatten
                # removes need for empty check of property list
                out_list = [[':START_ID'], props_list, [':END_ID'], [':TYPE']]
                out_list = [val for sublist in out_list for val in sublist]

                with open(header_path, 'w', encoding='utf-8') as f:

                    # concatenate with delimiter
                    row = self.delim.join(out_list)
                    f.write(row)

                # add file path to import statement
                self.import_call_edges.append([header_path, parts_path])

        return True

    def write_import_call(self) -> bool:
        """
        Function to write the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name, to the export folder as txt.

        Returns:
            bool: The return value. True for success, False otherwise.
        """

        file_path = os.path.join(self.outdir, 'neo4j-admin-import-call.sh')
        logger.info(f'Writing neo4j-admin import call to `{file_path}`.')

        with open(file_path, 'w', encoding='utf-8') as f:

            f.write(self._construct_import_call())

        return True

    def _construct_import_call(self) -> str:
        """
        Function to construct the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name. Built after all data has been
        processed to ensure that nodes are called before any edges.

        Returns:
            str: a bash command for neo4j-admin import
        """

        # escape backslashes in self.delim and self.adelim
        delim = self.delim.replace('\\', '\\\\')
        adelim = self.adelim.replace('\\', '\\\\')

        import_call = (
            f'bin/neo4j-admin import --database={self.db_name} '
            f'--delimiter="{delim}" --array-delimiter="{adelim}" '
        )

        if self.quote == "'":
            import_call += f'--quote="{self.quote}" '
        else:
            import_call += f"--quote='{self.quote}' "

        if self.wipe:
            import_call += f'--force=true '
        if self.skip_bad_relationships:
            import_call += '--skip-bad-relationships=true '
        if self.skip_duplicate_nodes:
            import_call += '--skip-duplicate-nodes=true '

        # append node import calls
        #Format the neo4j admin import statement for all node-files in 
        # 'self.import_call_nodes' 
        for header_path, parts_path in self.import_call_nodes:
            import_call += f'--nodes="{header_path},{parts_path}" '

        # append edge import calls
        # Format the neo4j admin import statement for all edge-files in 
        # 'self.import_call_nodes' 
        for header_path, parts_path in self.import_call_edges:
            import_call += f'--relationships="{header_path},{parts_path}" '
        
        return import_call
