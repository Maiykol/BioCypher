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
Abstract class for a writer to create csv files which can be used to import the data into a database. Intended to be inherited by database-specific writers with database-specific methods. This writer class contains t

Formatting: --delimiter=";"
            --array-delimiter="|"
            --quote="'"

The header contains information for each field, for ID and properties.
Headers would best be separate files, data files with similar name but
different ending.

# How to import:

1. stop the db

2. shell command

3. start db, test for consistency

A child-class of this class must implement these methods:
- _write_node_headers
- _write_edge_headers
- write_import_call
- _construct_import_call

"""

from abc import ABC, abstractmethod
from ._create import BioCypherEdge, BioCypherNode, BioCypherRelAsNode
from biocypher._config import config as _config
from more_itertools import peekable
import os
from collections import OrderedDict, defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Union, Optional
from types import GeneratorType
import glob

from ._logger import logger

logger.debug(f'Loading module {__name__}.')

__all__ = ['BatchWriter']

if TYPE_CHECKING:

    from ._translate import Translator, OntologyAdapter

# TODO retrospective check of written csvs?


class BatchWriter(ABC):
    """
    Class for writing node and edge representations to disk using csv format and creating a bash command to import the data.
    Each batch
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
            Name of the database that will be used in the generated
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
        leaves: dict,
        ontology_adapter: 'OntologyAdapter',
        translator: 'Translator',
        delimiter: str,
        array_delimiter: str = '|',
        quote: str  = '"',
        dirname: Optional[str] = None,
        db_name: str = '',
        skip_bad_relationships: bool = False,
        skip_duplicate_nodes: bool = False,
        wipe: bool = True,
        strict_mode: bool = False,
    ):
        self.db_name = db_name

        self.delim = delimiter
        self.adelim = array_delimiter
        self.quote = quote
        self.skip_bad_relationships = skip_bad_relationships
        self.skip_duplicate_nodes = skip_duplicate_nodes
        self.wipe = wipe

        self.strict_mode = strict_mode

        self.leaves = leaves
        self.ontology_adapter = ontology_adapter
        self.translator = translator
        self.node_property_dict = {}
        self.edge_property_dict = {}
        self.import_call_nodes = []
        self.import_call_edges = []

        timestamp = datetime.now().strftime('%Y%m%d%H%M')

        self.outdir = dirname or os.path.join(_config('outdir'), timestamp)
        self.outdir = os.path.abspath(self.outdir)

        logger.info(f'Creating output directory `{self.outdir}`.')
        os.makedirs(self.outdir, exist_ok=True)

        self.seen_node_ids = set()  # set to store the ids of nodes that have
        # already been written; to avoid duplicates
        self.duplicate_node_ids = set(
        )  # set to store the ids of nodes that were
        # found to have duplicates (avoid overloading the log)
        self.duplicate_node_types = set(
        )  # set to store the types of nodes that
        # have been found to have duplicates

        self.seen_edges = {}  # dict to store the set of edges that
        # have already been written; to avoid duplicates; per edge type
        self.duplicate_edge_ids = set()  # set to store the ids of edges that
        # were found to have duplicates (avoid overloading the log)
        self.duplicate_edge_types = set(
        )  # set to store the types of edges that
        # have been found to have duplicates

        # TODO not memory efficient, but should be fine for most cases; is
        # there a more elegant solution?

    def write_nodes(self, nodes, batch_size=int(1e6)):
        """
        Wrapper for writing nodes and their headers.

        Args:
            nodes (BioCypherNode): a list or generator of nodes in
                :py:class:`BioCypherNode` format

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        # TODO check represented_as

        # write node data
        passed = self._write_node_data(nodes, batch_size)
        if not passed:
            logger.error('Error while writing node data.')
            return False
        # pass property data to header writer per node type written
        passed = self._write_node_headers()
        if not passed:
            logger.error('Error while writing node headers.')
            return False

        return True

    def write_edges(
        self,
        edges: Union[list, GeneratorType],
        batch_size: int = int(1e6),
    ) -> bool:
        """
        Wrapper for writing edges and their headers.

        Args:
            edges (BioCypherEdge): a list or generator of edges in
                :py:class:`BioCypherEdge` or :py:class:`BioCypherRelAsNode`
                format

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        passed = False
        # unwrap generator in one step
        edges = list(edges)  # force evaluation to handle empty generator
        if edges:
            z = zip(
                *(
                    (
                        e.get_node(),
                        [
                            e.get_source_edge(),
                            e.get_target_edge(),
                        ],
                    ) if isinstance(e, BioCypherRelAsNode) else (None, [e])
                    for e in edges
                )
            )
            nod, edg = (list(a) for a in z)
            nod = [n for n in nod if n]
            edg = [val for sublist in edg for val in sublist]  # flatten

            if nod and edg:
                passed = self.write_nodes(nod) and self._write_edge_data(
                    edg,
                    batch_size,
                )
            else:
                passed = self._write_edge_data(edg, batch_size)

        else:
            # is this a problem? if the generator or list is empty, we
            # don't write anything.
            logger.debug(
                'No edges to write, possibly due to no matched Biolink classes.',
            )
            pass

        if not passed:
            logger.error('Error while writing edge data.')
            return False
        # pass property data to header writer per edge type written
        passed = self._write_edge_headers()
        if not passed:
            logger.error('Error while writing edge headers.')
            return False

        return True

    def _write_node_data(self, nodes, batch_size):
        """
        Writes biocypher nodes to CSV conforming to the headers created
        with `_write_node_headers()`, and is actually required to be run
        before calling `_write_node_headers()` to set the
        :py:attr:`self.node_property_dict` for passing the node properties
        to the instance. Expects list or generator of nodes from the
        :py:class:`BioCypherNode` class.

        Args:
            nodes (BioCypherNode): a list or generator of nodes in
                :py:class:`BioCypherNode` format

        Returns:
            bool: The return value. True for success, False otherwise.
        """

        if isinstance(nodes, GeneratorType) or isinstance(nodes, peekable):
            logger.debug('Writing node CSV from generator.')

            bins = defaultdict(list)  # dict to store a list for each
            # label that is passed in
            bin_l = {}  # dict to store the length of each list for
            # batching cutoff
            reference_props = defaultdict(
                dict,
            )  # dict to store a dict of properties
            # for each label to check for consistency and their type
            # for now, relevant for `int`
            labels = {}  # dict to store the additional labels for each
            # primary graph constituent from biolink hierarchy
            for node in nodes:
                _id = node.get_id()
                label = node.get_label()

                # check for non-id
                if not _id:
                    logger.warning(f'Node {label} has no id; skipping.')
                    continue

                # check if node has already been written, if so skip
                if _id in self.seen_node_ids:
                    self.duplicate_node_ids.add(_id)
                    if not label in self.duplicate_node_types:
                        self.duplicate_node_types.add(label)
                        logger.warning(
                            f'Duplicate nodes found in type {label}. '
                        )
                    continue

                if not label in bins.keys():
                    # start new list
                    all_labels = None
                    bins[label].append(node)
                    bin_l[label] = 1

                    # get properties from config if present
                    cprops = self.ontology_adapter.leaves.get(label).get(
                        'properties',
                    )
                    if cprops:
                        d = dict(cprops)

                        # add id and preferred id to properties; these are
                        # created in node creation (`_create.BioCypherNode`)
                        d['id'] = 'str'
                        d['preferred_id'] = 'str'

                        # add strict mode properties
                        if self.strict_mode:
                            d['source'] = 'str'
                            d['version'] = 'str'
                            d['licence'] = 'str'

                    else:
                        d = dict(node.get_properties())
                        # encode property type
                        for k, v in d.items():
                            if d[k] is not None:
                                d[k] = type(v).__name__
                    # else use first encountered node to define properties for
                    # checking; could later be by checking all nodes but much
                    # more complicated, particularly involving batch writing
                    # (would require "do-overs"). for now, we output a warning
                    # if node properties diverge from reference properties (in
                    # write_single_node_list_to_file) TODO if it occurs, ask
                    # user to select desired properties and restart the process

                    reference_props[label] = d

                    # get label hierarchy
                    # multiple labels:
                    all_labels = self.ontology_adapter.get_node_ancestry(label)

                    if all_labels:
                        # convert to pascal case
                        all_labels = [
                            self.translator.name_sentence_to_pascal(label)
                            for label in all_labels
                        ]
                        # remove duplicates
                        all_labels = list(OrderedDict.fromkeys(all_labels))
                        # order alphabetically
                        all_labels.sort()
                        # concatenate with array delimiter
                        all_labels = self.adelim.join(all_labels)
                    else:
                        all_labels = self.translator.name_sentence_to_pascal(
                            label
                        )

                    labels[label] = all_labels

                else:
                    # add to list
                    bins[label].append(node)
                    bin_l[label] += 1
                    if not bin_l[label] < batch_size:
                        # batch size controlled here
                        passed = self._write_single_node_list_to_file(
                            bins[label],
                            label,
                            reference_props[label],
                            labels[label],
                        )

                        if not passed:
                            return False

                        bins[label] = []
                        bin_l[label] = 0

                self.seen_node_ids.add(_id)

            # after generator depleted, write remainder of bins
            for label, nl in bins.items():
                passed = self._write_single_node_list_to_file(
                    nl,
                    label,
                    reference_props[label],
                    labels[label],
                )

                if not passed:
                    return False

            # use complete bin list to write header files
            # TODO if a node type has varying properties
            # (ie missingness), we'd need to collect all possible
            # properties in the generator pass

            # save config or first-node properties to instance attribute
            for label in reference_props.keys():
                self.node_property_dict[label] = reference_props[label]

            return True
        else:
            if type(nodes) is not list:
                logger.error('Nodes must be passed as list or generator.')
                return False
            else:

                def gen(nodes):
                    yield from nodes

                return self._write_node_data(gen(nodes), batch_size=batch_size)

    @abstractmethod
    def _write_node_headers(self):
        """
        Abstract method that takes care of importing properties of a graph entity that is represented
        as a node as per the definition in the `schema_config.yaml`

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        raise NotImplementedError(
            "Database writer must override '_write_node_headers'")

    def _write_single_node_list_to_file(
        self,
        node_list: list,
        label: str,
        prop_dict: dict,
        labels: str,
    ):
        """
        This function takes one list of biocypher nodes and writes them
        to an import compatible CSV file.

        Args:
            node_list (list): list of BioCypherNodes to be written
            label (str): the primary label of the node
            prop_dict (dict): properties of node class passed from parsing
                function and their types
            labels (str): string of one or several concatenated labels
                for the node class

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        if not all(isinstance(n, BioCypherNode) for n in node_list):
            logger.error('Nodes must be passed as type BioCypherNode.')
            return False

        # from list of nodes to list of strings
        lines = []

        for n in node_list:

            # check for deviations in properties
            # node properties
            n_props = n.get_properties()
            n_keys = list(n_props.keys())
            # reference properties
            ref_props = list(prop_dict.keys())

            # compare lists order invariant
            if not set(ref_props) == set(n_keys):
                onode = n.get_id()
                oprop1 = set(ref_props).difference(n_keys)
                oprop2 = set(n_keys).difference(ref_props)
                logger.error(
                    f'At least one node of the class {n.get_label()} '
                    f'has more or fewer properties than another. '
                    f'Offending node: {onode!r}, offending property: '
                    f'{max([oprop1, oprop2])}. '
                    f'All reference properties: {ref_props}, '
                    f'All node properties: {n_keys}.',
                )
                return False

            line = [n.get_id()]

            if ref_props:

                plist = []
                # make all into strings, put actual strings in quotes
                for k, v in prop_dict.items():
                    p = n_props.get(k)
                    if p is None:  # TODO make field empty instead of ""?
                        plist.append('')
                    elif v in [
                        'int',
                        'long',
                        'float',
                        'double',
                        'dbl',
                        'bool',
                        'boolean',
                    ]:
                        plist.append(str(p))
                    else:
                        if isinstance(p, list):
                            plist.append(
                                self.quote + self.adelim.join(p) + self.quote
                            )
                        else:
                            plist.append(self.quote + str(p) + self.quote)

                line.append(self.delim.join(plist))
            line.append(labels)

            lines.append(self.delim.join(line) + '\n')

        # avoid writing empty files
        if lines:
            self._write_next_part(label, lines)

        return True

    def _write_edge_data(self, edges, batch_size):
        """
        Writes biocypher edges to CSV conforming to the headers created
        with `_write_edge_headers()`, and is actually required to be run
        before calling `_write_node_headers()` to set the
        :py:attr:`self.edge_property_dict` for passing the edge
        properties to the instance. Expects list or generator of edges
        from the :py:class:`BioCypherEdge` class.

        Args:
            edges (BioCypherEdge): a list or generator of edges in
                :py:class:`BioCypherEdge` format

        Returns:
            bool: The return value. True for success, False otherwise.

        Todo:
            - currently works for mixed edges but in practice often is
              called on one iterable containing one type of edge only
        """

        if isinstance(edges, GeneratorType):
            logger.debug('Writing edge CSV from generator.')

            bins = defaultdict(list)  # dict to store a list for each
            # label that is passed in
            bin_l = {}  # dict to store the length of each list for
            # batching cutoff
            reference_props = defaultdict(
                dict,
            )  # dict to store a dict of properties
            # for each label to check for consistency and their type
            # for now, relevant for `int`
            for e in edges:
                if isinstance(e, BioCypherRelAsNode):
                    # shouldn't happen any more
                    logger.error(
                        "Edges cannot be of type 'RelAsNode'. "
                        f'Caused by: {e}',
                    )
                    return False

                if not (e.get_source_id() and e.get_target_id()):
                    logger.error(
                        'Edge must have source and target node. '
                        f'Caused by: {e}',
                    )
                    continue

                label = e.get_label()

                if not label in self.seen_edges.keys():
                    self.seen_edges[label] = set()

                src_tar_id = '_'.join([e.get_source_id(), e.get_target_id()])

                # check for duplicates
                if src_tar_id in self.seen_edges.get(label, set()):
                    self.duplicate_edge_ids.add(src_tar_id)
                    if not label in self.duplicate_edge_types:
                        self.duplicate_edge_types.add(label)
                        logger.warning(
                            f'Duplicate edges found in type {label}. '
                        )
                    continue

                else:
                    self.seen_edges[label].add(src_tar_id)

                if not label in bins.keys():
                    # start new list
                    bins[label].append(e)
                    bin_l[label] = 1

                    # get properties from config if present

                    # check whether label is in ontology_adapter.leaves
                    # (may not be if it is an edge that carries the
                    # "label_as_edge" property)
                    cprops = None
                    if label in self.ontology_adapter.leaves:
                        cprops = self.ontology_adapter.leaves.get(label).get(
                            'properties',
                        )
                    else:
                        # try via "label_as_edge"
                        for k, v in self.ontology_adapter.leaves.items():
                            if isinstance(v, dict):
                                if v.get('label_as_edge') == label:
                                    cprops = v.get('properties')
                                    break
                    if cprops:
                        d = cprops

                        # add strict mode properties
                        if self.strict_mode:
                            d['source'] = 'str'
                            d['version'] = 'str'
                            d['licence'] = 'str'

                    else:
                        d = dict(e.get_properties())
                        # encode property type
                        for k, v in d.items():
                            if d[k] is not None:
                                d[k] = type(v).__name__
                    # else use first encountered edge to define
                    # properties for checking; could later be by
                    # checking all edges but much more complicated,
                    # particularly involving batch writing (would
                    # require "do-overs"). for now, we output a warning
                    # if edge properties diverge from reference
                    # properties (in write_single_edge_list_to_file)
                    # TODO

                    reference_props[label] = d

                else:
                    # add to list
                    bins[label].append(e)
                    bin_l[label] += 1
                    if not bin_l[label] < batch_size:
                        # batch size controlled here
                        passed = self._write_single_edge_list_to_file(
                            bins[label],
                            label,
                            reference_props[label],
                        )

                        if not passed:
                            return False

                        bins[label] = []
                        bin_l[label] = 0

            # after generator depleted, write remainder of bins
            for label, nl in bins.items():

                passed = self._write_single_edge_list_to_file(
                    nl,
                    label,
                    reference_props[label],
                )

                if not passed:
                    return False

            # use complete bin list to write header files
            # TODO if a edge type has varying properties
            # (ie missingness), we'd need to collect all possible
            # properties in the generator pass

            # save first-edge properties to instance attribute
            for label in reference_props.keys():
                self.edge_property_dict[label] = reference_props[label]

            return True
        else:
            if type(edges) is not list:
                logger.error('Edges must be passed as list or generator.')
                return False
            else:

                def gen(edges):
                    yield from edges

                return self._write_edge_data(gen(edges), batch_size=batch_size)

    @abstractmethod
    def _write_edge_headers(self):
        """
        Abstract method to write a database import-file for a graph entity that is represented
        as an edge as per the definition in the `schema_config.yaml`,
        containing only the header for this type of edge.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        raise NotImplementedError(
            "Database writer must override '_write_edge_headers'")

    def _write_single_edge_list_to_file(
        self,
        edge_list: list,
        label: str,
        prop_dict: dict,
    ):
        """
        This function takes one list of biocypher edges and writes them
        to an admin import compatible CSV file.

        Args:
            edge_list (list): list of BioCypherEdges to be written

            label (str): the label (type) of the edge

            prop_dict (dict): properties of node class passed from parsing
                function and their types

        Returns:
            bool: The return value. True for success, False otherwise.
        """

        if not all(isinstance(n, BioCypherEdge) for n in edge_list):

            logger.error('Edges must be passed as type BioCypherEdge.')
            return False

        # from list of edges to list of strings
        lines = []
        for e in edge_list:
            # check for deviations in properties
            # edge properties
            e_props = e.get_properties()
            e_keys = list(e_props.keys())
            ref_props = list(prop_dict.keys())

            # compare list order invariant
            if not set(ref_props) == set(e_keys):
                oedge = f'{e.get_source_id()}-{e.get_target_id()}'
                oprop1 = set(ref_props).difference(e_keys)
                oprop2 = set(e_keys).difference(ref_props)
                logger.error(
                    f'At least one edge of the class {e.get_label()} '
                    f'has more or fewer properties than another. '
                    f'Offending edge: {oedge!r}, offending property: '
                    f'{max([oprop1, oprop2])}. '
                    f'All reference properties: {ref_props}, '
                    f'All edge properties: {e_keys}.',
                )
                return False

            if ref_props:

                plist = []
                # make all into strings, put actual strings in quotes
                for k, v in prop_dict.items():
                    p = e_props.get(k)
                    if p is None:  # TODO make field empty instead of ""?
                        plist.append('')
                    elif v in [
                        'int',
                        'long',
                        'float',
                        'double',
                        'dbl',
                        'bool',
                        'boolean',
                    ]:
                        plist.append(str(p))
                    else:
                        if isinstance(p, list):
                            plist.append(
                                self.quote + self.adelim.join(p) + self.quote
                            )
                        elif '**' in p:
                            plist.append(
                                self.quote + p.replace('**', self.adelim) +
                                self.quote
                            )
                        else:
                            plist.append(self.quote + str(p) + self.quote)

                lines.append(
                    self.delim.join(
                        [
                            e.get_source_id(),
                            # here we need a list of properties in
                            # the same order as in the header
                            self.delim.join(plist),
                            e.get_target_id(),
                            self.translator.
                            name_sentence_to_pascal(e.get_label(), ),
                        ],
                    ) + '\n',
                )
            else:
                lines.append(
                    self.delim.join(
                        [
                            e.get_source_id(),
                            e.get_target_id(),
                            self.translator.
                            name_sentence_to_pascal(e.get_label(), ),
                        ],
                    ) + '\n',
                )

        # avoid writing empty files
        if lines:
            self._write_next_part(label, lines)

        return True

    def _write_next_part(self, label: str, lines: list):
        """
        This function writes a list of strings to a new part file.

        Args:
            label (str): the label (type) of the edge; internal
            representation sentence case -> needs to become PascalCase
            for disk representation

            lines (list): list of strings to be written

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        # translate label to PascalCase
        label = self.translator.name_sentence_to_pascal(label)

        # list files in self.outdir
        files = glob.glob(os.path.join(self.outdir, f'{label}-part*.csv'))
        # find file with highest part number
        if files:
            next_part = (
                max(
                    [
                        int(
                            f.split('.')[-2].split('-')[-1].replace('part', '')
                        ) for f in files
                    ],
                ) + 1
            )
        else:
            next_part = 0

        # write to file
        padded_part = str(next_part).zfill(3)
        logger.info(
            f'Writing {len(lines)} entries to {label}-part{padded_part}.csv',
        )
        file_path = os.path.join(self.outdir, f'{label}-part{padded_part}.csv')

        with open(file_path, 'w', encoding='utf-8') as f:

            # concatenate with delimiter
            f.writelines(lines)

    def get_import_call(self) -> str:
        """
        Function to return the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name.

        Returns:
            str: a bash command for csv import
        """

        return self._construct_import_call()

    def write_import_call(self) -> bool:
        """
        Function to write the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name, to the export folder as txt.

        Returns:
            bool: The return value. True for success, False otherwise.

        Template:
            file_path = os.path.join(self.outdir, '<database>-import-call.sh')
            logger.info(f'Writing <database> import call to `{file_path}`.')

            with open(file_path, 'w', encoding='utf-8') as f:

                f.write(self._construct_import_call())

            return True
        """
        raise NotImplementedError(
            "Database writer must override 'write_import_call'"
        )

    @abstractmethod
    def _construct_import_call(self) -> str:
        """
        Function to construct the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name. Built after all data has been
        processed to ensure that nodes are called before any edges.

        Returns:
            str: a bash command for csv import
        """
        raise NotImplementedError(
            "Database writer must override '_construct_import_call'"
        )

    def get_duplicate_nodes(self):
        """
        Function to return a list of duplicate nodes.

        Returns:
            list: list of duplicate nodes
        """

        if self.duplicate_node_types:
            return (self.duplicate_node_types, self.duplicate_node_ids)
        else:
            return None

    def get_duplicate_edges(self):
        """
        Function to return a list of duplicate edges.

        Returns:
            list: list of duplicate edges
        """

        if self.duplicate_edge_types:
            return (self.duplicate_edge_types, self.duplicate_edge_ids)
        else:
            return None
