.. _semantics:

==============
Data Semantics
==============

ScrubJay uses a semantic language to define how to load datasets, how to transform them, what they contain, and
properties of the dimensions over which they are defined.

.. _dataspace:

DataSpace
---------

A DataSpace describes the datasets and dimensions available for ScrubJay to derive new datasets from.
Every ScrubJay query must be made in the context of a DataSpace.

DataSpaces do not actually encode the data, but rather definitions called :ref:`datasetid` instances that describe how
to create the data.
The dimensions of a DataSpace are defined as a single :ref:`dimensionspace` .
DatasetID columns each have an associated dimension in the DimensionSpace, and ScrubJay uses this information to
compare different columns to one another.

.. _datasetid:

DatasetID
---------

A DatasetID identifies how to create data from one or more sources. An "original" DatasetID describes how to load
data from a single source (e.g., a CSV file or database table), without performing any transformations, and a "derived"
DatasetID describes how to derive a DatasetID from one or more original DatasetIDs and one or more transformations
(e.g., join operations).

Every DatasetID contains the following fields:

:code:`type`
    For an original DatasetID, this describes the type of source, e.g., :code:`CSVDatasetID`.
    For a derived DatasetID, this describes the type of derivation, e.g., join.

:code:`sparkSchema`
    The low-level Spark schema of the data, describing data types of each column, e.g. :code:`int`, :code:`array<string>`

:code:`scrubJaySchema`
    The high-level ScrubJay schema of the data, describing the dimensions and units of columns, as well as whether each
    column represents a domain or a value (see :ref:`domains_and_values`).

Different types of DatasetIDs contain additional parameters. For example, a CSVDatasetID contains the path of the CSV
file. Derived DatasetIDs contain parameters for transformations as well.

.. _domains_and_values:

Domains and Values
------------------

When data is collected, some entity is being measured, and some measurement is being made. For example, a thermometer
measures temperature at some time and place. We define the entity being measured (the time and place) as the `domain`,
and the measurement itself (the temperature) as the `value`.

Each column in a DatasetID must be defined as either a domain or a value column. This way, ScrubJay can determine
whether two different measurements are measuring the same entity.

.. _dimensionspace:

DimensionSpace
--------------

A DimensionSpace specifies a set of dimensions and their properties.
Each dimension contains the fields:

:code:`name`
    A uniquely identifying name for the dimension.

:code:`ordered`
    Whether the dimension has an ordering, such that there is a less-than relation between two points on that dimension.

:code:`continuous`
    Whether the dimension is continuous, i.e., there always exists another point between any two points on that dimension.

