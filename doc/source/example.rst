..  _example:

Example
=======

We have three datasets:

1. Job queues describing what jobs ran on what nodes over what time ranges
2. Data describing which nodes reside on which racks
3. Collected FLOPs at different points in time for each node

Now, lets say we want to analyze how the FLOPs of different jobs are distributed over a rack.
This requires integrating all three datasets in a non-trivial way.
With ScrubJay, we specify the data columns that we want and ask ScrubJay to generate solutions containing them.

First, we load a :ref:`dataspace` which describes the dimensions and datasets we are using.

.. code-block:: scala

   val dataSpace = DataSpace.fromJsonFile("jobAnalysis.sj")

Then, we create a query target, which is just the schema of the dataset that we want, and use it to create a query
in the created dataspace.

.. code-block:: scala

   val querySchema = ScrubJaySchema(Array(
     ScrubJayField(domain = true, dimension = "rack"),
     ScrubJayField(domain = false, dimension = "flops")
   ))

   val query = Query(dataSpace, querySchema)

todo: Queries may be generated using the :ref:`sjql`.

We find solutions to the query (there may be multiple) using:

.. code-block:: scala

   val solutions = query.solutions

This gives us all solutions as a lazily-evaluated iterator.
A solution is a :ref:`datasetid`, which describes the resulting dataset and how to derive it.
To derive a solution as a Spark DataFrame, run the :code:`realize` function on it, specifying the dimension space used
to create the query.
For example, to derive the first solution:

.. code-block:: scala

   val realizedDataFrame = solutions.head.realize(dataSpace.dimensions)

We can also see how the solution was derived in a DAG, using :code:`toAsciiGraphString`:

.. code-block:: scala

   DatasetID.toAsciiGraphString(solution)

which produces::

                              ┌──────────────────────┐
                              │      CSVDataset      │
                              │domain:job:identifier,│
                              │  domain:node:list,   │
                              │ value:time:seconds,  │
                              │  domain:time:range   │
                              └───────────┬──────────┘
                                          │
               ┌──────────────────────────┘
               │
               v
   ┌───────────────────────┐                           ┌──────────────────────────┐
   │ ExplodeDiscreteRange  │ ┌───────────────────────┐ │        CSVDataset        │
   │domain:job:identifier, │ │      CSVDataset       │ │ domain:node:identifier,  │
   │domain:node:identifier,│ │domain:node:identifier,│ │domain:time:datetimestamp,│
   │  value:time:seconds,  │ │domain:rack:identifier │ │    value:flops:count     │
   │   domain:time:range   │ └───────────┬───────────┘ └───────┬──────────────────┘
   └───────────────────┬───┘             │                     │
                       │                 └────────────┐        │
                       │                              │        │
                       v                              v        v
          ┌─────────────────────────┐        ┌──────────────────────────┐
          │ ExplodeContinuousRange  │        │       NaturalJoin        │
          │ domain:job:identifier,  │        │ domain:node:identifier,  │
          │ domain:node:identifier, │        │ domain:rack:identifier,  │
          │   value:time:seconds,   │        │domain:time:datetimestamp,│
          │domain:time:datetimestamp│        │    value:flops:count     │
          └────────────┬────────────┘        └┬─────────────────────────┘
                       │                      │
                       └─────────────┐        │
                                     │        │
                                     v        v
                            ┌──────────────────────────┐
                            │    InterpolationJoin     │
                            │  domain:job:identifier,  │
                            │ domain:node:identifier,  │
                            │domain:time:datetimestamp,│
                            │ domain:rack:identifier,  │
                            │   value:time:seconds,    │
                            │    value:flops:count     │
                            └──────────────────────────┘
