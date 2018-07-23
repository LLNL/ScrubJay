# ScrubJay

A framework for automatic and scalable data integration. 
Describe your datasets (files, formats, database tables), then describe the integrated dataset(s) you desire, and let 
ScrubJay derive it for you in a consistent and reproducible way.

ScrubJay was developed for analyzing the supercomputing facilities at Lawrence Livermore National Laboratory, but is 
not specifically tied to any one kind of data. 

## Install

You can use a precompiled version of ScrubJay by downloading a release jar and adding it to your classpath.

todo: Create a github release jar and link to it here.

## Example Usage

We have three datasets:

1. Job queues describing what jobs ran on what nodes over what time ranges
2. Data describing which nodes reside on which racks
3. Collected FLOPs at different points in time for each node

Now, lets say we want to analyze how the FLOPs of different jobs are distributed over a rack.
This requires integrating all three datasets in a non-trivial way.
With ScrubJay, we specify the data columns that we want and ask ScrubJay to generate solutions containing them.

First, we load a DataSpace which describes the dimensions and datasets we are using.

```scala
   val dataSpace = DataSpace.fromJsonFile("jobAnalysis.sj")
```

Then, we create a query target, which is just the schema of the dataset that we want, and use it to create a query
in the created dataspace.

```scala
   val querySchema = ScrubJaySchema(Array(
     ScrubJayField(domain = true, dimension = "rack"),
     ScrubJayField(domain = false, dimension = "flops")
   ))

   val query = Query(dataSpace, querySchema)
```

todo: Queries may be generated using the ScrubJay Query Language.

We find solutions to the query (there may be multiple) using:

```scala
   val solutions = query.solutions
```

This gives us all solutions as a lazily-evaluated iterator.
A solution is a DatasetID, which describes the resulting dataset and how to derive it.
To derive a solution as a Spark DataFrame, run the `realize` function on it, specifying the dimension space used
to create the query.
For example, to derive the first solution:

```scala
   val realizedDataFrame = solutions.head.realize(dataSpace.dimensionSpace)
```

We can also see how the solution was derived in a DAG, using `toAsciiGraphString`:

```scala
   DatasetID.toAsciiGraphString(solution)
```

which produces

```

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
```
