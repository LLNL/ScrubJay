# ScrubJay

A framework for automatic and scalable data integration. 
Describe your datasets (files, formats, database tables), then describe the integrated dataset(s) you desire, and let 
ScrubJay derive it for you in a consistent and reproducible way.

ScrubJay was developed for analyzing the supercomputing facilities at Lawrence Livermore National Laboratory, but is 
not specifically tied to any one kind of data. 

## Example Usage

We have three datasets:

1. Job queues describing what jobs ran on what nodes over what time ranges
2. Data describing which nodes reside on which racks
3. Collected FLOPs at different points in time for each node

Now, lets say we want to analyze how the FLOPs of different jobs are distributed over a rack. 
This requires integrating all three datasets in a non-trivial way. 
With ScrubJay, we simply specify the data columns that we want, and it automatically determines valid data integration pipelines to produce a result.

First, we load a "data space" which describes the dimensions and datasets we are using.

```scala
  import scrubjay.dataspace.DataSpace
  
  val dataSpace = DataSpace.fromJsonFile("jobAnalysis.sj")
```

Then, we create a "query target" which is just the schema of the dataset that we want.
For example, if we want to get FLOPs values aggregated across racks, we write:

```scala
  import scrubjay.datasetid.{ScrubJayField, ScrubJaySchema}
  
  val queryTarget = ScrubJaySchema(Array(
    ScrubJayField(domain = true, dimension = "rack"),
    ScrubJayField(domain = false, dimension = "flops")
  ))
```

Because `domain = true` for the "rack" dimension, values will be aggregated into racks, and because `domain = false` for 
the "flops" dimension, FLOPs values will be aggregated.

Now, we can find possible derivations that will yield a dataset with this schema by enumerating solutions of the query 
target:

```scala
queryTarget.solutions.foreach(solution => solution.debugPrint(dataSpace.dimensionSpace))
```

This will print out a bunch of information about each solution in addition to running the derivation and showing the 
results.
The first piece of information is the internal schema of the resulting spark dataframe:

```
Spark Schema:
root
 |-- domain:job:identifier: string (nullable = true)
 |-- domain:node:identifier: string (nullable = true)
 |-- value:time:seconds: integer (nullable = true)
 |-- domain:time:datetimestamp: sjlocaldatetimestring (nullable = true)
 |-- domain:rack:identifier: string (nullable = true)
 |-- value:flops:count: integer (nullable = true)
```

We see that we have string columns describing jobs, nodes, and racks, integer values describing elapsed time and FLOPs,
and a datetimestamp column. 

Next, the ScrubJay schema describes the high-level ScrubJay semantics of these columns:

```
ScrubJay Schema:
ScrubJaySchema
|--ScrubJayField(domain=true, name=domain:job:identifier, dimension=job, units=ScrubJayUnitsField(identifier,POINT,null,null,null))
|--ScrubJayField(domain=true, name=domain:node:identifier, dimension=node, units=ScrubJayUnitsField(identifier,POINT,null,null,null))
|--ScrubJayField(domain=true, name=domain:time:datetimestamp, dimension=time, units=ScrubJayUnitsField(datetimestamp,POINT,null,null,null))
|--ScrubJayField(domain=true, name=domain:rack:identifier, dimension=rack, units=ScrubJayUnitsField(identifier,POINT,null,null,null))
|--ScrubJayField(domain=false, name=value:time:seconds, dimension=time, units=ScrubJayUnitsField(seconds,POINT,null,null,null))
|--ScrubJayField(domain=false, name=value:flops:count, dimension=flops, units=ScrubJayUnitsField(count,POINT,null,null,null))
```

We see that elapsed time and FLOPs are both values (`domain=false`) aggregated into job, node, datetimestamp, and rack 
domains. This will always be a superset of the query target schema.

Next, ScrubJay shows a directed-acyclic graph (DAG) showing the derivation pipeline from the unmodified datasets in the
data space to the query target.
This includes the derivation functions like exploding discrete and continuous ranges, and interpolatively joining
numerical columns.


```
Derivation Graph:
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

Finally, we have the printout of the resulting spark dataframe:
 
```
DataFrame:
+---------------------+----------------------+------------------+----------------------------------+----------------------+-----------------+
|domain:job:identifier|domain:node:identifier|value:time:seconds|domain:time:datetimestamp         |domain:rack:identifier|value:flops:count|
+---------------------+----------------------+------------------+----------------------------------+----------------------+-----------------+
|123                  |3                     |23                |LocalDateTime(2016-08-11T03:30)   |1                     |0                |
|456                  |4                     |45                |LocalDateTime(2016-08-11T03:31)   |2                     |500              |
|456                  |5                     |45                |LocalDateTime(2016-08-11T03:32)   |2                     |20               |
|456                  |4                     |45                |LocalDateTime(2016-08-11T03:31:30)|2                     |750              |
|123                  |1                     |23                |LocalDateTime(2016-08-11T03:30)   |1                     |0                |
|456                  |6                     |45                |LocalDateTime(2016-08-11T03:31:30)|2                     |75               |
|456                  |5                     |45                |LocalDateTime(2016-08-11T03:30:30)|2                     |5                |
|456                  |6                     |45                |LocalDateTime(2016-08-11T03:30)   |2                     |0                |
|123                  |1                     |23                |LocalDateTime(2016-08-11T03:30:30)|1                     |250              |
|456                  |5                     |45                |LocalDateTime(2016-08-11T03:31:30)|2                     |15               |
|456                  |5                     |45                |LocalDateTime(2016-08-11T03:30)   |2                     |0                |
|123                  |3                     |23                |LocalDateTime(2016-08-11T03:31)   |1                     |50               |
|456                  |6                     |45                |LocalDateTime(2016-08-11T03:32)   |2                     |100              |
|456                  |6                     |45                |LocalDateTime(2016-08-11T03:31)   |2                     |50               |
|456                  |4                     |45                |LocalDateTime(2016-08-11T03:32)   |2                     |1000             |
|123                  |1                     |23                |LocalDateTime(2016-08-11T03:31)   |1                     |500              |
|123                  |3                     |23                |LocalDateTime(2016-08-11T03:30:30)|1                     |25               |
|456                  |4                     |45                |LocalDateTime(2016-08-11T03:30)   |2                     |0                |
|123                  |2                     |23                |LocalDateTime(2016-08-11T03:30)   |1                     |0                |
|456                  |5                     |45                |LocalDateTime(2016-08-11T03:31)   |2                     |10               |
+---------------------+----------------------+------------------+----------------------------------+----------------------+-----------------+
```
