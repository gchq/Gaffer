Copyright 2017 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This page has been copied from the Operation module README. To make any changes please update that README and this page will be automatically updated when the next release is done.

Operations
============

This module contains the `Operation` interfaces and core operation implementations.

It is assumed that all Gaffer graphs will be able to handle these core operations.

An `Operation` implementation defines an operation to be processed on a
graph, or on a set of results which are returned by another operation. An
`Operation` class contains the configuration required to tell Gaffer how
to carry out the operation. For example, the `AddElements` operation contains
the elements to be added. The `GetElements` operation contains the seeds
to use to find elements in the graph and the filters to apply to the query.
The operation classes themselves should not contain the logic to carry out
the operation (as this may vary between the different supported store types),
just the configuration.

For each operation, each Gaffer store will have an `OperationHandler`, where
the processing logic is contained. This enables operations to be handled
differently in each store.

Operations can be chained together to form an `OperationChain`. When an
operation chain is executed on a Gaffer graph the output of one operation
is passed to the input of the next.

An `OperationChain.Builder` is provided to help with constructing a valid
operation chain - it ensures the output type of an operation matches the
input type of the next.

## How to write an Operation

Operations should be written to be as generic as possible to allow them
to be applied to different graphs/stores.

Operations must be JSON serialisable in order to be used via the REST API
- i.e. there must be a public constructor and all the fields should have
getters and setters.

Operation implementations need to implement the `Operation` interface and
the extra interfaces they wish to make use of. For example an operation
that takes a single input value should implement the `Input` interface.

Here is a list of some of the common interfaces:
- uk.gov.gchq.gaffer.operation.io.Input
- uk.gov.gchq.gaffer.operation.io.Output
- uk.gov.gchq.gaffer.operation.io.InputOutput - Use this instead of Input
and Output if your operation takes both input and output.
- uk.gov.gchq.gaffer.operation.io.MultiInput - Use this in addition if you
operation takes multiple inputs. This will help with JSON serialisation
- uk.gov.gchq.gaffer.operation.SeedMatching
- uk.gov.gchq.gaffer.operation.Validatable
- uk.gov.gchq.gaffer.operation.graph.OperationView
- uk.gov.gchq.gaffer.operation.graph.GraphFilters
- uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters

Each operation implementation should have a corresponding unit test class
that extends the `OperationTest` class.

Operation implementations should override the close method and ensure all
closeable fields are closed.

Any fields that are required should be annotated with the Required annotation.

All implementations should also have a static inner `Builder` class that
implements the required builders. For example:

```java
public static class Builder extends Operation.BaseBuilder<GetElements, Builder>
        implements InputOutput.Builder<GetElements, Iterable<? extends ElementId>, CloseableIterable<? extends Element>, Builder>,
        MultiInput.Builder<GetElements, ElementId, Builder>,
        SeededGraphFilters.Builder<GetElements, Builder>,
        SeedMatching.Builder<GetElements, Builder>,
        Options.Builder<GetElements, Builder> {
    public Builder() {
            super(new GetElements());
    }
}
```

## Lazy Results
Operation results are lazy (where possible) so that results are lazily
loaded whilst a user consumes each result.

For example if a user executes a GetAllElements on Accumulo:

```java
final Iterable<? extends Element> elements = graph.execute(new GetAllElements(), getUser());
```

The 'elements' iterable is lazy and the query is only executed on Accumulo when you start iterating around the results. 
If you iterate around the results a second time, the query on Accumulo will be executed again.

If you add another element 'X' to the graph before you consume the 'elements' iterable you will notice the results now also contain 'X'.

For this reason you should be very careful if you do an AddElements with a lazy iterable returned from a Get query on the same Graph. The problem that could arise is that the AddElements will lazily consume the lazy iterable of elements, potentially causing duplicates to be added. 

To do a Get followed by an Add on the same Graph, we recommend consuming and caching the Get results first. For a small number of results, this can be done simply using the ToList operation in your chain. e.g:

```java
new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new ToList<>())
                .then(new AddElements())
                .build();
```


For a large number of results you could add them to the gaffer cache temporarily:
```java
new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new ExportToGafferResultCache<>())
                .then(new DiscardOutput())
                .then((Operation) new GetGafferResultCacheExport())
                .then(new AddElements())
                .build()
```

## FAQs
Here are some frequently asked questions.

#### If I do a query like GetElements or GetAdjacentIds the response type is a CloseableIterable - why?
To avoid loading all the results into memory, Gaffer stores should return an iterable that lazily loads and returns the data as a user iterates around the results. In the cases of Accumulo and HBase this means a connection to Accumulo/HBase must remain open whilst you iterate around the results. This closeable iterable should automatically close itself when you get to the end of the results. However, if you decide not to read all the results, i.e you just want to check if the results are not empty !results.iterator().hasNext() or an exception is thrown whilst iterating around the results, then the results iterable will not be closed and hence the connection to Accumulo/HBase will remain open. Therefore, to be safe you should always consume the results in a try-with-resources block.

#### Following on from the previous question, why can't I iterate around the results in parallel?
As mentioned above the results iterable holds a connection open to Accumulo/HBase. To avoid opening multiple connections accidentally leaving the connections open, the Accumulo and HBase stores only allow one iterator to be active at a time. When you call .iterator() the connection is opened. If you call .iterator() again, the original connection is closed and a new connection is opened. This means you can't process the iterable in parallel using Java 8's streaming api. If the results will fit in memory you could add them to a Set/List and then process that collection in parallel.

#### How do I return all my results summarised?
You need to provide a View to override the groupBy fields for all the element groups defined in the Schema. If you set the groupBy field to an empty array it will mean no properties will be included in the element key, i.e all the properties will be summarised. You can do this be provided a View like this:

```json
"view": {
    "globalElements" : [{
        "groupBy" : []
    }]
}
```

#### My queries are returning duplicate results - why and how can I deduplicate them?
For example, if you have a Graph containing the Edge A-B and you do a GetElements with a large number of seeds, with the first seed A and the last seed B, then you will get the Edge A-B back twice. This is because Gaffer stores lazily return the results for your query to avoid loading all the results into memory so it will not realise the A-B has been queried for twice.

You can deduplicate your results in memory using the [ToSet](https://gchq.github.io/gaffer-doc/getting-started/operation-examples.html#toset-example) operation. But, be careful to only use this when you have a small number of results. It might be worth also using the [Limit](https://gchq.github.io/gaffer-doc/getting-started/operation-examples.html#limit-example) operation prior to ToSet to ensure you don't run out of memory.

e.g: 

```java
new OperationChain.Builder()
    .first(new GetAllElements())
    .then(new Limit<>(1000000))
    .then(new ToSet<>())
    .build();
```

#### I have just done a GetElements and now I want to do a second hop around the graph, but when I do a GetElements followed by another GetElements I get strange results.
You can seed a get related elements operation with vertices (EntityIds) or edges (EdgeIds). If you seed the operation with edges you will get back the Entities at the source and destination of the provided edges, in addition to the edges that match your seed.

For example, using this graph:

```

    --> 4 <--
  /     ^     \
 /      |      \
1  -->  2  -->  3
         \
           -->  5
```

If you start with seed 1 and do a GetElements (related) then you would get back:

```
1
2
4
1 -> 2
1 -> 4
``` 

If you chain this into another GetElements then you would get back some strange results:

```
#Seed 1 causes these results
1
2
1 -> 2
1 -> 4

#Seed 2 causes these results
2
1
3
4
5
1 -> 2
2 -> 3
2 -> 4
2 -> 5

#Seed 4 causes these results
4
1
2
3
1 -> 4
2 -> 4
3 -> 4

#Seed 1 -> 2 causes these results
1
2
1 -> 2

#Seed 1 -> 4 causes these results
1
4
1 -> 4
```

So you get a lot of duplicates and unwanted results. What you really want to do is to use the GetAdjacentIds query to simply hop down the first edges and return just the vertices at the opposite end of the related edges. You can still provide a View and apply filters to the edges you traverse down. In addition it is useful to add a direction to the query so you don't go back down the original edges. So let's say we only want to traverse down outgoing edges. Doing a GetAdjacentIds with seed 1 would return:

```
2
4
``` 

Then you can do another GetAdjacentIds and get the following:

```
#Seed 2 causes these results
4
5

#Seed 4 does not have any outgoing edges so it doesn't match anything
```

You can continue doing multiple GetAdjacentIds to traverse around the Graph further. If you want the properties on the edges to be returned you can use GetElements in your final operation in your chain.

#### Any tips for optimising my queries?
Limit the number of groups you query for using a View - this could result in a
big improvement.

When defining filters in your View try and use the preAggregationFilter for all your filters as
this will be run before aggregation and will mean less work has to be done to aggregate
properties that you will later just discard. On Accumulo and HBase, postTransformFilters 
are not distributed, the are computed on a single node so they can be slow.
 
Some stores (like Accumulo) store the properties in different columns and lazily
deserialise a column as properties in that column are requested. So if you limit
your filters to just 1 column then less data needs to be deserialised. For 
Accumulo and HBase the columns are split up depending on whether the property is 
a groupBy property, the timestampProperty, the visibilityProperty and the remaining. 
So if you want to execute a time window query and your timestamp is a groupBy 
property or the special timestampProperty then depending on the store you are
running against this may be optimised. On Accumulo this will be fast as it 
doesn't need to deserialise the entire Value, just the column qualifier or timestamp column
containing your timestamp property.

Also, when defining the order of Predicates in a Filter, the order is important.
It will run the predicates in the order your provide so order them so that the first
ones are the more efficient and will filter out the most data. It is generally
more efficient to load/deserialise the groupBy properties than the non-groupBy
properties, as there are normally less of them. So if your filter applies to 2 
properties, a groupBy and a non-groupBy property, then we recommend putting the 
groupBy property filter first as that will normally be more efficient.

When doing queries, if you don't specify Pre or Post Aggregation filters then this
means the entire filter can be skipped. When running on stores like Accumulo this
means entire iterators can be skipped and this will save a lot of time. 
So, if applicable, you will save time if you put all your filtering in either the
Pre or Post section (in some cases this isn't possible).

Gaffer lets you specify validation predicates in your Schema to validate your data
when added and continuously in the background for age off. 
You can optimise this validation, by removing any unnecessary validation. 
You can do most of the validation you require in your ElementGenerator class when
you generate your elements. The validation you provide in the schema should be 
just the validation that you actually have to have, because this may be run A LOT.
On Accumulo - it is run in major/minor compactions and for every query. 
If you can, just validate properties that are in the groupBy or just the 
timestampProperty, this will mean that the store may not need to deserialise 
all of the other properties just to perform the validation.


#### How can I optimise the GetAdjacentIds query?
When doing GetAdjacentIds, try and avoid using PostTransformFilters. 
If you don't specify these then the final part of the query won't need to deserialise 
the properties it can just extract the destination off the edge. Also see the answer 
above for general query optimisation.


#### How can I optimise my AddElementsFromHdfs?
Try using the SampleDataForSplitPoints and SplitStore operations to calculate 
splits points. These can then be used to partition your data in the map reduce job
used to import the data. If adding elements into an empty Accumulo table or a table 
without any splits then the SampleDataForSplitPoints and SplitStore operations will
be executed automatically for you. You can also optionally provide your own splits 
points for your AddElementsFromHdfs operation.


#### I want to filter the results of my query based on the destination of the result Edges
OK, there are several ways of doing this and you will need to chose the most appropriate
way for your needs. Also worth reading [GetElements example](https://gchq.github.io/gaffer-doc/getting-started/operation-examples.html#getelements-example).

If you are querying with just a single EntitySeed with a vertex value of X and require
the destination to be Y then you should change your query to use an EdgeSeed 
with source = X and destination = Y and directedType = EITHER.

If you are querying with multiple EntitySeeds then just change each seed into an 
EdgeSeed as described above.
 
If you require your destination to match a provided regex than you will need to use
the regex filter: uk.gov.gchq.koryphe.impl.predicate.Regex or uk.gov.gchq.koryphe.impl.predicate.MultiRegex.
See the [Predicate examples](https://gchq.github.io/gaffer-doc/getting-started/predicate-examples.html).
The predicate can then be used in you Operation View to filter out elements that
don't match the regex. 

So, assuming your edge 'yourEdge' is directed and you provide a seed 'X' that is the source of the edge then you would apply the filter (e.g a simple regex [yY]) to the DESTINATION value.
Alternatively if your seed is the destination then your filter should be applied to the SOURCE value.

```java
GetElements results = new GetElements.Builder()
    .input(new EntitySeed("X"))
    .view(new View.Builder()
        .edge("yourEdge", new ViewElementDefinition.Builder()
            .preAggregationFilter(
                new ElementFilter.Builder()
                    .select(IdentifierType.DESTINATION.name())
                    .execute(new Regex("[yY]"))
                    .build())
            .build())
        .build())
    .build();
```

Finally, if your edge is undirected or the seed could be either the source or the
destination, then you will need to provide a filter that checks the SOURCE or the DESTINATION matches the regex. 
GetElements results = new GetElements.Builder()
    .input(new EntitySeed("X"))
    .view(new View.Builder()
        .edge("yourEdge", new ViewElementDefinition.Builder()
            .preAggregationFilter(
                new ElementFilter.Builder()
                    .select(IdentifierType.SOURCE.name(), IdentifierType.DESTINATION.name())
                    .execute(new Or.Builder<>()
                            .select(0)
                            .execute(new Regex("[yY]"))
                            .select(1)
                            .execute(new Regex("[yY]"))
                            .build())
                    .build())
            .build())
        .build())
    .build();


