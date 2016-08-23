${HEADER}

${CODE_LINK}

In this example we'll add a visibility to our Edges so that we can control access to them.

Each row in our data file is a pair of integers with a colour label and a visibility. The visibility is either `"public"` or `"private"`. Anyone should be able to see Edges labelled `"public"` but only those with the correct credentials should be able to see `"private"` Edges. Of course, those allowed to access `"private"` Edges can also see `"public"` Edges.
${DATA}

First we'll have a step through of the code and see what happens when we run it. Then we'll go into a bit more detail about how it all works.

We have updated the generated to add the visibility label as a new property, a Java String, on the Edges:
${DATA_GENERATOR_JAVA}

The generated elements are:

```
${GENERATED_EDGES}
```

Because we've added an extra property we need to add it to the DataSchema - all the edges get a new `"visibility"` property. We have also told Gaffer that whenever it sees a property called 'visibility' this is a special property and should be used for restricting a user's visibility of the data.:
${DATA_SCHEMA_JSON}

and we've defined a new `"visibility.string"` type in our DataTypes, which is a Java String and must be non-null to be loaded into the Graph.:
${DATA_TYPES_JSON}

We've also updated the StoreTypes and specified that the visibility property is aggregated using the ${VISIBILITY_AGGREGATOR_LINK} function.
${STORE_TYPES_JSON}


After creating a Graph and adding our Edges to it we run a simple query to get back all edges containing vertex `"1"`

```java
GetRelatedEdges getRelatedEdges = new GetRelatedEdges.Builder()
        .addSeed(new EntitySeed("1"))
        .build();

for (Element e : graph5.execute(getRelatedEdges)) {
    System.out.println(e.toString());
}
```

and we get nothing back. This is because the user we ran the query with was not allowed to see "public" or "private", no edges were returned.

We can create a user that can see "private" Edges (and therefore "public" ones as well):

```java
final User privateUser = new User.Builder()
    .userId("privateUser")
    .dataAuth("private")
    .build();
```

If we rerun the query now, we get back all of the Edges:

```
${GET_PRIVATE_RELATED_EDGES_RESULT}
```

Notice that the Edges have been aggregated within their visibilities but not across them.

Let's look in a bit more detail at what's going on here.

####Accumulo, Visibilities and Aggregation

So far, we haven't really had to know much about the store that we are using behind our Graph.

We're running our Graph on top of [Accumulo] (https://accumulo.apache.org), which is a distributed key value store. 

In this section we'll go into a bit more detail about Accumulo. That said, this is not intended as an in depth exploration of what Accumulo is or how it works, but more of a general overview of the parts important for understanding this example. Sometimes we will forego accuracy for the sake of clarity. The link above goes into much more depth.

Accumulo is interesting mainly because it does two things:
* Manages row level security.
* Aggregates data as a background process and at query time.

#####Accumulo

Accumulo is a key value store, but the key has some structure, consisting of a number of 'columns':

* RowId
* Column Family
* Column Qualifier
* Column Visibility
* TimeStamp


The AccumuloStore requires some way of mapping parts of a Gaffer Element into an Accumulo Row. This is done using a ${ACCUMULO_KEY_PACKAGE} and the `storeTypes.json`. Let’s take another look at the `storeTypes.json` file for this example:
${STORE_TYPES_JSON}

We’ve override the default serialisation for the `visibility.string` type to use our own custom serialiser for the visibility property: [`gaffer.example.gettingstarted.serialiser.VisibilitySerialiser`](https://github.com/GovernmentCommunicationsHeadquarters/Gaffer/blob/master/example/src/main/java/gaffer/example/gettingstarted/serialiser/VisibilitySerialiser.java). To see how this works we need to understand how Accumulo’s row level security works, at least at a high level. In our DataSchema we assigned the visibilityProperty field to the property name 'visibility', this tells Accumulo to store that property in the visibility column.

#####Accumulo's Visibility Column and Visibility Serialisers

The full detail is [here](https://accumulo.apache.org/1.4/user_manual/Security.html) and [here](https://accumulo.apache.org/1.5/apidocs/org/apache/accumulo/core/security/ColumnVisibility.html). Basically,  Accumulo decides what a user can or can’t see based on some strings that it holds in its visibility column and some boolean operators that are applied to them. The strings are tokens that represent visibility labels. Boolean operators define what combinations are allowed. Connections to Accumulo are via an Accumulo user who has a particular set of ‘authorisations’. These authorisations are matched against the tokens and boolean operations held in the visibility column for each row and the row is returned if there is a positive match. 

In our example, we have elements with either `public` or `private` labels and we want people with `public` authorisations to only be able to see the `public` elements and those with `private` to see both `public` and `private`.

If we look at the VisibilitySerialiser class:

${VISIBILITY_SERIALISER_CODE}
 
We can see that in the `serialise()` method a `public` label on an element gets converted into an Accumulo token that allows access to users with either `public` or `private` access. That means the element is returned to anyone with either `public` or `private` credentials. If the element is `public` then we simply pass that value through to Accumulo. Crucially, if a user has no credentials, they see nothing at all. The `deserialise()` method converts back to the label on the element (which is the value held inside the `visibility` property).

So if we run the example we see that for the first query, where we haven’t passed any authorisations through to Accumulo, we get nothing back.

For the second query we have added some user Authorisations to our operation and we get back the public and private edges.

Notice that the `count`s for these edges have been aggregated within visibilities - we have aggregated the `private` and `public` ones separately. 

Clearly, we should be able to override this and just get the aggregated `count` for all of the edges we are allowed to see and give the resulting edge some sort of merged visibility representing the aggregate. 

This brings us on to the next topic.

#####Accumulo compactions and aggregations.

So if we want to aggregate our edges _across_ visibilities we need to describe how we merge visibilities together. 

In our case using `public` and `private`, we might suggest that if any of the edges that we are merging have the `private` label, then we label the merged (aggregated) edge as `private`. If the set we are merging are all at `public`, we simply label the result `public`.

So we would take these edges

```
${GET_PRIVATE_RELATED_EDGES_RESULT}
```

and get these

```
${GET_PRIVATE_AGGREGATED_RELATED_EDGES_RESULT}
```

To do this we need to use a custom aggregator function: ${VISIBILITY_AGGREGATOR_LINK}. If you look at this function you’ll see that it does just what we described above - takes a set of visibilities and returns `private` if one or more are `private` and public otherwise. (Notice it also complains if there is a visibility which is not `public` or `private`).

We supply this aggregator function in `storeTypes.json` with the `visibility.string` type.
```

You might say, ‘But we had already supplied that when we ran the previous query but we still didn’t aggregate across visibilities’. And you are correct.

First let’s look at how to make the Gaffer Operation merge across visibilities and then explain what’s going on.

If we add this to the query:

```java
getRelatedEdges.setSummarise(true);
```

and rerun it we get these Edges

```
${GET_PRIVATE_AGGREGATED_RELATED_EDGES_RESULT}
```

which is exactly what we wanted.

I grant that this can be confusing. On the one hand we get some properties aggregated for free whenever we run a query (the `count`s for example). But for some of the properties, we have to explicitly tell the query to summarise the results using the `setSummarise(true)` flag.

This is a quirk of Accumulo. But a powerful quirk.

Accumulo runs things called ‘compactions’ on the data in it. The full details are in the Accumulo documentation but we will describe it briefly here.

If we have several rows where the _whole_ key is _identical_ , Accumulo will, as a background process, merge the values together. The logic for doing the merge is supplied via an Accumulo [iterator](http://accumulo.apache.org/1.6/accumulo_user_manual.html#_iterators). 

At query time, Accumulo gives us a bit more flexibility in which keys we define as equal. We have to have equal RowIds (although we can do [range scans](https://accumulo.apache.org/user_manual_1.3-incubating/Writing_Accumulo_Clients.html#a-idscannera-scanner)) but we can be flexible on the rest, as long as we can tell Accumulo how to merge rows with the different keys. To do this we again use an Iterator.

So Accumulo can summarise our data for us as a background process or, in a slightly more flexible way, at query time.

What does this mean for a Gaffer graph stored in Accumulo? Well, any of the properties on our Elements that we put in the Accumulo value will always be summarised, using the compaction time iterator logic. Those that we put in the key (outside of the RowId and Column Family) we can _choose_ to summarise at query time as long as we supply functions that can describe how to do it. If we don’t choose to summarise at query time, we just get the compaction time logic applied.

So in this example, because we put the `count` property in the Accumulo value, it always gets merged. The visibility only gets merged when we specify full summarisation using the `setSummarise(true)` flag.