${HEADER}

${CODE_LINK}

The `FederatedStore` is simply a Gaffer store which forwards operations to a collection of sub-graphs and returns a single response as though it was a single graph.

This example explains how to create a `FederatedStore`, add/remove additional graphs, with/without authentication and call operations against the `FederatedStore`.

#### Configuration

To create a `FederatedStore` you need to initialise the store with a graphId and a properties file.

${FEDERATED_STORE_SNIPPET}

#### Adding Graphs

You can't add a graph using a graphId already in use, you will need to explicitly remove the old GraphId first.
You can limit access to the sub-graphs when adding to FederatedStore, see [Limiting Access](#limiting-access).

To load a graph into the `FederatedStore` you need to provide:
 * GraphId
 * Graph Schema
 * Graph Properties

**Note** Schema & Properties are not required if GraphId is known by the GraphLibrary

Using either the `FederatedStore` properties file...
```
gaffer.store.class=uk.gov.gchq.gaffer.federatedstore.FederatedStore
gaffer.store.properties.class=uk.gov.gchq.gaffer.store.StoreProperties

gaffer.federatedstore.customPropertiesAuths=<auths1>,<auths2>

gaffer.federatedstore.graphIds=<graphId1>, <graphId2>
gaffer.federatedstore.<GraphId1>.properties.file=<path/to/properties>
gaffer.federatedstore.<GraphId1>.schema.file=<path/to/schema>

gaffer.federatedstore.<GraphId2>.properties.id=<Id name in graphLibrary>
gaffer.federatedstore.<GraphId2>.schema.id=<Id name in graphLibrary>
```

or through the `AddGraph` operation. However this operation maybe limited for some
users if the `FederatedStore` property "gaffer.federatedstore.customPropertiesAuths" is defined.
Without matching authorisation users can only specify adding a graph with `StoreProperties` from the `GraphLibrary`.

${ADD_GRAPH_SNIPPET}

or through the rest service with json.

```json
${addGraphJson}
```

#### Removing Graphs

To remove a graph from the `FederatedStore` is even easier, you only need to know the graphId. This does not delete the graph only removes it from the scope.

${REMOVE_GRAPH_SNIPPET}

or through the rest service with json.

```
${removeGraphJson}
```

#### Getting all the GraphId's

To get a list of all the sub-graphs within the `FederatedStore` you can perform the following `GetAllGraphId` operation.

${GET_ALL_GRAPH_IDS_SNIPPET}

or through the rest service with json.

```json
${getAllGraphIdsJson}
```


and the result is:

```
${graphIds}
```

#### Performing Operations

Running operations against the `FederatedStore` is exactly same as running operations against any other store.
Behind the scenes the `FederatedStore` sends out operations/operation chains to the sub-graphs to be executed and returns back a single response.
`AddElements` operations is a special case, and only adds elements to sub-graphs when the edge or entity groupId is known by the sub-graph.

**Warning** When adding elements, if 2 sub-graphs contain the same group in the schema then the elements will be added to both of the sub-graphs.
A following `GetElements` operation would then return the same element from both sub-graph, resulting in duplicates.
It is advised to keep groups to within one sub-graph.

${ADD_ELEMENTS_SNIPPET}

${GET_ELEMENTS_SNIPPET}

and the results are:

```
${elements}
```

${GET_ELEMENTS_FROM_ACCUMULO_GRAPH_SNIPPET}

and the results are:

```
${elements from accumuloGraph}
```

#### Failed Execution
If the execution against a graph fails, that graph is skipped and the `FederatedStore` continues with the remaining graphs. Unless the operation has the option flag "skipFailedFederatedStoreExecute" set to `false`, in that situation a `OperationException` is thrown.

#### Limiting Access
It is possible to have a `FederatedStore` with many sub-graphs, however you may wish to limit the users access. This is possible by using authorisations at the time of adding a graph to the FederatedStore, this limits the graphs users can send operations to via the FederatedStore.
Explicitly setting authorisations to an empty list grants access to all users.

This can be done at `FederatedStore` Initialisation, by default if authorisations is not specified it will be public to all.

```
gaffer.federatedstore.<GraphId1>.auths=public,private

```

or via the `AddGraph` operation, by default if authorisations is not specified it is private to the user that added it to FederatedStore.

${ADD_SECURE_GRAPH_SNIPPET}

or through the rest service with json, by default if authorisations is not specified it is private to the user that added it to FederatedStore.

```json
${addSecureGraphJson}
```