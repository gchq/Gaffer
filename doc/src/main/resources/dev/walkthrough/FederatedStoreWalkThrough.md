${HEADER}

${CODE_LINK}

This example explains how to create a FederatedStore, add additional graphs and call operations against the FederatedStore.
The FederatedStore encapsulates a collection of sub-graphs and executes operations against them and returns a results just like it was a single graph.

#### Configuration

To create a FederatedStore you need to initialise the store with a graph name and a properties file.

${FEDERATED_STORE_SNIPPET}

#### Adding Graphs

To load a graph into the FederatedStore you need to provide three things.
    * GraphID
    * Graph Schema
    * Graph Properties file

You can't add a graph using a graphID already in use (That's not adding that's replacing) you will need to remove the old GraphId first.

Either through the FederatedStore properties file...
```
gaffer.store.class=uk.gov.gchq.gaffer.federatedstore.FederatedStore
gaffer.store.properties.class=uk.gov.gchq.gaffer.store.StoreProperties

gaffer.federatedstore.<GraphID>.properties=<path/to/properties>
gaffer.federatedstore.<GraphID>.schema=<path/to/schema>
```

or through the AddGraph operation.

${ADD_GRAPH_SNIPPET}

or through the rest service with json.

```
{
  "class" : "uk.gov.gchq.gaffer.federatedstore.operation.AddGraph",
  "graphId" : "AnotherGraph",
  "properties" : {
    "gaffer.store.class" : "uk.gov.gchq.gaffer.mapstore.SingleUseMapStore",
    "gaffer.store.mapstore.map.ingest.buffer.size" : "5",
    "gaffer.store.properties.class" : "uk.gov.gchq.gaffer.mapstore.MapStoreProperties"
  },
  "schema" : {
    "edges" : {
      ...
    },
    "entities" : {
      ...
    },
    "types" : {
      ...
    }
  }
}
```

#### Removing Graphs

To remove a graph from the FederatedStore (Does not delete the graph, just removes it from the scope) it is even easier you only need to know the graphID.

${REMOVE_GRAPH_SNIPPET}

or through the rest service with json.

```
{
  "class":"uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph",
  "graphId":"AnotherGraph"
}
```

#### Getting all the GraphID's

To get a list of all the sub-graphs within the FederatedStore you can perform the following GetAllGraphID operation.

${GETALLGRAPHID_SNIPPET}

or through the rest service with json.

```
{"class":"uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphID"}
```

#### Performing Operations

Running operations against the FederatedStore is exactly same as running operations against any other store.
Behind the scenes the FederatedStore sends out operations/operation chains to the sub-graphs to be executed and returns back a single response.

${ADD_ELEMENTS_SNIPPET}

${GET_ELEMENTS_SNIPPET}

AddElements operations is a special case, and only adds elements to graphs when the edge or entity groupID is known by the graph.

#### Failed Execution
If the execution against a graph fails a OperationException is thrown, unless the operation is an instance of Options and "skipFailedFederatedStoreExecute" is set to true. In that situation the FederatedStore will continue onto the next graph.




