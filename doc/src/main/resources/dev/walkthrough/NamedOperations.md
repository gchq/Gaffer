${HEADER}

${CODE_LINK}

This example explains how to configure your Gaffer Graph to allow named operations to be executed. 
Named operations enable encapsulation of an OperationChain into a new single NamedOperation.
The NamedOperation can be added to OperationChains and executed, just like
any other Operation. When run it executes the encapsulated OperationChain.
There are various possible uses for NamedOperations:
 * Making it simpler to run frequently used OperationChains
 * In a controlled way, allowing specific OperationChains to be run by a user that would not normally have permission to run them.

In addition to the NamedOperation there are a set of operations which manage named operations (AddNamedOperation, GetAllNamedOperations, DeleteNamedOperation).

#### Configuration
You will need to configure what cache to use for storing NamedOperations. 
There is one central cache service for Gaffer, so the same cache is used for named operations and the job tracker.
For example, to use the JCS cache service, add a dependency on the jcs-cache-service and set these store.properties:

```xml
<dependency>
    <groupId>uk.gov.gchq.gaffer</groupId>
    <artifactId>jcs-cache-service</artifactId>
    <version>[gaffer.version]</version>
</dependency>
```

```
gaffer.cache.service.class=uk.gov.gchq.gaffer.cache.impl.JcsCacheService

# Optionally provide custom cache properties
gaffer.cache.config.file=/path/to/config/file
```


#### Using Named Operations
OK, now for some examples of using NamedOperations.

We will use the same basic schema and data from the first developer walkthrough.

Start by creating your user instance and graph as you will have done previously:

${USER_SNIPPET}

${GRAPH_SNIPPET}

Then add a named operation to the cache with the AddNamedOperation operation:

${ADD_NAMED_OPERATION_SNIPPET}

Then create a NamedOperation and execute it

${CREATE_NAMED_OPERATION_SNIPPET}

${EXECUTE_NAMED_OPERATION_SNIPPET}

The results are:

```
${NAMED_OPERATION_RESULTS}
```

NamedOperations can take parameters, to allow the OperationChain executed to be configured. The parameter could be as
simple as specifying the resultLimit on a Limit operation, but specify a custom view to use in an operation, or the input to an operation.
When adding a NamedOperation with parameters the operation chain must be specified as a JSON string, with
parameter names enclosed '${' and '}'. For each parameter, a ParameterDetail object must be created which gives a description, a class type
and an optional default for the Parameter, and also indicates whether the parameter must be provided (ie. there is no default).

The following code adds a NamedOperation with a 'limitParam' parameter that allows the result limit for the OperationChain to be set:

${ADD_NAMED_OPERATION_WITH_PARAMETERS_SNIPPET}

A NamedOperation can then be created, with a value provided for the 'limitParam' parameter:

${CREATE_NAMED_OPERATION_WITH_PARAMETERS_SNIPPET}

and executed:

${EXECUTE_NAMED_OPERATION_WITH_PARAMETERS_SNIPPET}

giving these results:

```
${NAMED_OPERATION_WITH_PARAMETER_RESULTS}
```

Details of all available NamedOperations can be fetched using the GetAllNamedOperations operation:

${GET_ALL_NAMED_OPERATIONS_SNIPPET}

That gives the following results:

${ALL_NAMED_OPERATIONS}







