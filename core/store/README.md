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

This page has been copied from the Store module README. To make any changes please update that README and this page will be automatically updated when the next release is done.


# Store

This Store module defines the API for Store implementations. The abstract Store class handles Operations by delegating the Operations to their registered handlers.

Store implementations need to define a set of StoreTraits. These traits tells Gaffer the abilities the Store has. For example the ability to aggregate or filter elements.

When implementing a Store, the main task is to write handlers for the operations your Store chooses to support. This can be tricky, but there is a Store Integration test suite that should be used by all Store implementations to validate these operation handlers. When writing these handlers you should implement OperationHandler or OutputOperationHandler depending on whether the operation has an output.

In addition to OperationHandlers the other large part of this module is the Schema. The Schema is what defines what is in the Graph and how it should be persisted, compacted/summarised and validated.


## Customisable Operations

Some operations are not available by default and you will need to manually configure them.

These customisable operations can be added to you Gaffer graph by providing config
in one or more operation declaration json files.

## Named Operations
Named Operations depends on the Cache service being active at runtime.
In order for the cache service to run you must select your desired
implementation. You do this by adding another line to the store.properties
file:
```
gaffer.cache.service.class=uk.gov.gchq.gaffer.cache.impl.HashMapCacheService
```

To find out more about the different cache services on offer, see the
Cache Library README.

### ScoreOperationChain

Variables:
- opScores - required map of operation scores. These are the operation score values.
- authScores - required map of operation authorisation scores. These are the maximum scores allowed for a user with a given role.
- scoreResolvers - required (if using NamedOperations) list of score resolvers. These map operation class to its respective score resolver.

Example operation scores map:

```json
{ 
  "opScores": {
    "uk.gov.gchq.gaffer.operation.Operation": 1,
    "uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects": 0,
    "uk.gov.gchq.gaffer.operation.impl.get.GetAllElements": 3
  }
}
```

Example operation authorisation scores map:

```json
{
  "authScores": {
     "User": 4,
     "EnhancedUser": 10,
     "OtherUser": 6
  }
}
```

Example operation declarations json file:

```json
{
  "operations": [
    {
      "operation": "uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain",
      "handler": {
        "class": "uk.gov.gchq.gaffer.store.operation.handler.ScoreOperationChainHandler",
        "opScores": {
          "uk.gov.gchq.gaffer.operation.Operation": 2,
          "uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects": 0
        },
        "authScores": {
          "User": 4,
          "EnhancedUser": 10
        },
        "scoreResolvers": {
          "uk.gov.gchq.gaffer.named.operation.NamedOperation": {
            "class": "uk.gov.gchq.gaffer.store.operation.resolver.named.NamedOperationScoreResolver"
          }
        }
      }
    }
  ]
}
```
