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

Federated Demo
=============

## Deployment
Assuming you have Java 8, Maven and Git installed, you can build and run the latest version of the road traffic demo by doing the following:

```bash
# Clone the Gaffer repository, to reduce the amount you need to download this will only clone the master branch with a depth of 1 so there won't be any history.
git clone --depth 1 --branch master https://github.com/gchq/Gaffer.git
cd Gaffer

# Run the start script. This will download several maven dependencies such as tomcat.
# If you have a snapshot version and want to build all dependencies first then add -am as a script argument
./example/federated-demo/scripts/start.sh
```

The rest api will be deployed to localhost:8080/rest.

To add a Map graph, execute this operation:

```json
{
    "class": "uk.gov.gchq.gaffer.federatedstore.operation.AddGraph",
    "graphId": "mapGraph",
    "storeProperties": {
        "gaffer.store.class": "uk.gov.gchq.gaffer.mapstore.MapStore",
        "gaffer.store.mapstore.static": true
    },
    "schema": {
        "edges": {
            "BasicEdge": {
                "source": "vertex",
                "destination": "vertex",
                "directed": "true",
                "properties": {
                    "count": "count"
                }
            }
        },
        "types": {
            "vertex": {
                "class": "java.lang.String"
            },
            "count": {
                "class": "java.lang.Integer",
                "aggregateFunction": {
                    "class": "uk.gov.gchq.koryphe.impl.binaryoperator.Sum"
                }
            },
            "true": {
                "description": "A simple boolean that must always be true.",
                "class": "java.lang.Boolean",
                "validateFunctions": [
                    {
                        "class": "uk.gov.gchq.koryphe.impl.predicate.IsTrue"
                    }
                ]
            }
        }
    },
    "isPublic": true
}
```

And to add an Accumulo graph execute this:

```json
{
    "class": "uk.gov.gchq.gaffer.federatedstore.operation.AddGraph",
    "graphId": "accumuloGraph",
    "storeProperties": {
        "gaffer.store.class": "uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore",
        "accumulo.instance": "someInstanceName",
        "accumulo.zookeepers": "aZookeeper",
        "accumulo.user": "user01",
        "accumulo.password": "password"
    },
    "schema": {
        "entities": {
            "BasicEntity": {
                "vertex": "vertex",
                "properties": {
                    "count": "count"
                }
            }
        },
        "types": {
            "vertex": {
                "class": "java.lang.String"
            },
            "count": {
                "class": "java.lang.Integer",
                "aggregateFunction": {
                    "class": "uk.gov.gchq.koryphe.impl.binaryoperator.Sum"
                }
            },
            "true": {
                "description": "A simple boolean that must always be true.",
                "class": "java.lang.Boolean",
                "validateFunctions": [
                    {
                        "class": "uk.gov.gchq.koryphe.impl.predicate.IsTrue"
                    }
                ]
            }
        }
    },
    "isPublic": true
}
```


To add some example data execute this json in /graph/operations/execute:

```json
{
  "class" : "uk.gov.gchq.gaffer.operation.impl.add.AddElements",
  "input" : [ {
    "group" : "BasicEntity",
    "vertex" : "1",
    "properties" : {
      "count" : 1
    },
    "class" : "uk.gov.gchq.gaffer.data.element.Entity"
  }, {
    "group" : "BasicEdge",
    "source" : "1",
    "destination" : "2",
    "directed" : true,
    "properties" : {
      "count" : 1
    },
    "class" : "uk.gov.gchq.gaffer.data.element.Edge"
  } ]
}
```

Here is an example of an advanced federated operation chain:

```json
{
   "class": "uk.gov.gchq.gaffer.operation.OperationChain",
   "operations": [
      {
         "class": "uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain",
         "operationChain": {
            "operations": [
               {
                  "class": "uk.gov.gchq.gaffer.operation.impl.get.GetAllElements"
               },
               {
                  "class": "uk.gov.gchq.gaffer.operation.impl.Limit",
                  "resultLimit": 1,
                  "truncate": true
               }
            ]
         }
      },
      {
         "class": "uk.gov.gchq.gaffer.operation.impl.Limit",
         "resultLimit": 1,
         "truncate": true
      }
   ]
}
```

To fetch the merged schemas you can run:

```json
{
   "class": "uk.gov.gchq.gaffer.store.operation.GetSchema",
   "compact": false
}
```

To fetch just a the schema for the mapGraph you can add an option:
```json
{
    "class": "uk.gov.gchq.gaffer.store.operation.GetSchema",
    "compact": false,
    "options": {
        "gaffer.federatedstore.operation.graphIds": "mapGraph"
    }
}
```