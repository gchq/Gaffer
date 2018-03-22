curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{
    "class": "uk.gov.gchq.gaffer.federatedstore.operation.AddGraph",
    "graphId": "accEntities",
    "storeProperties": {
         "gaffer.store.class":"uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore",
         "accumulo.instance":"someInstanceName",
         "accumulo.zookeepers":"aZookeeper",
         "accumulo.user":"user01",
         "accumulo.password":"password"
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
         }
       }
    },
    "isPublic": true
 }' 'http://localhost:8080/rest/v2/graph/operations/execute'
