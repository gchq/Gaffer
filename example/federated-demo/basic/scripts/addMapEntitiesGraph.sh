curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{
    "class": "uk.gov.gchq.gaffer.federatedstore.operation.AddGraph",
    "graphId": "mapEntities",
    "storeProperties": {
       "gaffer.store.class":"uk.gov.gchq.gaffer.mapstore.MapStore"
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
