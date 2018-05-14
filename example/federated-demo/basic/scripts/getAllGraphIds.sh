curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{ 
    "class": "uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds" 
 }' 'http://localhost:8080/rest/v2/graph/operations/execute'
