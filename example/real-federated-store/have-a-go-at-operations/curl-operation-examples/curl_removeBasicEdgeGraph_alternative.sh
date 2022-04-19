curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{
"class": "removeGraph",
"graphId": "mapEdges"
}' 'http://localhost:8080/rest/graph/operations/execute'
