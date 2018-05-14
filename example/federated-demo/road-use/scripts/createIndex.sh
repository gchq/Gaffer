#!/usr/bin/env bash
curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d @example/federated-demo/road-use/json/4a_createIndexGraph.json 'http://localhost:8080/rest/v2/graph/operations/execute'
curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d @example/federated-demo/road-use/json/4b_createIndex.json 'http://localhost:8080/rest/v2/graph/operations/execute'

