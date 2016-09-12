/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

angular.module('app').factory('rawData', ['$http', 'settings', function($http, settings){
    var rawData = {};
    rawData.results = {entities: [], edges: []};

    var updateResultsListener;
    rawData.initialise = function(newUpdateResultsListener) {
        rawData.loadSchema();
        updateResultsListener = newUpdateResultsListener;
    };

    rawData.loadSchema = function() {
        var schema;
        $http.get(settings.restUrl + "/graph/schema")
             .success(function(data){
                rawData.schema = data;
                updateSchemaVertices();
             })
             .error(function(arg) {
                console.log("Unable to load schema: " + arg);
             });
    };

    rawData.execute = function(operationChain, onSuccess) {
        var queryUrl = settings.restUrl + "/graph/doOperation";
        if(!queryUrl.startsWith("http")) {
            queryUrl = "http://" + queryUrl;
        }

        if(!onSuccess) {
            onSuccess = updateResults;
        }

        $.ajax({
            url: queryUrl,
            type: "POST",
            data: operationChain,
            dataType: "json",
            contentType: "application/json",
            accept: "application/json",
            success: onSuccess,
            error: function(xhr, status, err) {
                console.log(queryUrl, status, err);
            }
       });
    }

    rawData.clearResults = function() {
        rawData.results = {entities: [], edges: []};
    }

    rawData.entityProperties = function(entity) {
        if(Object.keys(rawData.schema.entities[entity].properties).length) {
            return rawData.schema.entities[entity].properties;
        }

        return undefined;
    }

    rawData.edgeProperties = function(edge) {
        if(Object.keys(rawData.schema.edges[edge].properties).length) {
            return rawData.schema.edges[edge].properties;
        }

        return undefined;
    }

    rawData.functions = function(group, property, onSuccess) {
        var type;
        if(rawData.schema.entities[group]) {
            type = rawData.schema.entities[group].properties[property];
        } else if(rawData.schema.edges[group]) {
           type = rawData.schema.edges[group].properties[property];
       }

       var className = "";
       if(type) {
         className = rawData.schema.types[type].class;
       }

       var queryUrl = settings.restUrl + "/graph/filterFunctions/" + className;
       if(!queryUrl.startsWith("http")) {
           queryUrl = "http://" + queryUrl;
       }

       $.ajax({
           url: queryUrl,
           type: "GET",
           accept: "application/json",
           success: onSuccess,
           error: function(xhr, status, err) {
               console.log(queryUrl, status, err);
           }
      });

       return [];
    }

    rawData.functionParameters = function(functionClassName, onSuccess) {
          var queryUrl = settings.restUrl + "/graph/serialisedFields/" + functionClassName;
          if(!queryUrl.startsWith("http")) {
              queryUrl = "http://" + queryUrl;
          }

          $.ajax({
              url: queryUrl,
              type: "GET",
              accept: "application/json",
              success: onSuccess,
              error: function(xhr, status, err) {
                  console.log(queryUrl, status, err);
              }
         });
    }

    var updateResults = function(results) {
        if(results) {
            for (var i in results) {
                var element = results[i];
                if(element.vertex !== undefined && element.vertex !== '') {
                    rawData.results.entities.push(element);
                } else if(element.source !== undefined && element.source !== ''
                && element.destination !== undefined && element.destination !== '') {
                    rawData.results.edges.push(element);
                }
            }
        }

        updateResultsListener();
    }

    var updateSchemaVertices = function() {
        var vertices = [];
        if(rawData.schema) {
            for(var i in rawData.schema.entities) {
                if(vertices.indexOf(rawData.schema.entities[i].vertex) == -1) {
                    vertices.push(rawData.schema.entities[i].vertex);
                }
            }
            for(var i in rawData.schema.edges) {
                if(vertices.indexOf(rawData.schema.edges[i].source) == -1) {
                    vertices.push(rawData.schema.edges[i].source);
                }
                if(vertices.indexOf(rawData.schema.edges[i].destination) == -1) {
                    vertices.push(rawData.schema.edges[i].destination);
                }
            }
        }

        rawData.schemaVertices = vertices;
    }

    return rawData;
} ]);