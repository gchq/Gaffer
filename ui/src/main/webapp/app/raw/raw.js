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

angular.module('app').factory('raw', ['$http', 'settings', function($http, settings){
    var raw = {};
    raw.results = {entities: [], edges: []};

    var updateResultsListener;
    var updateScope;
    raw.initialise = function(newUpdateResultsListener, newUpdateScope) {
        raw.loadSchema();
        updateResultsListener = newUpdateResultsListener;
        updateScope = newUpdateScope;
    };

    raw.loadSchema = function() {
        var schema;
        raw.schema = {};
        updateSchemaVertices();
        $http.get(settings.restUrl + "/graph/schema")
             .success(function(data){
                raw.schema = data;
                updateSchemaVertices();
             })
             .error(function(arg) {
                console.log("Unable to load schema: " + arg);
             });
    };

    raw.execute = function(operationChain, onSuccess) {
        var queryUrl = settings.restUrl + "/graph/doOperation";
        if(!queryUrl.startsWith("http")) {
            queryUrl = "http://" + queryUrl;
        }

        if(!onSuccess) {
            onSuccess = updateResults;
        }

        raw.loading = true;
        $.ajax({
            url: queryUrl,
            type: "POST",
            data: operationChain,
            dataType: "json",
            contentType: "application/json",
            accept: "application/json",
            success: function(results){
                raw.loading = false;
                onSuccess(results);
                updateScope();
            },
            error: function(xhr, status, err) {
                console.log(queryUrl, status, err);
                alert("Error: " + xhr.statusCode().responseText);
                raw.loading = false;
                updateScope();
            }
       });
    }

    raw.clearResults = function() {
        raw.results = {entities: [], edges: []};
    }

    raw.entityProperties = function(entity) {
        if(Object.keys(raw.schema.entities[entity].properties).length) {
            return raw.schema.entities[entity].properties;
        }

        return undefined;
    }

    raw.edgeProperties = function(edge) {
        if(Object.keys(raw.schema.edges[edge].properties).length) {
            return raw.schema.edges[edge].properties;
        }

        return undefined;
    }

    raw.functions = function(group, property, onSuccess) {
        var type;
        if(raw.schema.entities[group]) {
            type = raw.schema.entities[group].properties[property];
        } else if(raw.schema.edges[group]) {
           type = raw.schema.edges[group].properties[property];
       }

       var className = "";
       if(type) {
         className = raw.schema.types[type].class;
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

    raw.functionParameters = function(functionClassName, onSuccess) {
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
                    raw.results.entities.push(element);
                } else if(element.source !== undefined && element.source !== ''
                && element.destination !== undefined && element.destination !== '') {
                    raw.results.edges.push(element);
                }
            }
        }

        updateResultsListener();
    }

    var updateSchemaVertices = function() {
        var vertices = [];
        if(raw.schema) {
            for(var i in raw.schema.entities) {
                if(vertices.indexOf(raw.schema.entities[i].vertex) == -1) {
                    vertices.push(raw.schema.entities[i].vertex);
                }
            }
            for(var i in raw.schema.edges) {
                if(vertices.indexOf(raw.schema.edges[i].source) == -1) {
                    vertices.push(raw.schema.edges[i].source);
                }
                if(vertices.indexOf(raw.schema.edges[i].destination) == -1) {
                    vertices.push(raw.schema.edges[i].destination);
                }
            }
        }

        raw.schemaVertices = vertices;
    }

    return raw;
} ]);