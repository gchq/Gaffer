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

angular.module('app').controller('AppController',
    [ '$scope', '$mdDialog', '$http', '$location', 'settings', 'graph', 'raw', 'table',
    function($scope, $mdDialog, $http, $location, settings, graph, raw, table){

    $scope.settings = settings;
    $scope.rawData = raw;
    $scope.graph = graph;
    $scope.table = table;
    $scope.operations = [];

    $scope.graphData = {entities: {}, edges: {}};

    $scope.selectedElementTabIndex = 0;
    $scope.selectedEntities = {};
    $scope.selectedEdges = {};

    $scope.buildQueryStep = 0;
    $scope.expandEntities = [];
    $scope.expandEdges = [];
    $scope.expandEntitiesContent = {};
    $scope.expandEdgesContent = {};
    $scope.inOutFlag = "BOTH";
    $scope.editingOperations = false;

    $scope.buildQuery = function() {
        $scope.openGraph();
        $scope.showBuildQuery = true;
        $scope.buildQueryTab = 0;
    }

    $scope.goToStep = function(stepId) {
        $scope.buildQueryStep = stepId - 1;
        if(stepId > 2) {
            executeBuildQueryCounts();
        }
    }

    $scope.redraw = function() {
        if($scope.showGraph) {
            $scope.selectedEntities = {};
            $scope.selectedEdges = {};
            graph.redraw();
       }
    };

    $scope.initialise = function() {
        $scope.openGraph();

        var updateResultsListener = function() {
            updateGraphData(raw.results);
            table.update(raw.results);
            if(!$scope.$$phase) {
               $scope.$apply();
            }
        }

        var updateScope = function() {
            if(!$scope.$$phase) {
               $scope.$apply();
            }
        }

        raw.initialise(updateResultsListener, updateScope);
    };

    function addSeedDialogController($scope, $mdDialog) {
        $scope.addSeedCancel = function() {
          $mdDialog.cancel();
        };

        $scope.addSeedAdd = function() {
          try {
             JSON.parse($scope.addSeedVertex);
          } catch(err) {
             // Try adding quotes
             $scope.addSeedVertex = "\"" + $scope.addSeedVertex + "\"";

             try {
               JSON.parse($scope.addSeedVertex);
             } catch(err) {
               alert("Error: vertex is not valid - " + $scope.addSeedVertex);
             }
          }

          var seed = {vertexType: $scope.addSeedVertexType, vertex: $scope.addSeedVertex};
           $scope.addSeedVertexType = '';
           $scope.addSeedVertex = '';
          $mdDialog.hide(seed);
        };
      }

    $scope.addSeedPrompt = function(ev) {
        $mdDialog.show({
              scope: $scope,
              preserveScope: true,
              controller: addSeedDialogController,
              templateUrl: 'app/graph/addSeedDialog.html',
              parent: angular.element(document.body),
              targetEvent: ev,
              clickOutsideToClose:true,
              fullscreen: $scope.customFullscreen
            })
            .then(function(seed) {
              $scope.addSeed(seed.vertexType, seed.vertex);
            });
      };

    var getVertexTypeFromEntityGroup = function(group) {
        for(var entityGroup in raw.schema.entities) {
            if(entityGroup === group) {
                return raw.schema.entities[entityGroup].vertex;
            }
        }
    }

    var getVertexTypesFromEdgeGroup = function(group) {
        for(var edgeGroup in raw.schema.edges) {
            if(edgeGroup === group) {
               return [raw.schema.edges[edgeGroup].source, raw.schema.edges[edgeGroup].destination];
            }
        }
    }

    var updateRelatedEntities = function() {
        $scope.relatedEntities = [];
        for(id in $scope.selectedEntities) {
            var vertexType = $scope.selectedEntities[id][0].vertexType;
            for(var entityGroup in raw.schema.entities) {
                var entity = raw.schema.entities[entityGroup];
                if(entity.vertex === vertexType
                    && $scope.relatedEntities.indexOf(entityGroup) === -1) {
                    $scope.relatedEntities.push(entityGroup);
                }
            }
        }
    }

    var updateRelatedEdges = function() {
        $scope.relatedEdges = [];
        for(id in $scope.selectedEntities) {
            var vertexType = $scope.selectedEntities[id][0].vertexType;
            for(var edgeGroup in raw.schema.edges) {
                var edge = raw.schema.edges[edgeGroup];
                if((edge.source === vertexType || edge.destination === vertexType)
                    && $scope.relatedEdges.indexOf(edgeGroup) === -1) {
                    $scope.relatedEdges.push(edgeGroup);
                }
            }
        }
    }

    $scope.addSeed = function(vertex, value) {
        graph.addSeed(vertex, value);
    }

    arrayContainsValue = function(arr, value) {
        var jsonValue = JSON.stringify(value);
        for(var i in arr) {
            if(JSON.stringify(arr[i]) === jsonValue) {
                return true;
            }
        }

        return false;
    }

    $scope.addFilterFunction = function(expandElementContent, element) {
        if(!expandElementContent[element]) {
            expandElementContent[element] = {};
        }

        if(!expandElementContent[element].filters) {
            expandElementContent[element].filters = [];
        }

        expandElementContent[element].filters.push({});
    }

    createBuildQueryOperation = function() {
        var operation = createOperation();
        var jsonVertex;
        for(var vertex in $scope.selectedEntities) {
            try {
               jsonVertex = JSON.parse(vertex);
            } catch(err) {
               jsonVertex = vertex;
            }
            operation.seeds.push({
                      "class": "uk.gov.gchq.gaffer.operation.data.EntitySeed",
                      "vertex": jsonVertex
                   });
        }

        for(var i in $scope.expandEntities) {
            var entity = $scope.expandEntities[i];
            operation.view.entities[entity] = {groupBy: []};

            var filterFunctions = convertFilterFunctions($scope.expandEntitiesContent[entity], raw.schema.entities[entity]);
            if(filterFunctions.length > 0) {
                operation.view.entities[entity].preAggregationFilterFunctions = filterFunctions;
            }
        }

        for(var i in $scope.expandEdges) {
            var edge = $scope.expandEdges[i];
            operation.view.edges[edge] = {groupBy: []};

            var filterFunctions = convertFilterFunctions($scope.expandEdgesContent[edge], raw.schema.edges[edge]);
            if(filterFunctions.length > 0) {
                operation.view.edges[edge].preAggregationFilterFunctions = filterFunctions;
            }
        }

        operation.includeIncomingOutGoing = $scope.inOutFlag;
        return operation;
    }

    var convertFilterFunctions = function(expandElementContent, elementDefinition) {
        var filterFunctions = [];
        if(expandElementContent && expandElementContent.filters) {
            for(var index in expandElementContent.filters) {
                var filter = expandElementContent.filters[index];
                if(filter.property && filter['function']) {
                    var functionJson = {
                        "function": {
                            class: filter['function']
                        },
                        selection: [ filter.property ]
                    };

                    for(var i in filter.availableFunctionParameters) {
                        if(filter.parameters[i]) {
                            functionJson["function"][filter.availableFunctionParameters[i]] = JSON.parse(filter.parameters[i]);
                        }
                    }

                    filterFunctions.push(functionJson);
                }
            }
        }
        return filterFunctions;
    }

    $scope.clearResults = function() {
        raw.clearResults();
        $scope.selectedEntities = {};
        $scope.selectedEdges = {};
        $scope.graphData = {entities: {}, edges: {}};
        table.clear();
        graph.clear();
    }

    $scope.updateGraph = function() {
        graph.update($scope.graphData);
    }

    var parseVertex = function(vertex) {
        try {
             JSON.parse(vertex);
        } catch(err) {
             // Try adding quotes
             vertex = "\"" + vertex + "\"";
        }

        return vertex;
    }

    var updateGraphData = function(results) {
        $scope.graphData = {entities: {}, edges: {}};
        for (var i in results.entities) {
            var entity = clone(results.entities[i]);
            entity.vertex = parseVertex(entity.vertex);
            var id = entity.vertex;
            entity.vertexType = getVertexTypeFromEntityGroup(entity.group);
            if(id in $scope.graphData.entities) {
                if(!arrayContainsValue($scope.graphData.entities[id], entity)) {
                    $scope.graphData.entities[id].push(entity);
                }
            } else {
                $scope.graphData.entities[id] = [entity];
            }
        }

        for (var i in results.edges) {
            var edge = clone(results.edges[i]);
            edge.source = parseVertex(edge.source);
            edge.destination = parseVertex(edge.destination);

            var vertexTypes = getVertexTypesFromEdgeGroup(edge.group);
            edge.sourceType = vertexTypes[0];
            edge.destinationType = vertexTypes[1];
            var id = edge.source + "|" + edge.destination + "|" + edge.directed + "|" + edge.group;
            if(id in $scope.graphData.edges) {
                if(!arrayContainsValue($scope.graphData.edges[id], edge)) {
                    $scope.graphData.edges[id].push(edge);
                }
            } else {
                $scope.graphData.edges[id] = [edge];
            }
        }
        $scope.updateGraph();
    }

    var clone = function(obj) {
        return JSON.parse(JSON.stringify(obj));
    }

    var createOperation = function() {
        return {
            class: "uk.gov.gchq.gaffer.operation.impl.get.GetElements",
            resultLimit:  settings.resultLimit,
            deduplicate: true,
            seeds: [],
            view: {
                entities: {},
                edges: {}
            }
        };
    }

    $scope.addOperation = function() {
        $scope.operations.push(createOperation());
    };

    $scope.resetBuildQuery = function() {
        $scope.expandEdges = [];
        $scope.expandEntities = [];
        $scope.showBuildQuery = false;
        $scope.buildQueryStep = 0;
        $scope.expandQueryCounts = undefined;
        $scope.expandEntitiesContent = {};
        $scope.expandEdgesContent = {};
    };

    $scope.executeBuildQuery = function() {
        var operation = createBuildQueryOperation();
        $scope.operations.push(operation);
        $scope.resetBuildQuery();
        raw.execute(JSON.stringify({operations: [operation]}));
    };

    var createCountOperation = function() {
        return {
            class: "uk.gov.gchq.gaffer.operation.impl.CountGroups",
            limit: (settings.resultLimit - 1)
        };
    }

    var executeBuildQueryCounts = function() {
        var operations = {operations: [createBuildQueryOperation(), createCountOperation()]};
        var onSuccess = function(data) {
            if(!data.limitHit) {
                var total = 0;
                for(var i in data.entityGroups) {
                    total += data.entityGroups[i];
                }
                for(var i in data.edgeGroups) {
                    total += data.edgeGroups[i];
                }
                data.total = total;
            }
            $scope.expandQueryCounts = data;
        }
        $scope.expandQueryCounts = undefined;
        raw.execute(JSON.stringify(operations), onSuccess);
    };

    $scope.executeAll = function() {
        $scope.clearResults();
        $scope.resetBuildQuery();
       for(var i in $scope.operations) {
           raw.execute(JSON.stringify({operations: [$scope.operations[i]]}));
       }
    }

  graph.onGraphElementSelect(function(element){
     $scope.selectedElementTabIndex = 0;
      var _id = element.id();
      for (var id in $scope.graphData.entities) {
            if(_id == id) {
                $scope.selectedEntities[id] = $scope.graphData.entities[id];
                $scope.selectedEntitiesCount = Object.keys($scope.selectedEntities).length;
                updateRelatedEntities();
                updateRelatedEdges();
                if(!$scope.$$phase) {
                    $scope.$apply();
                  }
                return;
            }
        };
      for (var id in $scope.graphData.edges) {
          if(_id == id) {
              $scope.selectedEdges[id] = $scope.graphData.edges[id];
              $scope.selectedEdgesCount = Object.keys($scope.selectedEdges).length;
              if(!$scope.$$phase) {
                  $scope.$apply();
                }
              return;
          }
      };

      $scope.selectedEntities[_id] = [{vertexType: element.data().vertexType, vertex: _id}];
      $scope.selectedEntitiesCount = Object.keys($scope.selectedEntities).length;
      updateRelatedEntities();
      updateRelatedEdges();

      if(!$scope.$$phase) {
        $scope.$apply();
      }
  });

  graph.onGraphElementUnselect(function(element){
      $scope.selectedElementTabIndex = 0;
      if(element.id() in $scope.selectedEntities) {
        delete $scope.selectedEntities[element.id()];
        $scope.selectedEntitiesCount = Object.keys($scope.selectedEntities).length;
        updateRelatedEntities();
        updateRelatedEdges();
      } else if(element.id() in $scope.selectedEdges) {
        delete $scope.selectedEdges[element.id()];
        $scope.selectedEdgesCount = Object.keys($scope.selectedEdges).length;
      }

      $scope.$apply();
  });

  $scope.toggle = function(item, list) {
    var idx = list.indexOf(item);
    if(idx > -1) {
        list.splice(idx, 1);
    } else {
        list.push(item);
    }
  }

  $scope.exists = function(item, list) {
    return list.indexOf(item) > -1;
  }

  $scope.onSelectedPropertyChange = function(group, selectedElement) {
    raw.functions(group, selectedElement.property, function(data) {
        selectedElement.availableFunctions = data
        $scope.$apply();
    });
    selectedElement.function = '';
  }

  $scope.onSelectedFunctionChange = function(group, selectedElement) {
    raw.functionParameters(selectedElement['function'], function(data) {
        selectedElement.availableFunctionParameters = data;
        $scope.$apply();
    });

    var elementDef = raw.schema.entities[group];
    if(!elementDef) {
         elementDef = raw.schema.edges[group];
    }
    var propertyClass = raw.schema.types[elementDef.properties[selectedElement.property]].class;
    if("java.lang.String" !== propertyClass
        && "java.lang.Boolean" !== propertyClass
        && "java.lang.Integer" !== propertyClass) {
        selectedElement.propertyClass = propertyClass;
    }

    selectedElement.parameters = {};
  }

  var switchTabs = function(tabFlag) {
      $scope.showRaw = false;
      $scope.showGraph = false;
      $scope.showResultsTable = false;
      $scope.showSettings = false;
      $scope[tabFlag] = true;
  };

  $scope.openRaw = function() {
     switchTabs('showRaw');
  };

  $scope.openGraph = function() {
      if(!$scope.graphDataGraphLoaded) {
          graph.load()
               .then(function(){
                 $scope.graphDataGraphLoaded = true;
               });
       }
     switchTabs('showGraph');
  };

  $scope.openResultsTable = function() {
    switchTabs('showResultsTable');
  };

  $scope.openSettings = function() {
     switchTabs('showSettings');
  };

  $scope.editOperations = function() {
    $scope.operationsForEdit = [];
    for(var i in $scope.operations) {
        $scope.operationsForEdit.push(JSON.stringify($scope.operations[i], null, 2));
    }
    $scope.editingOperations = true;
  }

  $scope.saveOperations = function() {
      $scope.operations = [];
      for(var i in $scope.operationsForEdit) {
          try {
            $scope.operations.push(JSON.parse($scope.operationsForEdit[i]));
          } catch(e) {
            console.log('Invalid json: ' + $scope.operationsForEdit[i]);
          }
      }
      $scope.editingOperations = false;
   }

  $scope.initialise();
} ]);
