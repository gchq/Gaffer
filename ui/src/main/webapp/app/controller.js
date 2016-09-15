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

angular.module('app').controller('AppController', [ '$scope', '$mdDialog', '$http', '$location', 'settings', 'graph', 'rawData', function($scope, $mdDialog, $http, $location, settings, graph, rawData){
    $scope.settings = settings;
    $scope.rawData = rawData;
    $scope.graph = graph;
    $scope.operations = [];

    $scope.graphData = {entities: {}, edges: {}};
    $scope.tableData = {entities: {}, edges: {}};

    $scope.selectedElementTabIndex = 0;
    $scope.selectedEntities = {};
    $scope.selectedEdges = {};

    $scope.extendQueryStep = 0;
    $scope.expandEntities = [];
    $scope.expandEdges = [];
    $scope.expandEntitiesContent = {};
    $scope.expandEdgesContent = {};
    $scope.inOutFlag = "BOTH";
    $scope.editingOperations = false;

    $scope.extendQuery = function() {
        $scope.openGraph();
        $scope.showExtendQuery = true;
        $scope.extendQueryTab = 0;
    }

    $scope.goToStep = function(stepId) {
        $scope.extendQueryStep = stepId - 1;
        if(stepId > 2) {
            executeExtendQueryCounts();
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
            updateGraphData();
            updateTableData();
        }

        rawData.initialise(updateResultsListener);
    };

    function addSeedDialogController($scope, $mdDialog) {
        $scope.addSeedCancel = function() {
          $mdDialog.cancel();
        };

        $scope.addSeedAdd = function() {
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
        for(var entityGroup in rawData.schema.entities) {
            if(entityGroup === group) {
                return rawData.schema.entities[entityGroup].vertex;
            }
        }
    }

    var getVertexTypesFromEdgeGroup = function(group) {
        for(var edgeGroup in rawData.schema.edges) {
            if(edgeGroup === group) {
               return [rawData.schema.edges[edgeGroup].source, rawData.schema.edges[edgeGroup].destination];
            }
        }
    }

    var updateRelatedEntities = function() {
        $scope.relatedEntities = [];
        for(id in $scope.selectedEntities) {
            var vertexType = $scope.selectedEntities[id][0].vertexType;
            for(var entityGroup in rawData.schema.entities) {
                var entity = rawData.schema.entities[entityGroup];
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
            for(var edgeGroup in rawData.schema.edges) {
                var edge = rawData.schema.edges[edgeGroup];
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

    createExtendQueryOperation = function() {
        var operation = createOperation();
        for(var vertex in $scope.selectedEntities) {
            operation.seeds.push({
                   "gaffer.operation.data.EntitySeed": {
                      "vertex": vertex
                   }
              });
        }

        for(var i in $scope.expandEntities) {
            var entity = $scope.expandEntities[i];
            var filterFunctions = convertFilterFunctions($scope.expandEntitiesContent[entity]);
            operation.view.entities[entity] = {preAggregationFilterFunctions: filterFunctions};
        }
        for(var i in $scope.expandEdges) {
            var edge = $scope.expandEdges[i];
            var filterFunctions = convertFilterFunctions($scope.expandEdgesContent[edge]);
            operation.view.edges[edge] = {preAggregationFilterFunctions: filterFunctions};
        }

        operation.includeIncomingOutGoing = $scope.inOutFlag;
        return operation;
    }

    var convertFilterFunctions = function(expandElementContent) {
        var filterFunctions = [];
        if(expandElementContent) {
        //for(var i in expandElementContent) {
            //var filters = expandElementContent[i];
            var filter = expandElementContent;
            if(filter.property && filter['function']) {
                var functionJson = {
                    "function": {
                        class: filter['function']
                    },
                    selection: [{ key: filter.property }]
                };

                for(var i in filter.availableFunctionParameters) {
                    if(filter.parameters[i]) {
                        functionJson["function"][filter.availableFunctionParameters[i]] = filter.parameters[i];
                    }
                }

                filterFunctions.push(functionJson);
            }
        //}
        }
        return filterFunctions;
    }

    $scope.clearResults = function() {
        rawData.clearResults();
        $scope.selectedEntities = {};
        $scope.selectedEdges = {};
        $scope.graphData = {entities: {}, edges: {}};
        $scope.tableData = {entities: {}, edges: {}};
        graph.clear();
    }

    $scope.updateGraph = function() {
        graph.update($scope.graphData);
    }

    var updateGraphData = function() {
        $scope.graphData = {entities: {}, edges: {}};
        for (var i in rawData.results.entities) {
            var entity = clone(rawData.results.entities[i]);
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

        for (var i in rawData.results.edges) {
            var edge = clone(rawData.results.edges[i]);
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

    var updateTableData = function() {
        $scope.tableData = {entities: {}, edges: {}};
        for (var i in rawData.results.entities) {
            var entity = rawData.results.entities[i];
            if(!$scope.tableData.entities[entity.group]) {
                $scope.tableData.entities[entity.group] = [];
            }
            $scope.tableData.entities[entity.group].push(entity);
        }

        for (var i in rawData.results.edges) {
            var edge = rawData.results.edges[i];
            if(!$scope.tableData.edges[edge.group]) {
                $scope.tableData.edges[edge.group] = [];
            }
            $scope.tableData.edges[edge.group].push(edge);
        }

        if(!$scope.$$phase) {
            $scope.$apply();
          }
    }

    var createOperation = function() {
        return {
            class: "gaffer.operation.impl.get.GetRelatedElements",
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

    $scope.resetExtendQuery = function() {
        $scope.expandEdges = [];
        $scope.expandEntities = [];
        $scope.showExtendQuery = false;
        $scope.extendQueryStep = 0;
        $scope.expandQueryCounts = undefined;
        $scope.expandEntitiesContent = {};
        $scope.expandEdgesContent = {};
    };

    $scope.executeExtendQuery = function() {
        var operation = createExtendQueryOperation();
        $scope.operations.push(operation);
        $scope.resetExtendQuery();
        rawData.execute(JSON.stringify({operations: [operation]}));
    };

    var createCountOperation = function() {
        return {
            class: "gaffer.operation.impl.CountGroups",
            limit: (settings.resultLimit - 1)
        };
    }

    var executeExtendQueryCounts = function() {
        var operations = {operations: [createExtendQueryOperation(), createCountOperation()]};
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
        rawData.execute(JSON.stringify(operations), onSuccess);
    };

    $scope.executeAll = function() {
        $scope.clearResults();
        $scope.resetExtendQuery();
       for(var i in $scope.operations) {
           rawData.execute(JSON.stringify({operations: [$scope.operations[i]]}));
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
        $scope.selectedEdgesCount = Object.keys($scope.selectedEdges).lengt;
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
    rawData.functions(group, selectedElement.property, function(data) {
        selectedElement.availableFunctions = data
        $scope.$apply();
    });
    selectedElement.function = '';
  }

  $scope.onSelectedFunctionChange = function(group, selectedElement) {
    rawData.functionParameters(selectedElement['function'], function(data) {
        selectedElement.availableFunctionParameters = data;
        $scope.$apply();
    });
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