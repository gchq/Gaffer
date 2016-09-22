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

angular.module('app').factory('graph', [ '$q', function( $q ){
  var graphCy;
  var graph = {};

  graph.load = function() {
    var deferred = $q.defer();
    graphCy = cytoscape({
      container: $('#graphCy')[0],
      style: [
        {
          selector: 'node',
          style: {
            'content': 'data(label)',
            'text-valign': 'center',
            'background-color': 'data(color)',
            'font-size': 14,
            'color': '#fff',
            'text-outline-width':1,
            'width': 'data(radius)',
            'height': 'data(radius)'
          }
        },
        {
          selector: 'edge',
          style: {
            'curve-style': 'bezier',
            'label': 'data(group)',
            'line-color': '#79B623',
            'target-arrow-color': '#79B623',
            'target-arrow-shape': 'triangle',
            'font-size': 14,
            'color': '#fff',
            'text-outline-width':1,
            'width': 5
          }
        },
        {
          selector: ':selected',
          css: {
            'background-color': 'data(selectedColor)',
            'line-color': '#35500F',
            'target-arrow-color': '#35500F'
          }
        }
      ],
      layout: {
        name: 'concentric'
      },
      elements: [],
      ready: function(){
        deferred.resolve( this );
      }
    });

    graphCy.on('select', function(evt){
      fire('onGraphElementSelect', [evt.cyTarget]);
    });

    graphCy.on('unselect', function(evt){
      fire('onGraphElementUnselect', [evt.cyTarget]);
    });

    return deferred.promise;
  };

  graph.listeners = {};

  graph.update = function(results) {
    for (var id in results.entities) {
      var existingNodes = graphCy.getElementById(id);
      if(existingNodes.length > 0) {
        if(existingNodes.data().radius < 60) {
          existingNodes.data('radius', 60);
          existingNodes.data('color', '#337ab7');
          existingNodes.data('selectedColor', '#204d74');
        }
      } else {
        graphCy.add({
          group: 'nodes',
          data: {
            id: id,
            label: id,
            radius: 60,
            color: '#337ab7',
            selectedColor: '#204d74'
          },
          position: {
            x: 100,
            y: 100
          }
        });
      }
    }

    for (var id in results.edges) {
      var edge = results.edges[id][0];
      if(graphCy.getElementById(edge.source).length === 0) {
        graphCy.add({
          group: 'nodes',
          data: {
            id: edge.source,
            label: edge.source,
            radius: 20,
            color: '#888',
            selectedColor: '#444',
            vertexType: edge.sourceType
          },
          position: {
            x: 100,
            y: 100
          }
        });
      }

      if(graphCy.getElementById(edge.destination).length === 0) {
        graphCy.add({
          group: 'nodes',
          data: {
            id: edge.destination,
            label: edge.destination,
            radius: 20,
            color: '#888',
            selectedColor: '#444',
            vertexType: edge.destinationType
          },
          position: {
            x: 100,
            y: 100
          }
        });
      }

      if(graphCy.getElementById(id).length === 0) {
        graphCy.add({
          group: 'edges',
          data: {
            id: id,
            source: edge.source,
            target: edge.destination,
            group: edge.group,
            selectedColor: '#35500F',
          }
        });
      }
    }
    graph.redraw();
  }
  graph.onGraphElementSelect = function(fn){
    listen('onGraphElementSelect', fn);
  };

  graph.onGraphElementUnselect = function(fn){
    listen('onGraphElementUnselect', fn);
  };

  graph.clear = function(){
    while(graphCy.elements().length > 0) {
      graphCy.remove(graphCy.elements()[0]);
    }
  };

  graph.redraw = function() {
    graphCy.layout({name: 'concentric'});
  }

  graph.addSeed = function(vertexType, vertex){
    if(graphCy.getElementById(vertex).length === 0) {
      graphCy.add({
        group: 'nodes',
        data: {
          id: vertex,
          label: vertex,
          vertex: vertex,
          color: '#888',
          selectedColor: '#444',
          radius: 20,
          vertexType: vertexType
        },
        position: {
          x: 100,
          y: 100
        }
      });
    }
  };

  graph.selectAllNodes = function() {
    graphCy.filter('node').select();
  }

  function fire(e, args){
    var listeners = graph.listeners[e];

    for( var i = 0; listeners && i < listeners.length; i++ ){
      var fn = listeners[i];

      fn.apply( fn, args );
    }
  }

  function listen(e, fn){
    var listeners = graph.listeners[e] = graph.listeners[e] || [];

    listeners.push(fn);
  }
  return graph;
} ]);
