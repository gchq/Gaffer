#
# Copyright 2016 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module contains Python copies of Gaffer java classes
"""

import json


class ToJson:
    """
    Enables implementations to be converted to json via a toJson method
    """

    def __repr__(self):
        return json.dumps(self.toJson())

    def toJson(self):
        """
        Converts an object to a simple json dictionary
        """
        raise NotImplementedError('Use an implementation')


class ResultConverter:
    @staticmethod
    def to_elements(result):
        elements = []
        if result is not None and isinstance(result, list):
            for resultItem in result:
                if 'class' in resultItem:
                    if resultItem['class'] == 'gaffer.data.element.Entity':
                        element = Entity(resultItem['group'],
                                         resultItem['vertex'])
                    elif resultItem['class'] == 'gaffer.data.element.Edge':
                        element = Edge(resultItem['group'],
                                       resultItem['source'],
                                       resultItem['destination'],
                                       resultItem['directed'])
                    else:
                        raise TypeError(
                            'Element type is not recognised: ' + str(
                                resultItem))

                    if 'properties' in resultItem:
                        element.properties = resultItem['properties']
                    elements.append(element)
                else:
                    raise TypeError(
                        'Element type is not recognised: ' + str(resultItem))

        # Return the elements
        return elements

    @staticmethod
    def to_entity_seeds(result):
        entities_seeds = []
        if result is not None and isinstance(result, list):
            for resultItem in result:
                entities_seeds.append(EntitySeed(resultItem['vertex']))
        return entities_seeds


class ElementSeed(ToJson):
    def __repr__(self):
        return json.dumps(self.toJson())

    def toJson(self):
        raise NotImplementedError('Use either EntitySeed or EdgeSeed')


class EntitySeed(ElementSeed):
    def __init__(self, vertex):
        super().__init__()
        self.vertex = vertex

    def toJson(self):
        return {'gaffer.operation.data.EntitySeed': {'vertex': self.vertex}}


class EdgeSeed(ElementSeed):
    def __init__(self, source, destination, directed):
        super().__init__()
        self.source = source
        self.destination = destination
        self.directed = directed

    def toJson(self):
        return {'gaffer.operation.data.EdgeSeed': {
            'source': self.source,
            'destination': self.destination,
            'directed': self.directed}
        }


class Element(ToJson):
    def __init__(self, class_name, group, properties=None):
        super().__init__()
        if not isinstance(class_name, str):
            raise TypeError('ClassName must be a class name string')
        if not isinstance(group, str):
            raise TypeError('Group must be a string')
        if not isinstance(properties, dict) and properties is not None:
            raise TypeError('properties must be a dictionary or None')
        self.class_name = class_name
        self.group = group
        self.properties = properties

    def toJson(self):
        element = {'class': self.class_name, 'group': self.group}
        if self.properties is not None:
            element['properties'] = self.properties
        return element


class Entity(Element):
    def __init__(self, group, vertex, properties=None):
        super().__init__('gaffer.data.element.Entity', group, properties)
        self.vertex = vertex

    def toJson(self):
        entity = super().toJson()
        entity['vertex'] = self.vertex
        return entity


class Edge(Element):
    def __init__(self, group, source, destination, directed, properties=None):
        super().__init__('gaffer.data.element.Edge', group, properties)
        # Validate the arguments
        if not isinstance(directed, bool):
            raise TypeError('Directed must be a boolean')
        self.source = source
        self.destination = destination
        self.directed = directed

    def toJson(self):
        edge = super().toJson()
        edge['source'] = self.source
        edge['destination'] = self.destination
        edge['directed'] = self.directed
        return edge


class View(ToJson):
    def __init__(self, entities=None, edges=None):
        super().__init__()
        self.entities = entities
        self.edges = edges

    def toJson(self):
        view = {}
        if self.entities is not None:
            el_defs = {}
            for elDef in self.entities:
                el_defs[elDef.group] = elDef.toJson()
            view['entities'] = el_defs
        if self.edges is not None:
            el_defs = {}
            for elDef in self.edges:
                el_defs[elDef.group] = elDef.toJson()
            view['edges'] = el_defs
        return view


class ElementDefinition(ToJson):
    def __init__(self, group, transient_properties=None, filter_functions=None,
                 transform_functions=None):
        super().__init__()
        self.group = group
        self.transientProperties = transient_properties
        self.filterFunctions = filter_functions
        self.transformFunctions = transform_functions

    def toJson(self):
        element_def = {}
        if self.transientProperties is not None:
            props = {}
            for prop in self.transientProperties:
                props[prop.name] = prop.class_name
            element_def['transientProperties'] = props
        if self.filterFunctions is not None:
            funcs = []
            for func in self.filterFunctions:
                funcs.append(func.toJson())
            element_def['filterFunctions'] = funcs
        if self.transformFunctions is not None:
            funcs = []
            for func in self.transformFunctions:
                funcs.append(func.toJson())
            element_def['transformFunctions'] = funcs
        return element_def


class Property(ToJson):
    def __init__(self, name, class_name):
        super().__init__()
        if not isinstance(name, str):
            raise TypeError('Name must be a string')
        if not isinstance(class_name, str):
            raise TypeError('ClassName must be a class name string')
        self.name = name
        self.class_name = class_name

    def toJson(self):
        return {self.name: self.class_name}


class GafferFunction(ToJson):
    def __init__(self, class_name, function_fields=None):
        super().__init__()
        self.class_name = class_name
        self.functionFields = function_fields

    def toJson(self):
        function_context = {}
        function = {'class': self.class_name}
        if self.functionFields is not None:
            for key in self.functionFields:
                function[key] = self.functionFields[key]
        function_context['function'] = function

        return function_context


class FilterFunction(GafferFunction):
    def __init__(self, class_name, selection, function_fields=None):
        super().__init__(class_name, function_fields)
        self.selection = selection

    def toJson(self):
        function_context = super().toJson()

        selection_json = []
        for item in self.selection:
            selection_json.append(item.toJson())
        function_context['selection'] = selection_json

        return function_context


class TransformFunction(GafferFunction):
    def __init__(self, class_name, selection, projection, function_fields=None):
        super().__init__(class_name, function_fields)
        self.selection = selection
        self.projection = projection

    def toJson(self):
        function_context = super().toJson()

        selection_json = []
        for item in self.selection:
            selection_json.append(item.toJson())
        function_context['selection'] = selection_json
        function_context['selection'] = selection_json

        projection_json = []
        for item in self.projection:
            projection_json.append(item.toJson())
        function_context['projection'] = projection_json

        return function_context


class PropertyKey(ToJson):
    def __init__(self, key):
        # Validate the arguments
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        self.key = key

    def toJson(self):
        return {'key': self.key, 'isId': False}


class IdentifierKey(ToJson):
    def __init__(self, key):
        # Validate the arguments
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        self.key = key

    def toJson(self):
        return {'key': self.key, 'isId': True}


class IncludeEdges:
    ALL = 'ALL'
    DIRECTED = 'DIRECTED'
    UNDIRECTED = 'UNDIRECTED'
    NONE = 'NONE'


class InOutType:
    BOTH = 'BOTH'
    IN = 'INCOMING'
    OUT = 'OUTGOING'


class OperationChain(ToJson):
    def __init__(self, operations):
        self.operations = operations

    def toJson(self):
        operations_json = []
        for operation in self.operations:
            operations_json.append(operation.toJson())
        return {'operations': operations_json}


class Operation(ToJson):
    def __init__(self, class_name, view=None, options=None):
        self.class_name = class_name
        self.view = view
        self.options = options

    def convert_result(self, result):
        raise NotImplementedError('Use an implementation of Operation instead')

    def toJson(self):
        operation = {'class': self.class_name}
        if self.options is not None:
            operation['options'] = self.options
        if self.view is not None:
            operation['view'] = self.view.toJson()
        return operation


class AddElements(Operation):
    """
    This class defines a Gaffer Add Operation.
    """

    def __init__(self, elements=None, skip_invalid_elements=False,
                 validate=True,
                 view=None, options=None):
        super().__init__('gaffer.operation.impl.add.AddElements', view, options)
        self.elements = elements
        self.skipInvalidElements = skip_invalid_elements
        self.validate = validate

    def toJson(self):
        operation = super().toJson()
        operation['skipInvalidElements'] = self.skipInvalidElements
        operation['validate'] = self.validate
        if self.elements is not None:
            elements_json = []
            for element in self.elements:
                elements_json.append(element.toJson())
            operation['elements'] = elements_json
        return operation

    def convert_result(self, result):
        return None


class GenerateElements(Operation):
    def __init__(self, generator_class_name, element_generator_fields=None,
                 objects=None, view=None, options=None):
        super().__init__('gaffer.operation.impl.generate.GenerateElements',
                         view, options)
        self.generatorClassName = generator_class_name
        self.elementGeneratorFields = element_generator_fields
        self.objects = objects

    def toJson(self):
        operation = super().toJson()

        if self.objects is not None:
            operation['objects'] = self.objects

        element_generator = {'class': self.generatorClassName}
        if self.elementGeneratorFields is not None:
            for field in self.elementGeneratorFields:
                element_generator[field.key] = field.value
        operation['elementGenerator'] = element_generator
        return operation

    def convert_result(self, result):
        return ResultConverter.to_elements(result)


class GenerateObjects(Operation):
    def __init__(self, generator_class_name, element_generator_fields=None,
                 elements=None, view=None, options=None):
        super().__init__('gaffer.operation.impl.generate.GenerateObjects', view,
                         options)
        self.generatorClassName = generator_class_name
        self.elementGeneratorFields = element_generator_fields
        self.elements = elements

    def toJson(self):
        operation = super().toJson()

        if self.elements is not None:
            elements_json = []
            for element in self.elements:
                elements_json.append(element.toJson())
            operation['elements'] = elements_json

        element_generator = {'class': self.generatorClassName}
        if self.elementGeneratorFields is not None:
            for field in self.elementGeneratorFields:
                element_generator[field.key] = field.value
        operation['elementGenerator'] = element_generator
        return operation

    def convert_result(self, result):
        return result


class InitialiseSetExport(Operation):
    def __init__(self, key=None, options=None):
        super().__init__(
            'gaffer.operation.impl.export.initialise.InitialiseSetExport',
            None,
            options)
        if not isinstance(key, str) and key is not None:
            raise TypeError('key must be a string')
        self.key = key

    def toJson(self):
        operation = super().toJson()

        if self.key is not None:
            operation['key'] = self.key

        return operation

    def convert_result(self, result):
        return None


class UpdateExport(Operation):
    def __init__(self, key=None, options=None):
        super().__init__('gaffer.operation.impl.export.UpdateExport', None,
                         options)
        if not isinstance(key, str) and key is not None:
            raise TypeError('key must be a string')
        self.key = key

    def toJson(self):
        operation = super().toJson()

        if self.key is not None:
            operation['key'] = self.key

        return operation

    def convert_result(self, result):
        return None


class FetchExporter(Operation):
    def __init__(self, options=None):
        super().__init__('gaffer.operation.impl.export.FetchExporter', None,
                         options)

    def convert_result(self, result):
        return result


class FetchExport(Operation):
    def __init__(self, key=None, options=None):
        super().__init__('gaffer.operation.impl.export.FetchExport', None,
                         options)
        if not isinstance(key, str) and key is not None:
            raise TypeError('key must be a string')
        self.key = key

    def toJson(self):
        operation = super().toJson()

        if self.key is not None:
            operation['key'] = self.key

        return operation

    def convert_result(self, result):
        return result


class GetOperation(Operation):
    def __init__(self, class_name, seeds=None, view=None, summarise=True,
                 include_entities=True, include_edges=IncludeEdges.ALL,
                 in_out_type=InOutType.BOTH, options=None):
        super().__init__(class_name, view, options)
        if not isinstance(class_name, str):
            raise TypeError(
                'ClassName must be the operation class name as a string')
        self.seeds = seeds
        self.summarise = summarise
        self.include_entities = include_entities
        self.include_edges = include_edges
        self.in_out_type = in_out_type

    def convert_result(self, result):
        return ResultConverter.to_elements(result)

    def toJson(self):
        operation = super().toJson()

        if self.seeds is not None:
            json_seeds = []
            for seed in self.seeds:
                if isinstance(seed, ElementSeed):
                    json_seeds.append(seed.toJson())
                elif isinstance(seed, str):
                    json_seeds.append(EntitySeed(seed).toJson())
                else:
                    raise TypeError(
                        'Seeds argument must contain ElementSeed objects')
            operation['seeds'] = json_seeds
        operation['includeEntities'] = self.include_entities
        operation['includeEdges'] = self.include_edges
        operation['includeIncomingOutGoing'] = self.in_out_type
        operation['summarise'] = self.summarise
        return operation


class GetRelatedElements(GetOperation):
    def __init__(self, seeds=None, view=None, summarise=True,
                 include_entities=True, include_edges=IncludeEdges.ALL,
                 in_out_type=InOutType.BOTH, options=None):
        super().__init__('gaffer.operation.impl.get.GetRelatedElements', seeds,
                         view, summarise, include_entities, include_edges,
                         in_out_type, options)


class GetRelatedEntities(GetOperation):
    def __init__(self, seeds=None, view=None, summarise=True,
                 in_out_type=InOutType.BOTH, options=None):
        super().__init__('gaffer.operation.impl.get.GetRelatedEntities', seeds,
                         view, summarise, True, IncludeEdges.NONE,
                         in_out_type, options)


class GetRelatedEdges(GetOperation):
    def __init__(self, seeds=None, view=None, summarise=True,
                 include_edges=IncludeEdges.ALL,
                 in_out_type=InOutType.BOTH, options=None):
        super().__init__('gaffer.operation.impl.get.GetRelatedEdges', seeds,
                         view, summarise, False, include_edges,
                         in_out_type, options)


class GetElementsBySeed(GetOperation):
    def __init__(self, seeds=None, view=None, summarise=True,
                 include_entities=True, include_edges=IncludeEdges.ALL,
                 in_out_type=InOutType.BOTH, options=None):
        super().__init__('gaffer.operation.impl.get.GetElementsBySeed', seeds,
                         view, summarise, include_entities, include_edges,
                         in_out_type, options)


class GetEntitiesBySeed(GetOperation):
    def __init__(self, seeds=None, view=None, summarise=True,
                 in_out_type=InOutType.BOTH, options=None):
        super().__init__('gaffer.operation.impl.get.GetEntitiesBySeed', seeds,
                         view, summarise, True, IncludeEdges.NONE,
                         in_out_type, options)


class GetEdgesBySeed(GetOperation):
    def __init__(self, seeds=None, view=None, summarise=True,
                 include_edges=IncludeEdges.ALL,
                 in_out_type=InOutType.BOTH, options=None):
        super().__init__('gaffer.operation.impl.get.GetEntitiesBySeed', seeds,
                         view, summarise, False, include_edges,
                         in_out_type, options)


class GetAdjacentEntitySeeds(GetOperation):
    def __init__(self, seeds=None, view=None, summarise=True,
                 in_out_type=InOutType.BOTH, options=None):
        super().__init__('gaffer.operation.impl.get.GetAdjacentEntitySeeds',
                         seeds, view, summarise, True, IncludeEdges.ALL,
                         in_out_type, options)

    def convert_result(self, result):
        return ResultConverter.to_entity_seeds(result)


class GetAllElements(GetOperation):
    def __init__(self, view=None, summarise=True, include_entities=True,
                 include_edges=IncludeEdges.ALL, options=None):
        super().__init__('gaffer.operation.impl.get.GetAllElements',
                         None, view, summarise, include_entities, include_edges,
                         InOutType.OUT, options)

    def convert_result(self, result):
        return ResultConverter.to_elements(result)


class GetAllEntities(GetOperation):
    def __init__(self, view=None, summarise=True, options=None):
        super().__init__('gaffer.operation.impl.get.GetAllEntities',
                         None, view, summarise, True, IncludeEdges.NONE,
                         InOutType.OUT, options)

    def convert_result(self, result):
        return ResultConverter.to_elements(result)


class GetAllEdges(GetOperation):
    def __init__(self, view=None, summarise=True,
                 include_edges=IncludeEdges.ALL, options=None):
        super().__init__('gaffer.operation.impl.get.GetAllEdges',
                         None, view, summarise, False, include_edges,
                         InOutType.OUT, options)

    def convert_result(self, result):
        return ResultConverter.to_elements(result)


class CountGroups(Operation):
    def __init__(self, limit=None, options=None):
        super().__init__('gaffer.operation.impl.CountGroups',
                         None, options)
        self.limit = limit

    def toJson(self):
        operation = super().toJson()

        if self.limit is not None:
            operation['limit'] = self.limit

        return operation

    def convert_result(self, result):
        return result
