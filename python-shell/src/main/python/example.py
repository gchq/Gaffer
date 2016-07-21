#
# Copyright 2016 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import gaffer as g
import gafferConnector


def run(host, verbose=False):
    return run_with_connector(create_connector(host, verbose))


def run_with_connector(gc):
    print()
    print('Running operations')
    print('--------------------------')
    print()

    add_elements(gc)
    get_elements(gc)
    get_adj_seeds(gc)
    get_all_elements(gc)
    generate_elements(gc)
    generate_domain_objs(gc)
    generate_domain_objects_chain(gc)
    get_element_group_counts(gc)
    get_sub_graph(gc)


def create_connector(host, verbose=False):
    return gafferConnector.GafferConnector(host, verbose)


def add_elements(gc):
    # Add Elements
    gc.execute_operation(
        g.AddElements(
            elements=[
                g.Entity(
                    group='entity',
                    vertex='1',
                    properties={
                        'count': 1
                    }
                ),
                g.Entity(
                    group='entity',
                    vertex='2',
                    properties={
                        'count': 1
                    }
                ),
                g.Entity(
                    group='entity',
                    vertex='3',
                    properties={
                        'count': 1
                    }
                ),
                g.Edge(
                    group='edge',
                    source='1',
                    destination='2',
                    directed=True,
                    properties={
                        'count': 1
                    }
                ),
                g.Edge(
                    group='edge',
                    source='2',
                    destination='3',
                    directed=True,
                    properties={
                        'count': 1
                    }
                )
            ]
        )
    )
    print('Elements have been added')
    print()


def get_elements(gc):
    # Get Elements
    elements = gc.execute_operation(
        g.GetRelatedElements(
            seeds=[g.EntitySeed('1')],
            view=g.View(
                entities=[
                    g.ElementDefinition(
                        group='entity',
                        transient_properties=[
                            g.Property('newProperty', 'java.lang.String')
                        ],
                        filter_functions=[
                            g.FilterFunction(
                                class_name='gaffer.function.simple.filter.IsEqual',
                                selection=[g.IdentifierKey('VERTEX')],
                                function_fields={'value': '1'}
                            )
                        ],
                        transform_functions=[
                            g.TransformFunction(
                                class_name='gaffer.rest.example.ExampleTransformFunction',
                                selection=[g.IdentifierKey('VERTEX')],
                                projection=[g.PropertyKey('newProperty')]
                            )
                        ]
                    )
                ],
                edges=[
                    g.ElementDefinition('edge')
                ]
            ),
            include_entities=False,
            include_edges=g.IncludeEdges.ALL
        )
    )
    print('Related elements')
    print(elements)
    print()


def get_adj_seeds(gc):
    # Adjacent Elements - chain 2 adjacent entities together
    adjSeeds = gc.execute_operations(
        [
            g.GetAdjacentEntitySeeds(
                seeds=[
                    g.EntitySeed(
                        vertex='1'
                    )
                ],
                in_out_type=g.InOutType.OUT
            ),
            g.GetAdjacentEntitySeeds(in_out_type=g.InOutType.OUT)
        ]
    )
    print('Adjacent entities - 2 hop')
    print(adjSeeds)
    print()


def get_all_elements(gc):
    # Adjacent Elements - chain 2 adjacent entities together
    all_elements = gc.execute_operation(
        g.GetAllElements(summarise=False)
    )
    print('All elements')
    print(all_elements)
    print()


def generate_elements(gc):
    # Generate Elements
    elements = gc.execute_operation(
        g.GenerateElements('gaffer.rest.example.ExampleDomainObjectGenerator',
                           objects=[
                               {
                                   'class': 'gaffer.rest.example.ExampleDomainObject',
                                   'ids': [
                                       '1',
                                       '2',
                                       True
                                   ],
                                   'type': 'edge'
                               },
                               {
                                   'class': 'gaffer.rest.example.ExampleDomainObject',
                                   'ids': [
                                       '1'
                                   ],
                                   'type': 'entity'
                               }
                           ]
                           )
    )
    print('Generated elements from provided domain objects')
    print(elements)
    print()


def generate_domain_objs(gc):
    # Generate Domain Objects - single provided element
    objects = gc.execute_operation(
        g.GenerateObjects('gaffer.rest.example.ExampleDomainObjectGenerator',
                          elements=[
                              g.Entity('entity', '1'),
                              g.Edge('edge', '1', '2', True)
                          ]
                          )
    )
    print('Generated objects from provided elements')
    print(objects)
    print()


def generate_domain_objects_chain(gc):
    # Generate Domain Objects - chain of get elements then generate objects
    objects = gc.execute_operations(
        [
            g.GetElementsBySeed(
                seeds=[g.EntitySeed('1')]
            ),
            g.GenerateObjects(
                'gaffer.rest.example.ExampleDomainObjectGenerator')
        ]
    )
    print('Generated objects from get elements by seed')
    print(objects)
    print()


def get_element_group_counts(gc):
    # Get Elements
    elements = gc.execute_operations([
        g.GetRelatedElements(
            seeds=[g.EntitySeed('1')]
        ),
        g.CountGroups(limit=1000)
    ])
    print('Groups counts (limited to 1000 elements)')
    print(elements)
    print()


def get_sub_graph(gc):
    # Initialise, update and fetch an in memory set export
    result = gc.execute_operations(
        [
            g.InitialiseSetExport(),
            g.GetAdjacentEntitySeeds(
                seeds=[g.EntitySeed('1')],
            ),
            g.UpdateExport(),
            g.GetAdjacentEntitySeeds(),
            g.UpdateExport(),
            g.FetchExport()
        ]
    )
    print('Initialise, update and fetch export with adjacent entities')
    entity_seeds = g.ResultConverter.to_entity_seeds(result)
    print(entity_seeds)
    print()


if __name__ == '__main__':
    run('http://localhost:8080/example-rest/v1')
