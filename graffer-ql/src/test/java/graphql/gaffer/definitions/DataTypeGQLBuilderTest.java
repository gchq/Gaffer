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

package graphql.gaffer.definitions;

import gaffer.graphql.GrafferQLException;
import gaffer.graphql.definitions.DataTypeGQLBuilder;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEdgeDefinition;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.TypeDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by joe on 9/8/16.
 */
public class DataTypeGQLBuilderTest {
    private static final String VERTEX_TYPE_NAME = "Vertex.string";
    private static final String VERTEX_TYPE_NAME_SAFE = "Vertexstring";
    private static final String ENTITY_TYPE_NAME = "MyTestEntity";
    private static final String EDGE_TYPE_NAME = "MyTestEdge";

    @Test
    public void testEntities() throws GrafferQLException {
        /**
         * Given
         */
        final GraphQLObjectType.Builder queryBuilder = mock(GraphQLObjectType.Builder.class);
        when(queryBuilder.field(any(GraphQLFieldDefinition.class))).thenReturn(queryBuilder);
        when(queryBuilder.build()).thenReturn(mock(GraphQLObjectType.class));

        final Schema schema = mock(Schema.class);
        // Setup an entity with the type as a vertex
        final Map<String, SchemaEntityDefinition> entityTypes = new HashMap<>();
        final SchemaEntityDefinition entityType = mock(SchemaEntityDefinition.class);
        entityTypes.put(ENTITY_TYPE_NAME, entityType);
        when(entityType.getVertex()).thenReturn(VERTEX_TYPE_NAME);
        when(schema.getEntities()).thenReturn(entityTypes);
        // Setup the data type itself
        final TypeDefinition typeDefinition = mock(TypeDefinition.class);

        /**
         * When
         */
        final GraphQLObjectType dataType = new DataTypeGQLBuilder()
                .gafferSchema(schema)
                .name(VERTEX_TYPE_NAME)
                .typeDefinition(typeDefinition)
                .queryTypeBuilder(queryBuilder)
                .build();

        /**
         * Then
         */
        assertEquals(dataType.getName(), VERTEX_TYPE_NAME_SAFE);
        final GraphQLFieldDefinition entityField = dataType.getFieldDefinition(ENTITY_TYPE_NAME);
        assertNotNull(entityField);
    }

    @Test
    public void testEdges() throws GrafferQLException {
        /**
         * Given
         */
        final GraphQLObjectType.Builder queryBuilder = mock(GraphQLObjectType.Builder.class);
        when(queryBuilder.field(any(GraphQLFieldDefinition.class))).thenReturn(queryBuilder);
        when(queryBuilder.build()).thenReturn(mock(GraphQLObjectType.class));

        final Schema schema = mock(Schema.class);
        // Setup an entity with the type as a vertex
        final Map<String, SchemaEdgeDefinition> edgeTypes = new HashMap<>();
        final SchemaEdgeDefinition edgeType = mock(SchemaEdgeDefinition.class);
        edgeTypes.put(EDGE_TYPE_NAME, edgeType);
        when(edgeType.getSource()).thenReturn(VERTEX_TYPE_NAME);
        when(schema.getEdges()).thenReturn(edgeTypes);
        // Setup the data type itself
        final TypeDefinition typeDefinition = mock(TypeDefinition.class);

        /**
         * When
         */
        final GraphQLObjectType dataType = new DataTypeGQLBuilder()
                .gafferSchema(schema)
                .name(VERTEX_TYPE_NAME)
                .typeDefinition(typeDefinition)
                .queryTypeBuilder(queryBuilder)
                .build();

        /**
         * Then
         */
        assertEquals(dataType.getName(), VERTEX_TYPE_NAME_SAFE);
        final GraphQLFieldDefinition entityField = dataType.getFieldDefinition(EDGE_TYPE_NAME);
        assertNotNull(entityField);
    }
}
