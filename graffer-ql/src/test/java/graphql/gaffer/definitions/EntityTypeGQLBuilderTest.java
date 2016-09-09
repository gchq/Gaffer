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
import gaffer.graphql.definitions.Constants;
import gaffer.graphql.definitions.EntityTypeGQLBuilder;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEntityDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static graphql.Scalars.GraphQLString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EntityTypeGQLBuilderTest {
    private static final String VERTEX_TYPE_NAME = "Vertex.string";
    private static final String VERTEX_TYPE_NAME_SAFE = "Vertexstring";
    private static final String ENTITY_TYPE_NAME = "MyTestEntity";

    @Test
    public void test() throws GrafferQLException {
        /**
         * Given
         */
        final GraphQLObjectType.Builder queryBuilder = mock(GraphQLObjectType.Builder.class);
        when(queryBuilder.field(any(GraphQLFieldDefinition.class))).thenReturn(queryBuilder);
        when(queryBuilder.build()).thenReturn(mock(GraphQLObjectType.class));

        final Schema schema = mock(Schema.class);
        final SchemaEntityDefinition entityDefinition = mock(SchemaEntityDefinition.class);
        when(entityDefinition.getVertex()).thenReturn(VERTEX_TYPE_NAME);
        final Map<String, GraphQLObjectType> dataObjectTypes = new HashMap<>();
        final GraphQLObjectType dataObjectType = mock(GraphQLObjectType.class);
        when(dataObjectType.getName()).thenReturn(VERTEX_TYPE_NAME_SAFE);
        dataObjectTypes.put(VERTEX_TYPE_NAME, dataObjectType);

        /**
         * When
         */
        final GraphQLObjectType entityType = new EntityTypeGQLBuilder()
                .name(ENTITY_TYPE_NAME)
                .schema(schema)
                .dataObjectTypes(dataObjectTypes)
                .elementDefinition(entityDefinition)
                .queryTypeBuilder(queryBuilder)
                .build();

        /**
         * Then
         */
        final GraphQLFieldDefinition vertexField = entityType.getFieldDefinition(Constants.VERTEX);
        assertNotNull(vertexField);
        assertEquals(new GraphQLNonNull(dataObjectType), vertexField.getType());

        final GraphQLFieldDefinition vertexValueField = entityType.getFieldDefinition(Constants.VERTEX_VALUE);
        assertNotNull(vertexValueField);
        assertEquals(new GraphQLNonNull(GraphQLString), vertexValueField.getType());
    }
}
