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
import gaffer.graphql.definitions.EdgeTypeGQLBuilder;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEdgeDefinition;
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

public class EdgeTypeGQLBuilderTest {
    private static final String SRC_VERTEX_TYPE_NAME = "Vertex.source";
    private static final String SRC_VERTEX_TYPE_NAME_SAFE = "SourceVString";
    private static final String DEST_VERTEX_TYPE_NAME = "Vertex.destination";
    private static final String DEST_VERTEX_TYPE_NAME_SAFE = "DestinationVString";
    private static final String EDGE_TYPE_NAME = "MyTestEntity";

    @Test
    public void test() throws GrafferQLException {
        /**
         * Given
         */
        final GraphQLObjectType.Builder queryBuilder = mock(GraphQLObjectType.Builder.class);
        when(queryBuilder.field(any(GraphQLFieldDefinition.class))).thenReturn(queryBuilder);
        when(queryBuilder.build()).thenReturn(mock(GraphQLObjectType.class));

        final Schema schema = mock(Schema.class);
        final SchemaEdgeDefinition edgeDefinition = mock(SchemaEdgeDefinition.class);
        when(edgeDefinition.getSource()).thenReturn(SRC_VERTEX_TYPE_NAME);
        when(edgeDefinition.getDestination()).thenReturn(DEST_VERTEX_TYPE_NAME);

        final Map<String, GraphQLObjectType> dataObjectTypes = new HashMap<>();

        final GraphQLObjectType sourceDataObjectType = mock(GraphQLObjectType.class);
        when(sourceDataObjectType.getName()).thenReturn(SRC_VERTEX_TYPE_NAME_SAFE);
        dataObjectTypes.put(SRC_VERTEX_TYPE_NAME, sourceDataObjectType);

        final GraphQLObjectType destDataObjectType = mock(GraphQLObjectType.class);
        when(destDataObjectType.getName()).thenReturn(DEST_VERTEX_TYPE_NAME_SAFE);
        dataObjectTypes.put(DEST_VERTEX_TYPE_NAME, destDataObjectType);

        /**
         * When
         */
        final GraphQLObjectType edgeType = new EdgeTypeGQLBuilder()
                .name(EDGE_TYPE_NAME)
                .schema(schema)
                .dataObjectTypes(dataObjectTypes)
                .elementDefinition(edgeDefinition)
                .queryTypeBuilder(queryBuilder)
                .build();

        /**
         * Then
         */
        final GraphQLFieldDefinition srcVertexField = edgeType.getFieldDefinition(Constants.SOURCE);
        assertNotNull(srcVertexField);
        assertEquals(new GraphQLNonNull(sourceDataObjectType), srcVertexField.getType());
        final GraphQLFieldDefinition sourceVertexValueField = edgeType.getFieldDefinition(Constants.SOURCE_VALUE);
        assertNotNull(sourceVertexValueField);
        assertEquals(new GraphQLNonNull(GraphQLString), sourceVertexValueField.getType());

        final GraphQLFieldDefinition destVertexField = edgeType.getFieldDefinition(Constants.DESTINATION);
        assertNotNull(destVertexField);
        assertEquals(new GraphQLNonNull(destDataObjectType), destVertexField.getType());
        final GraphQLFieldDefinition destVertexValueField = edgeType.getFieldDefinition(Constants.DESTINATION_VALUE);
        assertNotNull(destVertexValueField);
        assertEquals(new GraphQLNonNull(GraphQLString), destVertexValueField.getType());

    }
}
