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
package gaffer.graphql.definitions;

import gaffer.graphql.GrafferQLException;
import gaffer.graphql.fetch.EdgeByVertexDataFetcher;
import gaffer.graphql.fetch.EntityByVertexDataFetcher;
import gaffer.graphql.fetch.VertexArgDataFetcher;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEdgeDefinition;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.TypeDefinition;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLTypeReference;
import org.apache.commons.lang.CharUtils;

import java.util.Map;

import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInterfaceType.newInterface;
import static graphql.schema.GraphQLObjectType.newObject;

/**
 * A Builder class for composing GraphQL Object Types based on Gaffer Data Types.
 */
public class DataTypeGQLBuilder {

    // Base Vertex Type
    private static final GraphQLInterfaceType ABSTRACT_VERTEX_TYPE = newInterface()
            .name(Constants.VERTEX)
            .description("A abstract Gaffer Vertex.")
            .typeResolver(new NullTypeResolver())
            .build();

    private String rawName;
    private Schema gafferSchema;
    private TypeDefinition typeDefinition;
    private GraphQLObjectType.Builder queryTypeBuilder;

    public DataTypeGQLBuilder() {
    }

    public DataTypeGQLBuilder name(final String rawName) {
        this.rawName = rawName;
        return this;
    }

    public DataTypeGQLBuilder gafferSchema(final Schema gafferSchema) {
        this.gafferSchema = gafferSchema;
        return this;
    }

    public DataTypeGQLBuilder typeDefinition(final TypeDefinition typeDefinition) {
        this.typeDefinition = typeDefinition;
        return this;
    }

    public DataTypeGQLBuilder queryTypeBuilder(final GraphQLObjectType.Builder queryTypeBuilder) {
        this.queryTypeBuilder = queryTypeBuilder;
        return this;
    }

    public GraphQLObjectType build() throws GrafferQLException {
        if (null == this.rawName) {
            throw new GrafferQLException("Name given to data type builder is null");
        }
        if (null == this.gafferSchema) {
            throw new GrafferQLException("Gaffer Schema given to data type builder is null");
        }
        if (null == this.typeDefinition) {
            throw new GrafferQLException("Type Definition given to data type builder is null");
        }
        if (null == this.queryTypeBuilder) {
            throw new GrafferQLException("Query Type given to data type builder is null");
        }
        final GraphQLObjectType.Builder vertexTypeBuilder = newObject()
                .name(getSafeName(rawName))
                .withInterface(ABSTRACT_VERTEX_TYPE)
                .field(newFieldDefinition()
                        .name(Constants.VALUE)
                        .description("Value of the Property or Vertex")
                        .type(new GraphQLNonNull(GraphQLString))
                        .build());

        // Register any Entity Fields
        for (final Map.Entry<String, SchemaEntityDefinition> entry : gafferSchema.getEntities().entrySet()) {
            if (rawName.equals(entry.getValue().getVertex())) {
                vertexTypeBuilder.field(newFieldDefinition()
                        .name(entry.getKey())
                        .type(new GraphQLList(new GraphQLTypeReference(entry.getKey())))
                        .dataFetcher(new EntityByVertexDataFetcher(entry.getKey()))
                        .build());
            }
        }

        // Register any Edge Fields
        for (final Map.Entry<String, SchemaEdgeDefinition> entry : gafferSchema.getEdges().entrySet()) {
            if (rawName.equals(entry.getValue().getSource())) {
                vertexTypeBuilder.field(newFieldDefinition()
                        .name(entry.getKey())
                        .type(new GraphQLList(new GraphQLTypeReference(entry.getKey())))
                        .dataFetcher(new EdgeByVertexDataFetcher(entry.getKey(), true))
                        .build());
            }
            if (rawName.equals(entry.getValue().getDestination())) {
                vertexTypeBuilder.field(newFieldDefinition()
                        .name(entry.getKey())
                        .type(new GraphQLList(new GraphQLTypeReference(entry.getKey())))
                        .dataFetcher(new EdgeByVertexDataFetcher(entry.getKey(), false))
                        .build());
            }
        }

        final GraphQLObjectType type = vertexTypeBuilder.build();
        queryTypeBuilder
                .field(newFieldDefinition()
                        .name(type.getName())
                        .type(type)
                        .argument(newArgument()
                                .name(Constants.VERTEX)
                                .type(GraphQLString)
                                .build())
                        .dataFetcher(new VertexArgDataFetcher(Constants.VERTEX))
                        .build())
                .build();
        return type;
    }

    /**
     * Strip out any non alpha characters
     *
     * @param input The non safe string from the Gaffer Schema
     * @return A safe version of the string for GraphQL
     */
    private static String getSafeName(final String input) {
        final StringBuilder output = new StringBuilder();
        for (final char c : input.toCharArray()) {
            if (CharUtils.isAsciiAlpha(c)) {
                output.append(c);
            }
        }
        return output.toString();
    }
}
