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

import gaffer.data.element.Edge;
import gaffer.graphql.fetch.EdgeByArgDataFetcher;
import gaffer.graphql.fetch.VertexSourceDataFetcher;
import gaffer.store.schema.SchemaEdgeDefinition;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;

import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInterfaceType.newInterface;

/**
 * Builder for GraphQL objects based on Gaffer Edge Types
 */
public class EdgeTypeGQLBuilder extends ElementTypeGQLBuilder<Edge, SchemaEdgeDefinition, EdgeTypeGQLBuilder> {

    private static final GraphQLInterfaceType ABSTRACT_TYPE = newInterface()
            .name(Constants.EDGE)
            .description("A abstract Gaffer Edge.")
            .typeResolver(new NullTypeResolver())
            .build();

    @Override
    protected GraphQLInterfaceType getInterfaceType() {
        return ABSTRACT_TYPE;
    }

    @Override
    protected void contribute(final GraphQLObjectType.Builder builder) {
        final GraphQLObjectType sourceVertexType =
                getDataObjectTypes().get(getElementDefinition().getSource());
        final GraphQLObjectType destinationVertexType =
                getDataObjectTypes().get(getElementDefinition().getDestination());

        builder.field(newFieldDefinition()
                .name(Constants.SOURCE_VALUE)
                .description("Source Vertex Value of the Edge")
                .type(new GraphQLNonNull(GraphQLString))
                .build())
                .field(newFieldDefinition()
                        .name(Constants.SOURCE)
                        .description("Source Vertex Object of the Edge")
                        .type(new GraphQLNonNull(sourceVertexType))
                        .dataFetcher(new VertexSourceDataFetcher(Constants.SOURCE_VALUE))
                        .build())
                .field(newFieldDefinition()
                        .name(Constants.DESTINATION_VALUE)
                        .description("Destination Vertex Value of the Edge")
                        .type(new GraphQLNonNull(GraphQLString))
                        .build())
                .field(newFieldDefinition()
                        .name(Constants.DESTINATION)
                        .description("Destination Vertex Object of the Edge")
                        .type(new GraphQLNonNull(destinationVertexType))
                        .dataFetcher(new VertexSourceDataFetcher(Constants.DESTINATION_VALUE))
                        .build());
    }

    @Override
    protected void addToQuery(final GraphQLObjectType type,
                              final GraphQLObjectType.Builder queryTypeBuilder) {
        queryTypeBuilder
                .field(newFieldDefinition()
                        .name(type.getName())
                        .type(new GraphQLList(type))
                        .argument(newArgument()
                                .name(Constants.VERTEX)
                                .type(GraphQLString)
                                .build())
                        .argument(newArgument()
                                .name(Constants.SOURCE)
                                .type(GraphQLString)
                                .build())
                        .argument(newArgument()
                                .name(Constants.DESTINATION)
                                .type(GraphQLString)
                                .build())
                        .dataFetcher(new EdgeByArgDataFetcher(type.getName()))
                        .build())
                .build();
    }

    @Override
    public EdgeTypeGQLBuilder self() {
        return this;
    }
}
