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

import gaffer.data.element.Entity;
import gaffer.graphql.fetch.EntityByArgDataFetcher;
import gaffer.graphql.fetch.VertexSourceDataFetcher;
import gaffer.store.schema.SchemaEntityDefinition;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;

import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInterfaceType.newInterface;

/**
 * Builder for GraphQL objects based on Gaffer Entity Types
 */
public class EntityTypeGQLBuilder extends ElementTypeGQLBuilder<Entity, SchemaEntityDefinition, EntityTypeGQLBuilder> {

    private static final GraphQLInterfaceType ABSTRACT_TYPE = newInterface()
            .name(Constants.ENTITY)
            .description("A abstract Gaffer Entity.")
            .typeResolver(new NullTypeResolver())
            .build();

    @Override
    protected GraphQLInterfaceType getInterfaceType() {
        return ABSTRACT_TYPE;
    }

    @Override
    protected void contribute(final GraphQLObjectType.Builder builder) {
        final GraphQLObjectType vertexType = getDataObjectTypes().get(getElementDefinition().getVertex());

        builder.field(newFieldDefinition()
                .name(Constants.VERTEX_VALUE)
                .description("Vertex value the Entity is Attached to")
                .type(new GraphQLNonNull(GraphQLString))
                .build())
                .field(newFieldDefinition()
                        .name(Constants.VERTEX)
                        .description("Vertex the Entity is Attached to")
                        .type(new GraphQLNonNull(vertexType))
                        .dataFetcher(new VertexSourceDataFetcher(Constants.VERTEX_VALUE))
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
                        .dataFetcher(new EntityByArgDataFetcher(type.getName()))
                        .build())
                .build();
    }

    @Override
    public EntityTypeGQLBuilder self() {
        return this;
    }
}
