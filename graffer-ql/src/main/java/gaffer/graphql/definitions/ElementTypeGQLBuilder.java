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

import gaffer.data.element.Element;
import gaffer.graphql.GrafferQLException;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaElementDefinition;
import gaffer.store.schema.TypeDefinition;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLObjectType;
import org.apache.log4j.Logger;

import java.util.Map;

import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;

/**
 * General form of a GraphQL type builder based on a Gaffer Element definitions.
 */
public abstract class ElementTypeGQLBuilder<
        E extends Element,
        S extends SchemaElementDefinition,
        B extends ElementTypeGQLBuilder<E, S, B>> {

    private static final Logger LOGGER = Logger.getLogger(ElementTypeGQLBuilder.class);

    protected abstract GraphQLInterfaceType getInterfaceType();

    protected abstract void contribute(final GraphQLObjectType.Builder builder);
    protected abstract void addToQuery(final GraphQLObjectType type,
                                       final GraphQLObjectType.Builder queryTypeBuilder);

    public abstract B self();

    private Map<String, GraphQLObjectType> dataObjectTypes;
    private String name;
    private S elementDefinition;
    private Schema schema;
    private GraphQLObjectType.Builder queryTypeBuilder;

    public B dataObjectTypes(final Map<String, GraphQLObjectType> dataObjectTypes) {
        this.dataObjectTypes = dataObjectTypes;
        return self();
    }

    public B name(final String name) {
        this.name = name;
        return self();
    }

    public B elementDefinition(final S elementDefinition) {
        this.elementDefinition = elementDefinition;
        return self();
    }

    public B schema(final Schema schema) {
        this.schema = schema;
        return self();
    }

    public B queryTypeBuilder(final GraphQLObjectType.Builder queryTypeBuilder) {
        this.queryTypeBuilder = queryTypeBuilder;
        return self();
    }

    protected Map<String, GraphQLObjectType> getDataObjectTypes() {
        return dataObjectTypes;
    }

    protected String getName() {
        return name;
    }

    protected S getElementDefinition() {
        return elementDefinition;
    }

    protected Schema getSchema() {
        return schema;
    }

    public GraphQLObjectType build() throws GrafferQLException {
        if (null == this.dataObjectTypes) {
            throw new GrafferQLException("dataObjectTypes given to data type builder is null");
        }
        if (null == this.name) {
            throw new GrafferQLException("name given to data type builder is null");
        }
        if (null == this.schema) {
            throw new GrafferQLException("schema given to data type builder is null");
        }
        if (null == this.elementDefinition) {
            throw new GrafferQLException("elementDefinition given to data type builder is null");
        }
        if (null == this.queryTypeBuilder) {
            throw new GrafferQLException("queryTypeBuilder given to data type builder is null");
        }

        // Create a builder
        final GraphQLObjectType.Builder builder = newObject()
                .name(name)
                .withInterface(getInterfaceType());
        for (final String propName : elementDefinition.getProperties()) {
            final String propTypeName = elementDefinition.getPropertyTypeName(propName);
            GraphQLObjectType propObjectType = dataObjectTypes.get(propTypeName);
            final TypeDefinition prop = elementDefinition.getPropertyTypeDef(propName);
            LOGGER.debug("Property " + propName + " - " + propTypeName + " - " + prop);
            builder
                    .field(newFieldDefinition()
                            .name(propName)
                            .type(propObjectType)
                            .build());
        }

        contribute(builder);
        final GraphQLObjectType type = builder.build();
        addToQuery(type, queryTypeBuilder);
        return type;
    }
}
