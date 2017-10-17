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

package uk.gov.gchq.gaffer.store.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.SerialisationFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The {@link SchemaOptimiser} is used to reduce the size of a given {@link Schema}.
 */
public class SchemaOptimiser {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaOptimiser.class);

    private final SerialisationFactory serialisationFactory;

    public SchemaOptimiser() {
        this(new SerialisationFactory());
    }

    public SchemaOptimiser(final SerialisationFactory serialisationFactory) {
        this.serialisationFactory = serialisationFactory;
    }

    /**
     * Optimise the provided {@link Schema} by removing unused types and adding
     * default serialisers.
     *
     * @param schema         the schema to optimise
     * @param isStoreOrdered determines whether to enforce ordering in the default
     *                       serialisers or not
     * @return the optimised schema object
     */
    public Schema optimise(final Schema schema, final boolean isStoreOrdered) {
        if (null != schema && null != schema.getTypes()) {
            return new Schema.Builder()
                    .merge(schema)
                    .types(getOptimisedTypes(schema, isStoreOrdered))
                    .vertexSerialiser(getDefaultVertexSerialiser(schema, isStoreOrdered))
                    .build();
        }

        return schema;
    }

    private Map<String, TypeDefinition> getOptimisedTypes(final Schema schema, final boolean isStoreOrdered) {
        Map<String, TypeDefinition> types = null;
        if (null != schema.getTypes()) {
            types = new LinkedHashMap<>(schema.getTypes());

            // Remove unused types
            removeUnusedTypes(schema, types);
            addDefaultSerialisers(schema, types, isStoreOrdered);
        }

        return types;
    }

    private void removeUnusedTypes(final Schema schema, final Map<String, TypeDefinition> types) {
        final Iterable<SchemaElementDefinition> schemaElements =
                new ChainedIterable<>(schema.getEntities().values(), schema.getEdges().values());

        final Set<String> usedTypeNames = new HashSet<>();
        for (final SchemaElementDefinition elDef : schemaElements) {
            usedTypeNames.addAll(elDef.getIdentifierTypeNames());
            usedTypeNames.addAll(elDef.getPropertyTypeNames());
        }
        for (final String typeName : new HashSet<>(types.keySet())) {
            if (!usedTypeNames.contains(typeName)) {
                types.remove(typeName);
            }
        }
    }

    private void addDefaultSerialisers(final Schema schema, final Map<String, TypeDefinition> types, final boolean isStoreOrdered) {
        final Iterable<SchemaElementDefinition> schemaElements =
                new ChainedIterable<>(schema.getEntities().values(), schema.getEdges().values());

        // Separate type definitions into 2 sets: types that are used in 'group by' properties; other types.
        final Set<String> groupByTypes = new HashSet<>();
        final Set<String> otherTypes = new HashSet<>();
        for (final SchemaElementDefinition elDef : schemaElements) {
            for (final String property : elDef.getProperties()) {
                if (elDef.getGroupBy().contains(property)) {
                    groupByTypes.add(elDef.getPropertyTypeName(property));
                } else {
                    otherTypes.add(elDef.getPropertyTypeName(property));
                }
            }
        }
        otherTypes.removeAll(groupByTypes);

        // Add the default serialisers for the types.
        // If the store is ordered then the group by type defs need to have
        // serialisers that preserves the ordering of bytes.
        for (final String typeName : groupByTypes) {
            final TypeDefinition typeDef = types.get(typeName);
            if (null != typeDef) {
                if (null == typeDef.getSerialiser()) {
                    typeDef.setSerialiser(serialisationFactory.getSerialiser(typeDef.getClazz(), isStoreOrdered, true));
                } else if (isStoreOrdered && !typeDef.getSerialiser().preservesObjectOrdering()) {
                    LOGGER.info("{} serialiser is used for a 'group by' property in an ordered store and it does not preserve the order of bytes. See https://github.com/gchq/Gaffer/wiki/Dev-Guide#serialisers.", typeDef.getSerialiser().getClass().getName());
                }
            }
        }
        for (final String typeName : otherTypes) {
            final TypeDefinition typeDef = types.get(typeName);
            if (null != typeDef) {
                if (null == typeDef.getSerialiser()) {
                    typeDef.setSerialiser(serialisationFactory.getSerialiser(typeDef.getClazz(), false, false));
                }
            }
        }
    }

    private Serialiser getDefaultVertexSerialiser(final Schema schema, final boolean isStoreOrdered) {
        if (null != schema.getVertexSerialiser()) {
            return schema.getVertexSerialiser();
        }

        final Set<Class<?>> vertexClasses = new HashSet<>();
        for (final SchemaEntityDefinition definition : schema.getEntities().values()) {
            vertexClasses.add(definition.getIdentifierClass(IdentifierType.VERTEX));
        }
        for (final SchemaEdgeDefinition definition : schema.getEdges().values()) {
            vertexClasses.add(definition.getIdentifierClass(IdentifierType.SOURCE));
            vertexClasses.add(definition.getIdentifierClass(IdentifierType.DESTINATION));
        }
        vertexClasses.remove(null);

        if (!vertexClasses.isEmpty()) {
            Serialiser serialiser = null;

            if (vertexClasses.size() == 1) {
                serialiser = serialisationFactory.getSerialiser(vertexClasses.iterator().next(), isStoreOrdered, true);
            } else {
                for (final Class<?> clazz : vertexClasses) {
                    serialiser = serialisationFactory.getSerialiser(clazz, isStoreOrdered, true);
                    boolean canHandlerAll = true;
                    for (final Class<?> clazz2 : vertexClasses) {
                        if (!serialiser.canHandle(clazz2)) {
                            canHandlerAll = false;
                            serialiser = null;
                            break;
                        }
                    }

                    if (canHandlerAll) {
                        break;
                    }
                }
            }

            if (null == serialiser) {
                throw new IllegalArgumentException("No default serialiser could be found that would support all vertex class types "
                        + vertexClasses.toString() + ", please implement your own or change your vertex class types.");
            }

            if (isStoreOrdered && !serialiser.preservesObjectOrdering()) {
                LOGGER.info("{} serialiser is used for vertex serialisation in an ordered store and it does not preserve the order of bytes. See https://github.com/gchq/Gaffer/wiki/Dev-Guide#serialisers.",
                        serialiser.getClass().getName());
            }

            return serialiser;
        }

        return null;
    }
}
