/*
 * Copyright 2016-2017 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Contains the full list of {@link uk.gov.gchq.gaffer.data.element.Element} types to be stored in the graph.
 * <p>
 * Each type of element should have the identifier type(s) listed and a map of property names and their corresponding types.
 * Each type can either be a full java class name or a custom type. Using custom types then allows you to specify
 * validation and aggregation for the element components.
 * <p>
 * This class must be JSON serialisable.
 * A schema should normally be written in JSON and then it will be automatically deserialised at runtime.
 * An example of a JSON schemas can be found in the Example module.
 *
 * @see Schema.Builder
 * @see ElementDefinitions
 */
@JsonDeserialize(builder = Schema.Builder.class)
public class Schema extends ElementDefinitions<SchemaEntityDefinition, SchemaEdgeDefinition> implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementDefinitions.class);
    private final TypeDefinition unknownType = new TypeDefinition();

    /**
     * The {@link uk.gov.gchq.gaffer.serialisation.Serialisation} for all vertices.
     */
    private Serialisation vertexSerialiser;

    /**
     * A map of custom type name to {@link TypeDefinition}.
     *
     * @see TypeDefinition
     */
    private Map<String, TypeDefinition> types;

    private String visibilityProperty;

    private String timestampProperty;

    public Schema() {
        this(new LinkedHashMap<>());
    }

    protected Schema(final Map<String, TypeDefinition> types) {
        this.types = types;
    }

    public static Schema fromJson(final InputStream... inputStreams) throws SchemaException {
        return new Schema.Builder().json(inputStreams).build();
    }

    public static Schema fromJson(final Path... filePaths) throws SchemaException {
        return new Schema.Builder().json(filePaths).build();
    }

    public static Schema fromJson(final byte[]... jsonBytes) throws SchemaException {
        return new Schema.Builder().json(jsonBytes).build();
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Uses toJson instead.")
    @Override
    public Schema clone() {
        return fromJson(toJson(false));
    }

    /**
     * Checks the schema has aggregators.
     *
     * @return {@code true} if the schema contains aggregators, otherwise {@code false}
     */
    public boolean hasAggregators() {
        boolean schemaContainsAggregators = false;

        for (final TypeDefinition type : types.values()) {
            if (null != type.getAggregateFunction()) {
                schemaContainsAggregators = true;
            }
        }

        return schemaContainsAggregators;
    }

    /**
     * Validates the schema to ensure all element definitions are valid.
     * Throws a SchemaException if it is not valid.
     *
     * @return true if valid, otherwise false.
     * @throws SchemaException if validation fails then a SchemaException is thrown.
     */
    public boolean validate() throws SchemaException {
        for (final String edgeGroup : getEdgeGroups()) {
            if (null != getEntity(edgeGroup)) {
                LOGGER.warn("Groups must not be shared between Entity definitions and Edge definitions."
                        + "Found edgeGroup '" + edgeGroup + "' in the collection of entities");
                return false;
            }
        }

        final boolean hasAggregators = hasAggregators();

        for (final Entry<String, SchemaEdgeDefinition> elementDefEntry : getEdges().entrySet()) {
            if (null == elementDefEntry.getValue()) {
                throw new SchemaException("Edge definition was null for group: " + elementDefEntry.getKey());
            }

            if (!elementDefEntry.getValue().validate(hasAggregators)) {
                LOGGER.warn("VALIDITY ERROR: Invalid edge definition for group: " + elementDefEntry.getKey());
                return false;
            }
        }

        for (final Entry<String, SchemaEntityDefinition> elementDefEntry : getEntities().entrySet()) {
            if (null == elementDefEntry.getValue()) {
                throw new SchemaException("Entity definition was null for group: " + elementDefEntry.getKey());
            }

            if (!elementDefEntry.getValue().validate(hasAggregators)) {
                LOGGER.warn("VALIDITY ERROR: Invalid entity definition for group: " + elementDefEntry.getKey());
                return false;
            }
        }
        return true;
    }

    public Map<String, TypeDefinition> getTypes() {
        return types;
    }

    public TypeDefinition getType(final String typeName) {
        TypeDefinition typeDef = types.get(typeName);
        if (null == typeDef) {
            typeDef = unknownType;
        }

        return typeDef;
    }

    /**
     * Returns the vertex serialiser for this schema.
     * <p>
     * There can be only one vertex serialiser for all elements because in order for searches to work correctly,
     * the byte representation of the search term's (seeds) must match the byte representation stored,
     * i.e you need to know how your results have been serialised which effectively means all vertices must be serialised the same way within a table.
     *
     * @return An implementation of {@link uk.gov.gchq.gaffer.serialisation.Serialisation} that will be used to serialise all vertices.
     */
    @JsonIgnore
    public Serialisation getVertexSerialiser() {
        return vertexSerialiser;
    }

    public String getVertexSerialiserClass() {
        if (null == vertexSerialiser) {
            return null;
        }

        return vertexSerialiser.getClass().getName();
    }

    @Override
    public SchemaElementDefinition getElement(final String group) {
        return (SchemaElementDefinition) super.getElement(group);
    }

    public String getVisibilityProperty() {
        return visibilityProperty;
    }

    public String getTimestampProperty() {
        return timestampProperty;
    }

    @Override
    public String toString() {
        try {
            return "Schema" + new String(toJson(true), CommonConstants.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] toCompactJson() throws SchemaException {
        return toJson(false, "description");
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> extends ElementDefinitions.BaseBuilder<Schema, SchemaEntityDefinition, SchemaEdgeDefinition, CHILD_CLASS> {
        public BaseBuilder() {
            super(new Schema());
        }

        protected BaseBuilder(final Schema schema) {
            super(schema);
        }

        @Override
        public CHILD_CLASS entity(final String group) {
            return entity(group, new SchemaEntityDefinition());
        }

        @Override
        public CHILD_CLASS edge(final String group) {
            return edge(group, new SchemaEdgeDefinition());
        }

        /**
         * Sets the {@link uk.gov.gchq.gaffer.serialisation.Serialisation}.
         *
         * @param vertexSerialiser the {@link uk.gov.gchq.gaffer.serialisation.Serialisation} to set
         * @return this Builder
         */
        public CHILD_CLASS vertexSerialiser(final Serialisation vertexSerialiser) {
            getThisSchema().vertexSerialiser = vertexSerialiser;
            return self();
        }

        /**
         * Sets the {@link uk.gov.gchq.gaffer.serialisation.Serialisation} from class name.
         *
         * @param vertexSerialiserClass the {@link uk.gov.gchq.gaffer.serialisation.Serialisation} class name to set
         * @return this Builder
         */
        public CHILD_CLASS vertexSerialiserClass(final String vertexSerialiserClass) {
            if (null == vertexSerialiserClass) {
                getThisSchema().vertexSerialiser = null;
            } else {
                Class<? extends Serialisation> serialiserClass;
                try {
                    serialiserClass = Class.forName(vertexSerialiserClass).asSubclass(Serialisation.class);
                } catch (ClassNotFoundException e) {
                    throw new SchemaException(e.getMessage(), e);
                }
                try {
                    vertexSerialiser(serialiserClass.newInstance());
                } catch (IllegalAccessException | IllegalArgumentException | SecurityException | InstantiationException e) {
                    throw new SchemaException(e.getMessage(), e);
                }
            }

            return self();
        }

        public CHILD_CLASS type(final String typeName, final TypeDefinition type) {
            getThisSchema().types.put(typeName, type);
            return self();
        }

        public CHILD_CLASS type(final String typeName, final Class<?> typeClass) {
            return type(typeName, new TypeDefinition(typeClass));
        }

        public CHILD_CLASS types(final Map<String, TypeDefinition> types) {
            getThisSchema().types.clear();
            getThisSchema().types.putAll(types);
            return self();
        }

        public CHILD_CLASS visibilityProperty(final String visibilityProperty) {
            getThisSchema().visibilityProperty = visibilityProperty;
            return self();
        }

        public CHILD_CLASS timestampProperty(final String timestampProperty) {
            getThisSchema().timestampProperty = timestampProperty;
            return self();
        }

        @Override
        @JsonIgnore
        public CHILD_CLASS merge(final Schema schema) {
            validateSharedGroups(getThisSchema().getEntityGroups(), schema.getEntityGroups());
            validateSharedGroups(getThisSchema().getEdgeGroups(), schema.getEdgeGroups());

            if (getThisSchema().getEntities().isEmpty()) {
                getThisSchema().getEntities().putAll(schema.getEntities());
            } else {
                for (final Map.Entry<String, SchemaEntityDefinition> entry : schema.getEntities().entrySet()) {
                    if (!getThisSchema().getEntities().containsKey(entry.getKey())) {
                        entity(entry.getKey(), entry.getValue());
                    } else {
                        final SchemaEntityDefinition mergedElementDef = new SchemaEntityDefinition.Builder()
                                .merge(getThisSchema().getEntities().get(entry.getKey()))
                                .merge(entry.getValue())
                                .build();
                        getThisSchema().getEntities().put(entry.getKey(), mergedElementDef);
                    }
                }
            }

            if (getThisSchema().getEdges().isEmpty()) {
                getThisSchema().getEdges().putAll(schema.getEdges());
            } else {
                for (final Map.Entry<String, SchemaEdgeDefinition> entry : schema.getEdges().entrySet()) {
                    if (!getThisSchema().getEdges().containsKey(entry.getKey())) {
                        edge(entry.getKey(), entry.getValue());
                    } else {
                        final SchemaEdgeDefinition mergedElementDef = new SchemaEdgeDefinition.Builder()
                                .merge(getThisSchema().getEdges().get(entry.getKey()))
                                .merge(entry.getValue())
                                .build();
                        getThisSchema().getEdges().put(entry.getKey(), mergedElementDef);
                    }
                }
            }

            if (null != schema.getVertexSerialiser()) {
                if (null == getThisSchema().vertexSerialiser) {
                    getThisSchema().vertexSerialiser = schema.getVertexSerialiser();
                } else if (!getThisSchema().vertexSerialiser.getClass().equals(schema.getVertexSerialiser().getClass())) {
                    throw new SchemaException("Unable to merge schemas. Conflict with vertex serialiser, options are: "
                            + getThisSchema().vertexSerialiser.getClass().getName() + " and " + schema.getVertexSerialiser().getClass().getName());
                }
            }

            if (null == getThisSchema().visibilityProperty) {
                getThisSchema().visibilityProperty = schema.getVisibilityProperty();
            } else if (null != schema.getVisibilityProperty() && !getThisSchema().visibilityProperty.equals(schema.getVisibilityProperty())) {
                throw new SchemaException("Unable to merge schemas. Conflict with visibility property, options are: "
                        + getThisSchema().visibilityProperty + " and " + schema.getVisibilityProperty());
            }

            if (null == getThisSchema().timestampProperty) {
                getThisSchema().timestampProperty = schema.getTimestampProperty();
            } else if (null != schema.getTimestampProperty() && !getThisSchema().timestampProperty.equals(schema.getTimestampProperty())) {
                throw new SchemaException("Unable to merge schemas. Conflict with timestamp property, options are: "
                        + getThisSchema().timestampProperty + " and " + schema.getTimestampProperty());
            }

            if (getThisSchema().types.isEmpty()) {
                getThisSchema().types.putAll(schema.types);
            } else {
                for (final Entry<String, TypeDefinition> entry : schema.types.entrySet()) {
                    final String newType = entry.getKey();
                    final TypeDefinition newTypeDef = entry.getValue();
                    final TypeDefinition typeDef = getThisSchema().types.get(newType);
                    if (null == typeDef) {
                        getThisSchema().types.put(newType, newTypeDef);
                    } else {
                        typeDef.merge(newTypeDef);
                    }
                }
            }

            return self();
        }

        @JsonIgnore
        public CHILD_CLASS json(final InputStream... inputStreams) throws SchemaException {
            return json(Schema.class, inputStreams);
        }

        @JsonIgnore
        public CHILD_CLASS json(final Path... filePaths) throws SchemaException {
            return json(Schema.class, filePaths);
        }

        @JsonIgnore
        public CHILD_CLASS json(final byte[]... jsonBytes) throws SchemaException {
            return json(Schema.class, jsonBytes);
        }

        @Override
        public Schema build() {
            for (final SchemaElementDefinition elementDef : getThisSchema().getEntities().values()) {
                elementDef.schemaReference = getThisSchema();
            }

            for (final SchemaElementDefinition elementDef : getThisSchema().getEdges().values()) {
                elementDef.schemaReference = getThisSchema();
            }

            expandElementDefinitions(getThisSchema());

            getThisSchema().types = Collections.unmodifiableMap(getThisSchema().types);

            return super.build();
        }

        private Schema getThisSchema() {
            return getElementDefs();
        }

        private void validateSharedGroups(final Set<String> groupsA, final Set<String> groupsB) {
            final Set<String> sharedGroups = new HashSet<>(groupsA);
            sharedGroups.retainAll(groupsB);
            if (!sharedGroups.isEmpty()) {
                throw new SchemaException("Element groups cannot be shared across different schema files/parts. Each group must be fully defined in a single schema. Please fix these groups: " + sharedGroups);
            }
        }

        private void expandElementDefinitions(final Schema schema) {
            for (final Entry<String, SchemaEdgeDefinition> entry : Lists.newArrayList(schema.getEdges().entrySet())) {
                schema.getEdges().put(entry.getKey(), entry.getValue().getExpandedDefinition());
            }

            for (final Entry<String, SchemaEntityDefinition> entry : Lists.newArrayList(schema.getEntities().entrySet())) {
                schema.getEntities().put(entry.getKey(), entry.getValue().getExpandedDefinition());
            }
        }
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final Schema schema) {
            merge(schema);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
