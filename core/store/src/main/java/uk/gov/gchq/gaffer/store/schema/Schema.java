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
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.GroupUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.koryphe.ValidationResult;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * <p>
 * Contains the full list of {@link uk.gov.gchq.gaffer.data.element.Element} types to be stored in the graph.
 * </p>
 * <p>
 * Each type of element should have the identifier type(s) listed and a map of property names and their corresponding types.
 * Each type can either be a full java class name or a custom type. Using custom types then allows you to specify
 * validation and aggregation for the element components.
 * </p>
 * <p>
 * This class must be JSON serialisable.
 * A schema should normally be written in JSON and then it will be automatically deserialised at runtime.
 * An example of a JSON schemas can be found in the Example module.
 * </p>
 *
 * @see Schema.Builder
 * @see ElementDefinitions
 */
@JsonDeserialize(builder = Schema.Builder.class)
public class Schema extends ElementDefinitions<SchemaEntityDefinition, SchemaEdgeDefinition> implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementDefinitions.class);
    private final TypeDefinition unknownType = new TypeDefinition();

    /**
     * @deprecated the ID should not be used. The ID should be supplied to the graph library separately
     */
    @Deprecated
    private String id;

    /**
     * The {@link Serialiser} for all vertices.
     */
    private Serialiser vertexSerialiser;

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

    /**
     * @return schema id
     * @deprecated the ID should be supplied to the graph library separately
     */
    @Deprecated
    public String getId() {
        return id;
    }

    /**
     * @param id the schema id
     * @deprecated the ID should be supplied to the graph library separately
     */
    @Deprecated
    public void setId(final String id) {
        this.id = id;
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
    @JsonIgnore
    public boolean isAggregationEnabled() {
        boolean isEnabled = false;

        for (final Entry<String, ? extends SchemaElementDefinition> entry : getElementDefinitions()) {
            if (null != entry.getValue() && entry.getValue().isAggregate()) {
                isEnabled = true;
                break;
            }
        }

        return isEnabled;
    }

    @JsonIgnore
    public List<String> getAggregatedGroups() {
        final List<String> groups = new ArrayList<>();

        for (final Entry<String, ? extends SchemaElementDefinition> entry : getElementDefinitions()) {
            if (null != entry.getValue() && entry.getValue().isAggregate()) {
                groups.add(entry.getKey());
            }
        }

        return groups;
    }

    private Iterable<Map.Entry<String, ? extends SchemaElementDefinition>> getElementDefinitions() {
        if (null == getEntities()) {
            return (Iterable) getEdges().entrySet();
        }

        if (null == getEdges()) {
            return (Iterable) getEntities().entrySet();
        }

        return new ChainedIterable<>(getEntities().entrySet(), getEdges().entrySet());
    }

    /**
     * Validates the schema to ensure all element definitions are valid.
     * Throws a SchemaException if it is not valid.
     *
     * @return ValidationResult the validation result
     * @throws SchemaException if validation fails then a SchemaException is thrown.
     */
    public ValidationResult validate() throws SchemaException {
        final ValidationResult result = new ValidationResult();
        for (final String edgeGroup : getEdgeGroups()) {
            if (null != getEntity(edgeGroup)) {
                result.addError("Groups must not be shared between Entity definitions and Edge definitions."
                        + "Found edgeGroup '" + edgeGroup + "' in the collection of entities");
            }
        }

        for (final Entry<String, SchemaEdgeDefinition> elementDefEntry : getEdges().entrySet()) {
            if (null == elementDefEntry.getValue()) {
                throw new SchemaException("Edge definition was null for group: " + elementDefEntry.getKey());
            }
            result.add(elementDefEntry.getValue().validate(), "VALIDITY ERROR: Invalid edge definition for group: " + elementDefEntry.getKey());
        }

        for (final Entry<String, SchemaEntityDefinition> elementDefEntry : getEntities().entrySet()) {
            if (null == elementDefEntry.getValue()) {
                throw new SchemaException("Entity definition was null for group: " + elementDefEntry.getKey());
            }
            result.add(elementDefEntry.getValue().validate(), "VALIDITY ERROR: Invalid entity definition for group: " + elementDefEntry.getKey());
        }
        return result;
    }

    public boolean hasValidation() {
        for (final SchemaElementDefinition elementDef : new ChainedIterable<SchemaElementDefinition>(getEntities().values(), getEdges().values())) {
            if (null != elementDef) {
                if (elementDef.hasValidation()) {
                    return true;
                }
            }
        }
        return false;
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
     * <p>
     * Returns the vertex serialiser for this schema.
     * </p>
     * <p>
     * There can be only one vertex serialiser for all elements because in order for searches to work correctly,
     * the byte representation of the search term's (seeds) must match the byte representation stored,
     * i.e you need to know how your results have been serialised which effectively means all vertices must be serialised the same way within a table.
     * </p>
     *
     * @return An implementation of {@link Serialiser} that will be used to serialise all vertices.
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Serialiser getVertexSerialiser() {
        return vertexSerialiser;
    }

    @Deprecated
    @JsonIgnore
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
            return new ToStringBuilder(this)
                    .append(new String(toJson(true), CommonConstants.UTF_8))
                    .build();
        } catch (final UnsupportedEncodingException e) {
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

        /**
         * @param id the id.
         * @return this builder.
         * @deprecated the ID should not be used. The ID should be supplied to the graph library separately
         */
        @Deprecated
        public CHILD_CLASS id(final String id) {
            getThisSchema().id = id;
            return self();
        }

        /**
         * Sets the {@link Serialiser}.
         *
         * @param vertexSerialiser the {@link Serialiser} to set
         * @return this Builder
         */
        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        public CHILD_CLASS vertexSerialiser(final Serialiser vertexSerialiser) {
            getThisSchema().vertexSerialiser = vertexSerialiser;
            return self();
        }

        /**
         * Sets the {@link Serialiser} from class name.
         *
         * @param vertexSerialiserClass the {@link Serialiser} class name to set
         * @return this Builder
         */
        @Deprecated
        @JsonSetter("vertexSerialiserClass")
        public CHILD_CLASS vertexSerialiserClass(final String vertexSerialiserClass) {
            if (null == vertexSerialiserClass) {
                getThisSchema().vertexSerialiser = null;
            } else {
                Class<? extends Serialiser> serialiserClass;
                try {
                    serialiserClass = Class.forName(vertexSerialiserClass).asSubclass(Serialiser.class);
                } catch (final ClassNotFoundException e) {
                    throw new SchemaException(e.getMessage(), e);
                }
                try {
                    vertexSerialiser(serialiserClass.newInstance());
                } catch (final IllegalAccessException | IllegalArgumentException | SecurityException | InstantiationException e) {
                    throw new SchemaException(e.getMessage(), e);
                }
            }

            return self();
        }

        public CHILD_CLASS type(final String typeName, final TypeDefinition type) {
            getThisSchema().types.put(typeName, null != type ? type : new TypeDefinition());
            return self();
        }

        public CHILD_CLASS type(final String typeName, final Class<?> typeClass) {
            return type(typeName, null != typeClass ? new TypeDefinition(typeClass) : null);
        }

        public CHILD_CLASS types(final Map<String, TypeDefinition> types) {
            getThisSchema().types.clear();
            if (null != types) {
                getThisSchema().types.putAll(types);
            }
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
            if (null != schema) {
                validateSharedGroups(getThisSchema().getEntities(), schema.getEntities());
                validateSharedGroups(getThisSchema().getEdges(), schema.getEdges());

                // Schema ID is deprecated - remove this when ID is removed.
                if (null == getThisSchema().getId()) {
                    getThisSchema().setId(schema.getId());
                } else if (null != schema.getId()
                        && !schema.getId().equals(getThisSchema().getId())) {
                    getThisSchema().setId(getThisSchema().getId() + "_" + schema.getId());
                }

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
            validateGroupNames();

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

        private void validateSharedGroups(final Map<String, ? extends SchemaElementDefinition> elements1, final Map<String, ? extends SchemaElementDefinition> elements2) {
            final Set<String> sharedGroups = new HashSet<>(elements1.keySet());
            sharedGroups.retainAll(elements2.keySet());
            if (!sharedGroups.isEmpty()) {
                // Groups are shared. Check they are compatible.
                for (final String sharedGroup : sharedGroups) {

                    // Check if just one group has the properties and groupBy fields set.
                    final SchemaElementDefinition elementDef1 = elements1.get(sharedGroup);
                    if ((null == elementDef1.properties || elementDef1.properties.isEmpty())
                            && elementDef1.groupBy.isEmpty()) {
                        continue;
                    }
                    final SchemaElementDefinition elementDef2 = elements2.get(sharedGroup);
                    if ((null == elementDef2.properties || elementDef2.properties.isEmpty())
                            && elementDef2.groupBy.isEmpty()) {
                        continue;
                    }

                    // Check to see if the properties are the same.
                    if (Objects.equals(elementDef1.properties, elementDef2.properties)
                            && Objects.equals(elementDef1.groupBy, elementDef2.groupBy)) {
                        continue;
                    }

                    throw new SchemaException("Element group properties cannot be defined in different schema parts, they must all be defined in a single schema part. "
                            + "Please fix this group: " + sharedGroup);
                }
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

        private void validateGroupNames() {
            getThisSchema().getEdgeGroups().forEach(GroupUtil::validateName);

            getThisSchema().getEntityGroups().forEach(GroupUtil::validateName);
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
