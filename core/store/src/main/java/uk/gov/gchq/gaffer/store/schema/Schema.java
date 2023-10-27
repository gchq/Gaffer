/*
 * Copyright 2016-2023 Crown Copyright
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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.commonutil.GroupUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.schema.exception.SplitElementGroupDefSchemaException;
import uk.gov.gchq.gaffer.store.schema.exception.VertexSerialiserSchemaException;
import uk.gov.gchq.gaffer.store.schema.exception.VisibilityPropertySchemaException;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.iterable.ChainedIterable;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

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
@JsonPropertyOrder(value = {"class", "edges", "entities", "types"}, alphabetic = true)
public class Schema extends ElementDefinitions<SchemaEntityDefinition, SchemaEdgeDefinition> implements Cloneable {
    public static final String FORMAT_EXCEPTION = "%s, options are: %s and %s";
    public static final String FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S = "Unable to merge schemas because of conflict with the %s";
    public static final String FORMAT_ERROR_WITH_THE_SCHEMA_TYPE_NAMED_S_DUE_TO_S = "Error with the schema type named:%s due to: %s";
    public static final String ERROR_MERGING_SCHEMA_DUE_TO = "Error merging Schema due to: ";
    private final TypeDefinition unknownType = new TypeDefinition();

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

    private Map<String, String> config;

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
            if (elementDef != null && elementDef.hasValidation()) {
                return true;
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

    @Override
    public SchemaElementDefinition getElement(final String group) {
        return (SchemaElementDefinition) super.getElement(group);
    }

    public String getVisibilityProperty() {
        return visibilityProperty;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public String getConfig(final String key) {
        return null != config ? config.get(key) : null;
    }

    public void addConfig(final String key, final String value) {
        if (null == config) {
            config = new HashMap<>();
        }
        config.put(key, value);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append(new String(toJson(true), StandardCharsets.UTF_8))
                .build();
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

        public CHILD_CLASS config(final Map<String, String> config) {
            getThisSchema().config = config;
            return self();
        }

        public CHILD_CLASS config(final String key, final String value) {
            getThisSchema().addConfig(key, value);
            return self();
        }

        @Override
        @JsonIgnore
        public CHILD_CLASS merge(final Schema schema) {
            if (nonNull(schema)) {
                try {
                    Schema thatSchema = JSONSerialiser.deserialise(JSONSerialiser.serialise(schema), Schema.class);

                    validateSharedGroupsAreCompatible(thatSchema);

                    mergeElements(thatSchema);

                    mergeVertexSerialiser(thatSchema);

                    mergeVisibility(thatSchema);

                    mergeTypes(thatSchema);

                    mergeConfig(thatSchema);

                } catch (final SchemaException e) {
                    throw e.prependToMessage(ERROR_MERGING_SCHEMA_DUE_TO);
                } catch (final Exception e) {
                    throw new SchemaException(ERROR_MERGING_SCHEMA_DUE_TO + e.getMessage(), e);
                }
            }

            return self();
        }

        private void mergeEntities(final Schema thatSchema) {
            if (getThisSchema().getEntities().isEmpty()) {
                getThisSchema().getEntities().putAll(thatSchema.getEntities());
            } else {
                for (final Entry<String, SchemaEntityDefinition> entry : thatSchema.getEntities().entrySet()) {
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
        }

        private void mergeEdges(final Schema thatSchema) {
            if (getThisSchema().getEdges().isEmpty()) {
                getThisSchema().getEdges().putAll(thatSchema.getEdges());
            } else {
                for (final Entry<String, SchemaEdgeDefinition> entry : thatSchema.getEdges().entrySet()) {
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
        }

        private void mergeVertexSerialiser(final Schema thatSchema) {
            if (null != thatSchema.getVertexSerialiser()) {
                if (null == getThisSchema().vertexSerialiser) {
                    getThisSchema().vertexSerialiser = thatSchema.getVertexSerialiser();
                } else if (!getThisSchema().vertexSerialiser.getClass().equals(thatSchema.getVertexSerialiser().getClass())) {
                    throw new VertexSerialiserSchemaException(getThisSchema().vertexSerialiser.getClass().getName(), thatSchema.getVertexSerialiser().getClass().getName());
                }
            }
        }

        private void mergeVisibility(final Schema thatSchema) {
            if (null == getThisSchema().visibilityProperty) {
                getThisSchema().visibilityProperty = thatSchema.getVisibilityProperty();
            } else if (null != thatSchema.getVisibilityProperty() && !getThisSchema().visibilityProperty.equals(thatSchema.getVisibilityProperty())) {
                throw new VisibilityPropertySchemaException(getThisSchema().visibilityProperty, thatSchema.getVisibilityProperty());
            }
        }

        private void mergeTypes(final Schema thatSchema) {
            if (getThisSchema().types.isEmpty()) {
                getThisSchema().types.putAll(thatSchema.types);
            } else {
                for (final Entry<String, TypeDefinition> entry : thatSchema.types.entrySet()) {
                    final String newType = entry.getKey();
                    final TypeDefinition newTypeDef = entry.getValue();
                    final TypeDefinition typeDef = getThisSchema().types.get(newType);
                    if (null == typeDef) {
                        getThisSchema().types.put(newType, newTypeDef);
                    } else {
                        try {
                            typeDef.merge(newTypeDef);
                        } catch (final SchemaException e) {
                            throw e.prependToMessage(String.format(FORMAT_ERROR_WITH_THE_SCHEMA_TYPE_NAMED_S_DUE_TO_S, entry.getKey(), ""));
                        } catch (final Exception e) {
                            throw new SchemaException(String.format(FORMAT_ERROR_WITH_THE_SCHEMA_TYPE_NAMED_S_DUE_TO_S, entry.getKey(), e.getMessage()), e);
                        }
                    }
                }
            }
        }

        private void mergeConfig(final Schema thatSchema) {
            if (null == getThisSchema().config) {
                getThisSchema().config = thatSchema.config;
            } else if (null != thatSchema.config) {
                getThisSchema().config.putAll(thatSchema.config);
            }
        }

        private void mergeElements(final Schema thatSchema) {
            mergeEntities(thatSchema);
            mergeEdges(thatSchema);
        }

        private void validateSharedGroupsAreCompatible(final Schema thatSchema) {
            final String thisVisibilityProperty = getThisSchema().visibilityProperty;
            final String thatVisibilityProperty = thatSchema.visibilityProperty;
            validateSharedGroupsAreCompatible(getThisSchema().getEntities(), thatSchema.getEntities(), thisVisibilityProperty, thatVisibilityProperty);
            validateSharedGroupsAreCompatible(getThisSchema().getEdges(), thatSchema.getEdges(), thisVisibilityProperty, thatVisibilityProperty);
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

        private void validateSharedGroupsAreCompatible(final Map<String, ? extends SchemaElementDefinition> thisElements, final Map<String, ? extends SchemaElementDefinition> thatElements, final String thisVisibilityProperty, final String thatVisibilityProperty) {
            final Set<String> sharedGroups = new HashSet<>(thisElements.keySet());
            sharedGroups.retainAll(thatElements.keySet());

            if (!sharedGroups.isEmpty()) {
                // Groups are shared. Check they are compatible.
                for (final String sharedGroup : sharedGroups) {

                    // Check if just one group has the properties and groupBy fields set.
                    final SchemaElementDefinition thisElementDef = thisElements.get(sharedGroup);

                    if ((null == thisElementDef.properties || thisElementDef.properties.isEmpty())
                            && thisElementDef.groupBy.isEmpty()) {
                        continue;
                    }
                    final SchemaElementDefinition thatElementDef = thatElements.get(sharedGroup);
                    if ((null == thatElementDef.properties || thatElementDef.properties.isEmpty())
                            && thatElementDef.groupBy.isEmpty()) {
                        continue;
                    }

                    final Map<String, String> thisProperties = getPropertiesWithoutVisibility(thisVisibilityProperty, thisElementDef);
                    final Map<String, String> thatProperties = getPropertiesWithoutVisibility(thatVisibilityProperty, thatElementDef);

                    // Check to see if the properties are the same.
                    if (Objects.equals(thisProperties, thatProperties)
                            && Objects.equals(thisElementDef.groupBy, thatElementDef.groupBy)) {
                        continue;
                    }

                    // Check to see if either of the properties are a subset of another properties
                    if (thatProperties.entrySet().containsAll(thisProperties.entrySet())
                            || thisProperties.entrySet().containsAll(thatProperties.entrySet())) {
                        continue;
                    }

                    throw new SplitElementGroupDefSchemaException(sharedGroup);
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

        private static Map<String, String> getPropertiesWithoutVisibility(final String visibilityProperty, final SchemaElementDefinition elementDef) {
            return elementDef.properties.entrySet().stream()
                    .filter(s -> !Objects.equals(s.getKey(), visibilityProperty))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
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
