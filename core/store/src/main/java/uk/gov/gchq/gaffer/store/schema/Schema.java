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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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

    /**
     * The {@link uk.gov.gchq.gaffer.serialisation.Serialisation} for all vertices.
     */
    private Serialisation vertexSerialiser;

    /**
     * A map of custom type name to {@link TypeDefinition}.
     *
     * @see TypeDefinitions
     * @see TypeDefinition
     */
    private final TypeDefinitions types;

    private String visibilityProperty;

    private String timestampProperty;

    protected Schema() {
        this(new TypeDefinitions());
    }

    protected Schema(final TypeDefinitions types) {
        this.types = types;
    }

    public <T extends SchemaElementDefinition> T expandChild(final T childDefinition, final T parentDefinition) {
        if (childDefinition.getGroupBy().isEmpty()) {
            childDefinition.setGroupBy(parentDefinition.getGroupBy());
        }
        LinkedHashMap<String, String> childProps = new LinkedHashMap<>(childDefinition.getPropertyMap());
        Set<String> childPropertyNames = childProps.keySet();
        Map<String, String> parentProperties = parentDefinition.getPropertyMap();
        for (final Map.Entry<String, String> entry : parentProperties.entrySet()) {
            String parentPropName = entry.getKey();
            if (!childPropertyNames.contains(parentPropName)) {
                childProps.put(parentPropName, entry.getValue());
            }
        }
        childDefinition.setPropertyMap(childProps);
        Map<IdentifierType, String> identifiers = childDefinition.getIdentifierMap();
        for (final IdentifierType identifierType : parentDefinition.getIdentifiers()) {
            if (!childDefinition.containsIdentifier(identifierType)) {
                identifiers.put(identifierType, parentDefinition.getIdentifierTypeName(identifierType));
            }
        }
        return childDefinition;
    }

    public <T extends SchemaElementDefinition> T collapseChild(final T childDefinition, final T parentDefinition) {
        if (childDefinition.getGroupBy().equals(parentDefinition.getGroupBy())) {
            childDefinition.setGroupBy(new LinkedHashSet<String>());
        }

        LinkedHashMap<String, String> props = new LinkedHashMap<>(childDefinition.getPropertyMap());
        for (final String prop : parentDefinition.getProperties()) {
            if (!props.keySet().contains(prop)) {
                props.put(prop, parentDefinition.getPropertyMap().get(prop));
            }
        }

        LinkedHashMap<String, String> childProps = new LinkedHashMap<>(childDefinition.getPropertyMap());
        Set<String> childPropertyNames = childProps.keySet();
        Map<String, String> parentProperties = parentDefinition.getPropertyMap();
        for (final Map.Entry<String, String> entry : parentProperties.entrySet()) {
            String propName = entry.getKey();
            if (childPropertyNames.contains(propName) && entry.getValue().equals(childProps.get(propName))) {
                childProps.remove(propName);
            }
        }
        childDefinition.setPropertyMap(childProps);

        Map<IdentifierType, String> identifiers = childDefinition.getIdentifierMap();
        for (final IdentifierType identifierType : parentDefinition.getIdentifiers()) {
            if (childDefinition.containsIdentifier(identifierType) && childDefinition.getIdentifierTypeName(identifierType).equals(parentDefinition.getIdentifierTypeName(identifierType))) {
                identifiers.remove(identifierType);
            }
        }
        return childDefinition;
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Uses toJson instead.")
    @Override
    public Schema clone() {
        return new Schema.Builder().json(toJson(false)).build();
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

        for (final Entry<String, SchemaEdgeDefinition> elementDefEntry : getEdges().entrySet()) {
            if (null == elementDefEntry.getValue()) {
                throw new SchemaException("Edge definition was null for group: " + elementDefEntry.getKey());
            }

            if (!elementDefEntry.getValue().validate()) {
                LOGGER.warn("VALIDITY ERROR: Invalid edge definition for group: " + elementDefEntry.getKey());
                return false;
            }
        }

        for (final Entry<String, SchemaEntityDefinition> elementDefEntry : getEntities().entrySet()) {
            if (null == elementDefEntry.getValue()) {
                throw new SchemaException("Entity definition was null for group: " + elementDefEntry.getKey());
            }

            if (!elementDefEntry.getValue().validate()) {
                LOGGER.warn("VALIDITY ERROR: Invalid entity definition for group: " + elementDefEntry.getKey());
                return false;
            }
        }
        return true;
    }

    public TypeDefinitions getTypes() {
        return types;
    }

    public TypeDefinition getType(final String typeName) {
        return types.getType(typeName);
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

    @JsonGetter("edges")
    protected Map<String, SchemaEdgeDefinition> getEdgesCollapsed() throws SchemaException {
        Map<String, SchemaEdgeDefinition> edges = super.getEdges();
        for (final SchemaElementDefinition elementDef : edges.values()) {

            if (null != elementDef.getParentGroup()) {
                SchemaElementDefinition parentDefinition = getEdge(elementDef.getParentGroup());
                if (null == parentDefinition) {
                    throw new SchemaException("Attempted to get an Invalid edge, the parent group \"" + elementDef.getParentGroup() + "\" specified could not be found.");
                }
                collapseChild(elementDef, parentDefinition);
            }
        }
        return edges;
    }

    @JsonGetter("entities")
    protected Map<String, SchemaEntityDefinition> getEntitiesCollapsed() throws SchemaException {
        Map<String, SchemaEntityDefinition> entities = super.getEntities();
        for (final SchemaEntityDefinition elementDef : entities.values()) {
            if (null != elementDef.getParentGroup()) {
                SchemaElementDefinition parentDefinition = getEntity(elementDef.getParentGroup());
                if (null == parentDefinition) {
                    throw new SchemaException("Attempted to get an Invalid entity, the parent group \"" + elementDef.getParentGroup() + "\" specified could not be found.");
                }
                collapseChild(elementDef, parentDefinition);
            }
        }
        return entities;
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

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static class Builder extends ElementDefinitions.Builder<SchemaEntityDefinition, SchemaEdgeDefinition> {
        public Builder() {
            this(new Schema());
        }

        public Builder(final Schema schema) {
            super(schema);
        }

        /**
         * Sets the {@link uk.gov.gchq.gaffer.serialisation.Serialisation}.
         *
         * @param vertexSerialiser the {@link uk.gov.gchq.gaffer.serialisation.Serialisation} to set
         * @return this Builder
         */
        public Builder vertexSerialiser(final Serialisation vertexSerialiser) {
            getThisSchema().vertexSerialiser = vertexSerialiser;
            return this;
        }

        /**
         * Sets the {@link uk.gov.gchq.gaffer.serialisation.Serialisation} from class name.
         *
         * @param vertexSerialiserClass the {@link uk.gov.gchq.gaffer.serialisation.Serialisation} class name to set
         * @return this Builder
         */
        public Builder vertexSerialiserClass(final String vertexSerialiserClass) {
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

            return this;
        }

        @Override
        public Builder edge(final String group, final SchemaEdgeDefinition edgeDef) {
            edgeDef.setTypesLookup(getThisSchema().types);
            return (Builder) super.edge(group, edgeDef);
        }

        public Builder edge(final String group) {
            return edge(group, new SchemaEdgeDefinition());
        }

        @Override
        public Builder edges(final Map<String, SchemaEdgeDefinition> edges) {
            return (Builder) super.edges(edges);
        }

        @Override
        public Builder entity(final String group, final SchemaEntityDefinition entityDef) {
            entityDef.setTypesLookup(getThisSchema().types);
            return (Builder) super.entity(group, entityDef);
        }

        public Builder entity(final String group) {
            return entity(group, new SchemaEntityDefinition());
        }

        @Override
        public Builder entities(final Map<String, SchemaEntityDefinition> entities) {
            return (Builder) super.entities(entities);
        }

        public Builder type(final String typeName, final TypeDefinition type) {
            getThisSchema().types.put(typeName, type);
            return this;
        }

        public Builder type(final String typeName, final Class<?> typeClass) {
            return type(typeName, new TypeDefinition(typeClass));
        }

        public Builder types(final TypeDefinitions types) {
            getThisSchema().types.putAll(types);
            return this;
        }

        public Builder visibilityProperty(final String visibilityProperty) {
            getThisSchema().visibilityProperty = visibilityProperty;
            return this;
        }

        public Builder timestampProperty(final String timestampProperty) {
            getThisSchema().timestampProperty = timestampProperty;
            return this;
        }

        @JsonIgnore
        @Override
        public Builder merge(final ElementDefinitions<SchemaEntityDefinition, SchemaEdgeDefinition> elementDefs) {
            if (elementDefs instanceof Schema) {
                return merge(((Schema) elementDefs));
            } else {
                return (Builder) super.merge(elementDefs);
            }
        }

        @JsonIgnore
        public Builder merge(final Schema schema) {
            super.merge(schema);

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

            getThisSchema().types.merge(schema.getTypes());
            return this;
        }

        @JsonIgnore
        public Builder json(final InputStream... inputStreams) throws SchemaException {
            return (Builder) json(Schema.class, inputStreams);
        }

        @JsonIgnore
        public Builder json(final Path... filePaths) throws SchemaException {
            return (Builder) json(Schema.class, filePaths);
        }

        @JsonIgnore
        public Builder json(final byte[]... jsonBytes) throws SchemaException {
            return (Builder) json(Schema.class, jsonBytes);
        }

        @Override
        public Schema build() {
            getThisSchema().types.lock();
            for (SchemaEdgeDefinition schemaEdgeDefinition : getThisSchema().getEdges().values()) {
                schemaEdgeDefinition.
            }

            return (Schema) super.build();
        }

        @JsonIgnore
        @Override
        protected Schema getElementDefs() {
            return (Schema) super.getElementDefs();
        }

        private Schema getThisSchema() {
            return getElementDefs();
        }
    }
}
