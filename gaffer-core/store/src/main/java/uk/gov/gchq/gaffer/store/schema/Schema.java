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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Contains the full list of {@link gaffer.data.element.Element} types to be stored in the graph.
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
public class Schema extends ElementDefinitions<SchemaEntityDefinition, SchemaEdgeDefinition> implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementDefinitions.class);

    /**
     * The {@link gaffer.serialisation.Serialisation} for all vertices.
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

    public Schema() {
        this(new TypeDefinitions());
    }

    protected Schema(final TypeDefinitions types) {
        this.types = types;
    }

    public static Schema fromJson(final InputStream... inputStreams) throws SchemaException {
        return fromJson(Schema.class, inputStreams);
    }

    public static Schema fromJson(final Path... filePaths) throws SchemaException {
        return fromJson(Schema.class, filePaths);
    }

    public static Schema fromJson(final byte[]... jsonBytes) throws SchemaException {
        return fromJson(Schema.class, jsonBytes);
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Uses toJson instead.")
    @Override
    public Schema clone() {
        return fromJson(toJson(false));
    }

    /**
     * Validates the schema to ensure all element definitions are valid.
     * Throws a SchemaException if it is not valid.
     *
     * @return true if valid, otherwise false.
     * @throws SchemaException if validation fails then a SchemaException is thrown.
     */
    public boolean validate() throws SchemaException {
        for (String edgeGroup : getEdgeGroups()) {
            if (null != getEntity(edgeGroup)) {
                LOGGER.warn("Groups must not be shared between Entity definitions and Edge definitions."
                        + "Found edgeGroup '" + edgeGroup + "' in the collection of entities");
                return false;
            }
        }

        for (Map.Entry<String, SchemaEdgeDefinition> elementDefEntry : getEdges().entrySet()) {
            if (null == elementDefEntry.getValue()) {
                throw new SchemaException("Edge definition was null for group: " + elementDefEntry.getKey());
            }

            if (!elementDefEntry.getValue().validate()) {
                LOGGER.warn("VALIDITY ERROR: Invalid edge definition for group: " + elementDefEntry.getKey());
                return false;
            }
        }

        for (Map.Entry<String, SchemaEntityDefinition> elementDefEntry : getEntities().entrySet()) {
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

    /**
     * This does not override the current types it just appends the additional types.
     *
     * @param newTypes the new types to be added.
     */
    @JsonSetter("types")
    public void addTypes(final TypeDefinitions newTypes) {
        types.putAll(newTypes);
    }

    public void addType(final String typeName, final TypeDefinition type) {
        types.put(typeName, type);
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
     * @return An implementation of {@link gaffer.serialisation.Serialisation} that will be used to serialise all vertices.
     */
    @JsonIgnore
    public Serialisation getVertexSerialiser() {
        return vertexSerialiser;
    }

    public void setVertexSerialiser(final Serialisation vertexSerialiser) {
        this.vertexSerialiser = vertexSerialiser;
    }

    public String getVertexSerialiserClass() {
        if (null == vertexSerialiser) {
            return null;
        }

        return vertexSerialiser.getClass().getName();
    }

    public void setVertexSerialiserClass(final String vertexSerialiserClass) {
        if (null == vertexSerialiserClass) {
            this.vertexSerialiser = null;
        } else {
            Class<? extends Serialisation> serialiserClass;
            try {
                serialiserClass = Class.forName(vertexSerialiserClass).asSubclass(Serialisation.class);
            } catch (ClassNotFoundException e) {
                throw new SchemaException(e.getMessage(), e);
            }
            try {
                setVertexSerialiser(serialiserClass.newInstance());
            } catch (IllegalAccessException | IllegalArgumentException | SecurityException | InstantiationException e) {
                throw new SchemaException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void setEdges(final Map<String, SchemaEdgeDefinition> edges) {
        super.setEdges(edges);
        for (SchemaElementDefinition def : edges.values()) {
            def.setTypesLookup(types);
        }
    }

    @Override
    public void setEntities(final Map<String, SchemaEntityDefinition> entities) {
        super.setEntities(entities);
        for (SchemaElementDefinition def : entities.values()) {
            def.setTypesLookup(types);
        }
    }

    @Override
    public SchemaElementDefinition getElement(final String group) {
        return (SchemaElementDefinition) super.getElement(group);
    }

    public String getVisibilityProperty() {
        return visibilityProperty;
    }

    public void setVisibilityProperty(final String visibilityProperty) {
        this.visibilityProperty = visibilityProperty;
    }

    public String getTimestampProperty() {
        return timestampProperty;
    }

    public void setTimestampProperty(final String timestampProperty) {
        this.timestampProperty = timestampProperty;
    }

    @Override
    public void merge(final ElementDefinitions<SchemaEntityDefinition, SchemaEdgeDefinition> elementDefs) {
        if (elementDefs instanceof Schema) {
            merge(((Schema) elementDefs));
        } else {
            super.merge(elementDefs);
        }
    }

    public void merge(final Schema schema) {
        super.merge(schema);

        if (null != schema.getVertexSerialiser()) {
            if (null == getVertexSerialiser()) {
                setVertexSerialiser(schema.getVertexSerialiser());
            } else if (!vertexSerialiser.getClass().equals(schema.getVertexSerialiser().getClass())) {
                throw new SchemaException("Unable to merge schemas. Conflict with vertex serialiser, options are: "
                        + vertexSerialiser.getClass().getName() + " and " + schema.getVertexSerialiser().getClass().getName());
            }
        }

        if (null == visibilityProperty) {
            setVisibilityProperty(schema.getVisibilityProperty());
        } else if (null != schema.getVisibilityProperty() && !visibilityProperty.equals(schema.getVisibilityProperty())) {
            throw new SchemaException("Unable to merge schemas. Conflict with visibility property, options are: "
                    + visibilityProperty + " and " + schema.getVisibilityProperty());
        }

        if (null == timestampProperty) {
            setTimestampProperty(schema.getTimestampProperty());
        } else if (null != schema.getTimestampProperty() && !timestampProperty.equals(schema.getTimestampProperty())) {
            throw new SchemaException("Unable to merge schemas. Conflict with timestamp property, options are: "
                    + timestampProperty + " and " + schema.getTimestampProperty());
        }

        types.merge(schema.getTypes());
    }

    @Override
    protected void addEdge(final String group, final SchemaEdgeDefinition elementDef) {
        elementDef.setTypesLookup(types);
        super.addEdge(group, elementDef);
    }

    @Override
    protected void addEntity(final String group, final SchemaEntityDefinition elementDef) {
        elementDef.setTypesLookup(types);
        super.addEntity(group, elementDef);
    }

    @Override
    public String toString() {
        try {
            return "Schema" + new String(toJson(true), CommonConstants.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Builder extends ElementDefinitions.Builder<SchemaEntityDefinition, SchemaEdgeDefinition> {
        public Builder() {
            this(new Schema());
        }

        public Builder(final Schema schema) {
            super(schema);
        }

        /**
         * Sets the {@link gaffer.serialisation.Serialisation}.
         *
         * @param vertexSerialiser the {@link gaffer.serialisation.Serialisation} to set
         * @return this Builder
         * @see Schema#setVertexSerialiser(Serialisation)
         */
        public Builder vertexSerialiser(final Serialisation vertexSerialiser) {
            getElementDefs().setVertexSerialiser(vertexSerialiser);

            return this;
        }

        /**
         * Sets the {@link gaffer.serialisation.Serialisation} from class name.
         *
         * @param vertexSerialiserClass the {@link gaffer.serialisation.Serialisation} class name to set
         * @return this Builder
         * @see Schema#setVertexSerialiserClass(java.lang.String)
         */
        public Builder vertexSerialiser(final String vertexSerialiserClass) {
            getElementDefs().setVertexSerialiserClass(vertexSerialiserClass);

            return this;
        }

        @Override
        public Builder edge(final String group, final SchemaEdgeDefinition edgeDef) {
            return (Builder) super.edge(group, edgeDef);
        }

        public Builder edge(final String group) {
            return edge(group, new SchemaEdgeDefinition());
        }

        @Override
        public Builder entity(final String group, final SchemaEntityDefinition entityDef) {
            return (Builder) super.entity(group, entityDef);
        }

        public Builder entity(final String group) {
            return entity(group, new SchemaEntityDefinition());
        }

        public Builder type(final String typeName, final TypeDefinition type) {
            getElementDefs().addType(typeName, type);
            return this;
        }

        public Builder type(final String typeName, final Class<?> typeClass) {
            return type(typeName, new TypeDefinition(typeClass));
        }

        public Builder types(final TypeDefinitions types) {
            getElementDefs().addTypes(types);
            return this;
        }

        public Builder visibilityProperty(final String propertyName) {
            getElementDefs().setVisibilityProperty(propertyName);
            return this;
        }

        public Builder timestampProperty(final String propertyName) {
            getElementDefs().setTimestampProperty(propertyName);
            return this;
        }

        @Override
        public Schema build() {
            final Schema schema = (Schema) super.build();
            if (!schema.validate()) {
                throw new SchemaException("The schema is not valid. Check the logs for more information.");
            }

            return schema;
        }

        @Override
        public Schema buildModule() {
            return (Schema) super.buildModule();
        }

        @Override
        protected Schema getElementDefs() {
            return (Schema) super.getElementDefs();
        }
    }
}
