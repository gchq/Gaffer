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

package gaffer.store.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import gaffer.data.elementdefinition.ElementDefinitions;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.serialisation.Serialisation;
import gaffer.serialisation.implementation.JavaSerialiser;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * A <code>StoreSchema</code> contains all the {@link gaffer.data.element.Element}s to be stored and specifically
 * the information needed to store them - i.e positions and serialisers.
 * <p>
 * This class must be JSON serialisable.
 * A store schema should normally be written in JSON and then deserialised at runtime.
 * Examples of JSON data schemas can be found in the example projects.
 *
 * @see gaffer.store.schema.StoreSchema.Builder
 * @see gaffer.store.schema.StoreElementDefinition
 * @see gaffer.store.schema.StorePropertyDefinition
 */
public class StoreSchema extends ElementDefinitions<StoreElementDefinition, StoreElementDefinition> {
    private static final long serialVersionUID = 2377712298631263987L;
    private static final Serialisation DEFAULT_VERTEX_SERIALISER = new JavaSerialiser();

    /**
     * The {@link gaffer.serialisation.Serialisation} for all identifiers. By default it is set to
     * {@link gaffer.serialisation.implementation.JavaSerialiser}.
     */
    private Serialisation vertexSerialiser = DEFAULT_VERTEX_SERIALISER;

    /**
     * A map of keys to positions.
     * This could be used to set the identifier, group or general property positions.
     */
    private Map<String, String> positions;

    public StoreSchema() {
        super();
    }

    public StoreSchema(final Map<String, StoreElementDefinition> edges, final Map<String, StoreElementDefinition> entities) {
        setEdges(edges);
        setEntities(entities);
    }

    public static StoreSchema fromJson(final InputStream inputStream) throws SchemaException {
        return fromJson(inputStream, StoreSchema.class);
    }

    public static StoreSchema fromJson(final Path filePath) throws SchemaException {
        return fromJson(filePath, StoreSchema.class);
    }

    public static StoreSchema fromJson(final byte[] jsonBytes) throws SchemaException {
        return fromJson(jsonBytes, StoreSchema.class);
    }

    public String getPosition(final String key) {
        return positions.get(key);
    }

    /**
     * @return a map of keys to positions.
     * This could be used to set the identifier, group or general property positions.
     */
    public Map<String, String> getPositions() {
        return positions;
    }

    /**
     * @param positions a map of keys to positions.
     *                  This could be used to set the identifier, group or general property positions.
     */
    public void setPositions(final Map<String, String> positions) {
        this.positions = positions;
    }

    /**
     * Returns the vertex serialiser for this store schema.
     * <p>
     * There can be only one vertex serialiser per store schema because in order for searches to work correctly,
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
        if (null != vertexSerialiser) {
            this.vertexSerialiser = vertexSerialiser;
        } else {
            this.vertexSerialiser = DEFAULT_VERTEX_SERIALISER;
        }
    }

    public String getVertexSerialiserClass() {
        final Class<? extends Serialisation> serialiserClass = vertexSerialiser.getClass();
        if (!DEFAULT_VERTEX_SERIALISER.getClass().equals(serialiserClass)) {
            return serialiserClass.getName();
        }

        return null;
    }

    public void setVertexSerialiserClass(final String vertexSerialiserClass) {
        if (null == vertexSerialiserClass) {
            this.vertexSerialiser = DEFAULT_VERTEX_SERIALISER;
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
    public StoreElementDefinition getElement(final String group) {
        return (StoreElementDefinition) super.getElement(group);
    }

    /**
     * Builds a {@link gaffer.store.schema.StoreElementDefinition}.
     */
    public static class Builder extends ElementDefinitions.Builder<StoreElementDefinition, StoreElementDefinition> {
        public Builder() {
            this(new StoreSchema());
        }

        public Builder(final StoreSchema storeSchema) {
            super(storeSchema);
        }

        @Override
        public Builder edge(final String group, final StoreElementDefinition elementDef) {
            return (Builder) super.edge(group, elementDef);
        }

        @Override
        public Builder entity(final String group, final StoreElementDefinition elementDef) {
            return (Builder) super.entity(group, elementDef);
        }

        /**
         * Adds a position for an identifier type, group or property name.
         *
         * @param key      the key to add a position for.
         * @param position the position
         * @return this Builder
         * @see gaffer.store.schema.StoreSchema#setPositions(java.util.Map)
         */
        public Builder position(final String key, final String position) {
            Map<String, String> positions = getElementDefs().getPositions();
            if (null == positions) {
                positions = new HashMap<>();
                getElementDefs().setPositions(positions);
            }
            positions.put(key, position);

            return this;
        }

        /**
         * Sets the {@link gaffer.serialisation.Serialisation}.
         *
         * @param vertexSerialiser the {@link gaffer.serialisation.Serialisation} to set
         * @return this Builder
         * @see gaffer.store.schema.StoreSchema#setVertexSerialiser(Serialisation)
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
         * @see gaffer.store.schema.StoreSchema#setVertexSerialiserClass(java.lang.String)
         */
        public Builder vertexSerialiser(final String vertexSerialiserClass) {
            getElementDefs().setVertexSerialiserClass(vertexSerialiserClass);

            return this;
        }

        @Override
        public StoreSchema build() {
            return getElementDefs();
        }

        @Override
        protected StoreSchema getElementDefs() {
            return (StoreSchema) super.getElementDefs();
        }
    }
}
