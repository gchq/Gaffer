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

package gaffer.data.elementdefinition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contains the full list of groups in the graph.
 * <p>
 * This class must be JSON serialisable.
 * A data schema should normally be written in JSON and then deserialised at runtime.
 * Examples of JSON data schemas can be found in the example projects.
 *
 * @param <EntityDef> the type of {@link ElementDefinition} for the entities
 * @param <EdgeDef>   the type of {@link ElementDefinition} for the edges
 */
public abstract class ElementDefinitions<EntityDef extends ElementDefinition, EdgeDef extends ElementDefinition> implements Serializable {
    protected static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    private static final long serialVersionUID = -6997056863871610386L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementDefinitions.class);

    /**
     * Map of edge type to edge definition.
     */
    private Map<String, EdgeDef> edges;

    /**
     * Map of entity type to entity definition.
     */
    private Map<String, EntityDef> entities;

    public ElementDefinitions() {
        edges = new HashMap<>();
        entities = new HashMap<>();
    }


    public static <T extends ElementDefinitions> T fromJson(final Path filePath, final Class<T> clazz) throws SchemaException {
        try {
            return fromJson(Files.readAllBytes(filePath), clazz);
        } catch (IOException e) {
            throw new SchemaException("Failed to read file: " + filePath.getFileName(), e);
        }
    }

    public static <T extends ElementDefinitions> T fromJson(final InputStream inputStream, final Class<T> clazz) throws SchemaException {
        try {
            return JSON_SERIALISER.deserialise(inputStream, clazz);
        } catch (SerialisationException e) {
            throw new SchemaException("Failed to load element definitions from input stream", e);
        }
    }

    public static <T extends ElementDefinitions> T fromJson(final byte[] jsonBytes, final Class<T> clazz) throws SchemaException {
        try {
            return JSON_SERIALISER.deserialise(jsonBytes, clazz);
        } catch (SerialisationException e) {
            throw new SchemaException("Failed to load element definitions from input stream", e);
        }
    }


    /**
     * Validates the schema to ensure all element definitions are valid.
     * Throws a SchemaException if it is not valid.
     *
     * @return true if valid, otherwise false.
     * @throws SchemaException if validation fails then a SchemaException is thrown.
     */
    public boolean validate() throws SchemaException {
        for (String edgeGroup : edges.keySet()) {
            if (entities.containsKey(edgeGroup)) {
                LOGGER.warn("Groups must not be shared between Entity definitions and Edge definitions."
                        + "Found edgeGroup '" + edgeGroup + "' in the collection of entities");
                return false;
            }
        }

        for (Map.Entry<String, EdgeDef> elementDefEntry : edges.entrySet()) {
            if (null == elementDefEntry.getValue()) {
                throw new SchemaException("Edge definition was null for group: " + elementDefEntry.getKey());
            }

            if (!elementDefEntry.getValue().validate()) {
                LOGGER.warn("VALIDITY ERROR: Invalid edge definition for group: " + elementDefEntry.getKey());
                return false;
            }
        }

        for (Map.Entry<String, EntityDef> elementDefEntry : entities.entrySet()) {
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

    public byte[] toJson(final boolean prettyPrint) throws SchemaException {
        try {
            return JSON_SERIALISER.serialise(this, prettyPrint);
        } catch (SerialisationException e) {
            throw new SchemaException(e.getMessage(), e);
        }
    }

    /**
     * Looks the group up in the entity definitions then if it doesn't find a definition it will look it up in the edge definitions.
     * If you know the type of the element (Entity or Edge) then use getEntity or getEdge.
     *
     * @param group an group
     * @return the {@link gaffer.data.elementdefinition.ElementDefinition} for the given group
     */
    public ElementDefinition getElement(final String group) {
        return isEntity(group) ? getEntity(group) : getEdge(group);
    }

    public EdgeDef getEdge(final String group) {
        return edges.get(group);
    }

    public EntityDef getEntity(final String group) {
        return entities.get(group);
    }

    public boolean isEntity(final String group) {
        return entities.containsKey(group);
    }

    public boolean isEdge(final String group) {
        return edges.containsKey(group);
    }

    public Class<? extends Element> getGroup(final String group) {
        return isEntity(group) ? Element.class : isEdge(group) ? Edge.class : null;
    }

    @JsonIgnore
    public Set<String> getEdgeGroups() {
        return null != edges ? edges.keySet() : new HashSet<String>(0);
    }

    @JsonIgnore
    public Set<String> getEntityGroups() {
        return null != entities ? entities.keySet() : new HashSet<String>(0);
    }

    public void setEdges(final Map<String, EdgeDef> edges) {
        this.edges = edges;
    }

    public void setEntities(final Map<String, EntityDef> entities) {
        this.entities = entities;
    }

    public Map<String, EdgeDef> getEdges() {
        return Collections.unmodifiableMap(edges);
    }

    public Map<String, EntityDef> getEntities() {
        return Collections.unmodifiableMap(entities);
    }

    protected void addEdge(final String group, final EdgeDef elementDef) {
        edges.put(group, elementDef);
    }

    protected void addEntity(final String group, final EntityDef elementDef) {
        entities.put(group, elementDef);
    }

    /**
     * Builder for {@link gaffer.data.elementdefinition.ElementDefinitions}.
     *
     * @param <EntityDef> the entity definition type.
     * @param <EdgeDef>   the entity definition type.
     */
    public static class Builder<EntityDef extends ElementDefinition, EdgeDef extends ElementDefinition> {
        private final ElementDefinitions<EntityDef, EdgeDef> elementDefs;

        public Builder(final ElementDefinitions<EntityDef, EdgeDef> elementDefs) {
            this.elementDefs = elementDefs;
        }

        /**
         * Adds an edge definition for a given edge type.
         *
         * @param group   the edge type
         * @param edgeDef the edge definition for the given edge type.
         * @return this Builder
         */
        public Builder edge(final String group, final EdgeDef edgeDef) {
            elementDefs.addEdge(group, edgeDef);
            return this;
        }

        /**
         * Adds an entity definition for a given entity type.
         *
         * @param group     the entity type
         * @param entityDef the entity definition for the given entity type.
         * @return this Builder
         */
        public Builder entity(final String group, final EntityDef entityDef) {
            elementDefs.addEntity(group, entityDef);
            return this;
        }

        /**
         * Builds the {@link gaffer.data.elementdefinition.ElementDefinitions} and returns it.
         *
         * @return the build {@link gaffer.data.elementdefinition.ElementDefinitions}.
         */
        public ElementDefinitions<EntityDef, EdgeDef> build() {
            return elementDefs;
        }

        protected ElementDefinitions<EntityDef, EdgeDef> getElementDefs() {
            return elementDefs;
        }
    }
}
