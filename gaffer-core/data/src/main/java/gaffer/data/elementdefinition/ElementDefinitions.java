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
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Contains the full list of groups in the graph.
 * <p>
 * This class must be JSON serialisable.
 * A schema should normally be written in JSON and then deserialised at runtime.
 * Examples of JSON schemas can be found in the example projects.
 *
 * @param <ENTITY_DEF> the type of {@link ElementDefinition} for the entities
 * @param <EDGE_DEF>   the type of {@link ElementDefinition} for the edges
 */
public abstract class ElementDefinitions<ENTITY_DEF extends ElementDefinition, EDGE_DEF extends ElementDefinition> {
    protected static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    /**
     * Map of edge type to edge definition.
     */
    private Map<String, EDGE_DEF> edges;

    /**
     * Map of entity type to entity definition.
     */
    private Map<String, ENTITY_DEF> entities;

    public ElementDefinitions() {
        edges = new HashMap<>();
        entities = new HashMap<>();
    }


    public static <T extends ElementDefinitions> T fromJson(final Class<T> clazz, final Path... filePaths) throws SchemaException {
        return fromJson(clazz, (Object[]) filePaths);
    }

    public static <T extends ElementDefinitions> T fromJson(final Class<T> clazz, final InputStream... inputStreams) throws SchemaException {
        try {
            return fromJson(clazz, (Object[]) inputStreams);
        } finally {
            if (null != inputStreams) {
                for (InputStream inputStream : inputStreams) {
                    IOUtils.closeQuietly(inputStream);
                }
            }
        }
    }

    public static <T extends ElementDefinitions> T fromJson(final Class<T> clazz, final byte[]... jsonBytes) throws SchemaException {
        return fromJson(clazz, (Object[]) jsonBytes);
    }

    private static <T extends ElementDefinitions> T fromJson(final Class<T> clazz, final Object[] jsonItems) throws SchemaException {
        T elementDefs = null;
        for (Object jsonItem : jsonItems) {
            final T elDefsTmp;
            try {
                if (jsonItem instanceof InputStream) {
                    elDefsTmp = JSON_SERIALISER.deserialise(((InputStream) jsonItem), clazz);
                } else if (jsonItem instanceof Path) {
                    elDefsTmp = JSON_SERIALISER.deserialise(Files.readAllBytes((Path) jsonItem), clazz);
                } else {
                    elDefsTmp = JSON_SERIALISER.deserialise(((byte[]) jsonItem), clazz);
                }
            } catch (IOException e) {
                throw new SchemaException("Failed to load element definitions from bytes", e);
            }

            if (null == elementDefs) {
                elementDefs = elDefsTmp;
            } else {
                elementDefs.merge(elDefsTmp);
            }
        }

        return elementDefs;
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

    public EDGE_DEF getEdge(final String group) {
        return edges.get(group);
    }

    public ENTITY_DEF getEntity(final String group) {
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

    public void setEdges(final Map<String, EDGE_DEF> edges) {
        this.edges = edges;
    }

    public void setEntities(final Map<String, ENTITY_DEF> entities) {
        this.entities = entities;
    }

    public Map<String, EDGE_DEF> getEdges() {
        return Collections.unmodifiableMap(edges);
    }

    public Map<String, ENTITY_DEF> getEntities() {
        return Collections.unmodifiableMap(entities);
    }

    protected void addEdge(final String group, final EDGE_DEF elementDef) {
        edges.put(group, elementDef);
    }

    protected void addEntity(final String group, final ENTITY_DEF elementDef) {
        entities.put(group, elementDef);
    }

    public void merge(final ElementDefinitions<ENTITY_DEF, EDGE_DEF> elementDefs) {
        for (Entry<String, ENTITY_DEF> entry : elementDefs.getEntities().entrySet()) {
            if (!edges.containsKey(entry.getKey())) {
                addEntity(entry.getKey(), entry.getValue());
            } else {
                entities.get(entry.getKey()).merge(entry.getValue());
            }
        }

        for (Entry<String, EDGE_DEF> entry : elementDefs.getEdges().entrySet()) {
            if (!edges.containsKey(entry.getKey())) {
                addEdge(entry.getKey(), entry.getValue());
            } else {
                edges.get(entry.getKey()).merge(entry.getValue());
            }
        }
    }

    /**
     * Builder for {@link gaffer.data.elementdefinition.ElementDefinitions}.
     *
     * @param <ENTITY_DEF> the entity definition type.
     * @param <EDGE_DEF>   the entity definition type.
     */
    public static class Builder<ENTITY_DEF extends ElementDefinition, EDGE_DEF extends ElementDefinition> {
        private final ElementDefinitions<ENTITY_DEF, EDGE_DEF> elementDefs;

        protected Builder(final ElementDefinitions<ENTITY_DEF, EDGE_DEF> elementDefs) {
            this.elementDefs = elementDefs;
        }

        /**
         * Adds an edge definition for a given edge type.
         *
         * @param group   the edge type
         * @param edgeDef the edge definition for the given edge type.
         * @return this Builder
         */
        protected Builder edge(final String group, final EDGE_DEF edgeDef) {
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
        protected Builder entity(final String group, final ENTITY_DEF entityDef) {
            elementDefs.addEntity(group, entityDef);
            return this;
        }

        /**
         * Builds the {@link gaffer.data.elementdefinition.ElementDefinitions} validates it and returns it.
         *
         * @return the build {@link gaffer.data.elementdefinition.ElementDefinitions}.
         */
        protected ElementDefinitions<ENTITY_DEF, EDGE_DEF> build() {
            return elementDefs;
        }

        /**
         * Builds the {@link gaffer.data.elementdefinition.ElementDefinitions} without validating it and returns it.
         *
         * @return the build {@link gaffer.data.elementdefinition.ElementDefinitions}.
         */
        protected ElementDefinitions<ENTITY_DEF, EDGE_DEF> buildModule() {
            return elementDefs;
        }

        protected ElementDefinitions<ENTITY_DEF, EDGE_DEF> getElementDefs() {
            return elementDefs;
        }
    }
}
