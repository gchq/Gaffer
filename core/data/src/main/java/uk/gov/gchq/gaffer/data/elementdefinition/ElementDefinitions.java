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

package uk.gov.gchq.gaffer.data.elementdefinition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

    protected ElementDefinitions() {
        edges = new HashMap<>();
        entities = new HashMap<>();
    }

    public byte[] toJson(final boolean prettyPrint, final String... fieldsToExclude) throws SchemaException {
        try {
            return JSON_SERIALISER.serialise(this, prettyPrint, fieldsToExclude);
        } catch (SerialisationException e) {
            throw new SchemaException(e.getMessage(), e);
        }
    }

    /**
     * Looks the group up in the entity definitions then if it doesn't find a definition it will look it up in the edge definitions.
     * If you know the type of the element (Entity or Edge) then use getEntity or getEdge.
     *
     * @param group an group
     * @return the {@link uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinition} for the given group
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

    @JsonIgnore
    public Set<String> getEdgeGroups() {
        return null != edges ? edges.keySet() : Collections.EMPTY_SET;
    }

    @JsonIgnore
    public Set<String> getEntityGroups() {
        return null != entities ? entities.keySet() : Collections.EMPTY_SET;
    }

    /**
     * Returns a new hash set with all entity and edge groups.
     *
     * @return a new hash set with all entity and edge groups.
     */
    @JsonIgnore
    public Set<String> getGroups() {
        final Set<String> entityGroups = getEntityGroups();
        final Set<String> edgeGroups = getEdgeGroups();
        final Set<String> groups = new HashSet<>(entityGroups.size() + edgeGroups.size());
        groups.addAll(entityGroups);
        groups.addAll(edgeGroups);
        return groups;
    }

    @JsonIgnore
    public boolean hasGroups() {
        return (null != entities && !entities.isEmpty())
                || (null != edges && !edges.isEmpty());
    }

    public Map<String, EDGE_DEF> getEdges() {
        return edges;
    }

    public Map<String, ENTITY_DEF> getEntities() {
        return entities;
    }

    protected void setEdges(final Map<String, EDGE_DEF> edges) {
        this.edges = edges;
    }

    protected void setEntities(final Map<String, ENTITY_DEF> entities) {
        this.entities = entities;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ElementDefinitions<?, ?> that = (ElementDefinitions<?, ?>) o;

        return new EqualsBuilder()
                .append(edges, that.edges)
                .append(entities, that.entities)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(edges)
                .append(entities)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("edges", edges)
                .append("entities", entities)
                .toString();
    }

    protected void lock() {
        edges = Collections.unmodifiableMap(edges);
        entities = Collections.unmodifiableMap(entities);
    }

    /**
     * Builder for {@link uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions}.
     *
     * @param <ENTITY_DEF> the entity definition type.
     * @param <EDGE_DEF>   the entity definition type.
     */
    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "set")
    public abstract static class BaseBuilder<ELEMENT_DEFS extends ElementDefinitions<ENTITY_DEF, EDGE_DEF>, ENTITY_DEF extends ElementDefinition, EDGE_DEF extends ElementDefinition, CHILD_CLASS extends BaseBuilder<ELEMENT_DEFS, ENTITY_DEF, EDGE_DEF, ?>> {
        private ELEMENT_DEFS elementDefs;

        protected BaseBuilder(final ELEMENT_DEFS elementDefs) {
            this.elementDefs = elementDefs;
        }

        protected abstract CHILD_CLASS edge(final String group);

        /**
         * Adds an edge definition for a given edge type.
         *
         * @param group   the edge type
         * @param edgeDef the edge definition for the given edge type.
         * @return this Builder
         */
        public CHILD_CLASS edge(final String group, final EDGE_DEF edgeDef) {
            elementDefs.getEdges().put(group, edgeDef);
            return self();
        }

        @JsonSetter("edges")
        public CHILD_CLASS edges(final Map<String, EDGE_DEF> edges) {
            elementDefs.getEdges().clear();
            elementDefs.getEdges().putAll(edges);
            return self();
        }

        public CHILD_CLASS edges(final Collection<String> groups) {
            for (final String group : groups) {
                edge(group);
            }

            return self();
        }

        protected abstract CHILD_CLASS entity(final String group);


        /**
         * Adds an entity definition for a given entity type.
         *
         * @param group     the entity type
         * @param entityDef the entity definition for the given entity type.
         * @return this Builder
         */
        public CHILD_CLASS entity(final String group, final ENTITY_DEF entityDef) {
            elementDefs.getEntities().put(group, entityDef);
            return self();
        }

        @JsonSetter("entities")
        public CHILD_CLASS entities(final Map<String, ENTITY_DEF> entities) {
            elementDefs.getEntities().clear();
            elementDefs.getEntities().putAll(entities);
            return self();
        }

        @JsonIgnore
        public CHILD_CLASS entities(final Collection<String> groups) {
            for (final String group : groups) {
                entity(group);
            }

            return self();
        }


        public CHILD_CLASS json(final Class<? extends ELEMENT_DEFS> clazz, final Path... filePaths) throws SchemaException {
            return json(clazz, (Object[]) filePaths);
        }

        public CHILD_CLASS json(final Class<? extends ELEMENT_DEFS> clazz, final InputStream... inputStreams) throws SchemaException {
            try {
                return json(clazz, (Object[]) inputStreams);
            } finally {
                if (null != inputStreams) {
                    for (final InputStream inputStream : inputStreams) {
                        IOUtils.closeQuietly(inputStream);
                    }
                }
            }
        }

        public CHILD_CLASS json(final Class<? extends ELEMENT_DEFS> clazz, final byte[]... jsonBytes) throws SchemaException {
            return json(clazz, (Object[]) jsonBytes);
        }

        public CHILD_CLASS json(final Class<? extends ELEMENT_DEFS> clazz, final Object[] jsonItems) throws SchemaException {
            for (final Object jsonItem : jsonItems) {
                try {
                    if (jsonItem instanceof InputStream) {
                        merge(JSON_SERIALISER.deserialise(((InputStream) jsonItem), clazz));
                    } else if (jsonItem instanceof Path) {
                        final Path path = (Path) jsonItem;
                        if (Files.isDirectory(path)) {
                            for (final Path filePath : Files.newDirectoryStream(path)) {
                                merge(JSON_SERIALISER.deserialise(Files.readAllBytes(filePath), clazz));
                            }
                        } else {
                            merge(JSON_SERIALISER.deserialise(Files.readAllBytes(path), clazz));
                        }
                    } else {
                        merge(JSON_SERIALISER.deserialise(((byte[]) jsonItem), clazz));
                    }
                } catch (final IOException e) {
                    throw new SchemaException("Failed to load element definitions from bytes", e);
                }
            }

            return self();
        }

        protected abstract CHILD_CLASS merge(final ELEMENT_DEFS newElementDefs);

        /**
         * Builds the {@link uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions} validates it and returns it.
         *
         * @return the build {@link uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions}.
         */
        public ELEMENT_DEFS build() {
            elementDefs.lock();
            return elementDefs;
        }

        protected ELEMENT_DEFS getElementDefs() {
            return elementDefs;
        }

        protected void setElementDefs(final ELEMENT_DEFS elementDefs) {
            this.elementDefs = elementDefs;
        }

        protected abstract CHILD_CLASS self();
    }
}
