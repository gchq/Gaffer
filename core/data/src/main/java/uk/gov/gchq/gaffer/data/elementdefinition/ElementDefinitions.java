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

package uk.gov.gchq.gaffer.data.elementdefinition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.io.IOException;
import java.io.InputStream;
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
 * A schema should normally be written in JSON and then deserialised at runtime.
 * Examples of JSON schemas can be found in the example projects.
 *
 * @param <ENTITY_DEF> the type of {@link ElementDefinition} for the entities
 * @param <EDGE_DEF>   the type of {@link ElementDefinition} for the edges
 */
@JsonDeserialize(builder = ElementDefinitions.Builder.class)
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

    public Map<String, EDGE_DEF> getEdges() {
        return Collections.unmodifiableMap(edges);
    }

    public Map<String, ENTITY_DEF> getEntities() {
        return Collections.unmodifiableMap(entities);
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

    /**
     * Builder for {@link uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions}.
     *
     * @param <ENTITY_DEF> the entity definition type.
     * @param <EDGE_DEF>   the entity definition type.
     */
    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "set")
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
            elementDefs.edges.put(group, edgeDef);
            return this;
        }

        protected Builder edges(final Map<String, EDGE_DEF> edges) {
            elementDefs.edges.putAll(edges);
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
            elementDefs.entities.put(group, entityDef);
            return this;
        }

        protected Builder entities(final Map<String, ENTITY_DEF> entities) {
            elementDefs.entities.putAll(entities);
            return this;
        }

        protected Builder merge(final ElementDefinitions<ENTITY_DEF, EDGE_DEF> newElementDefs) {
            for (final Map.Entry<String, ENTITY_DEF> entry : newElementDefs.getEntities().entrySet()) {
                if (!elementDefs.entities.containsKey(entry.getKey())) {
                    entity(entry.getKey(), entry.getValue());
                } else {
                    elementDefs.entities.get(entry.getKey()).merge(entry.getValue());
                }
            }

            for (final Map.Entry<String, EDGE_DEF> entry : newElementDefs.getEdges().entrySet()) {
                if (!elementDefs.edges.containsKey(entry.getKey())) {
                    edge(entry.getKey(), entry.getValue());
                } else {
                    elementDefs.edges.get(entry.getKey()).merge(entry.getValue());
                }
            }

            return this;
        }

        protected <T extends ElementDefinitions<ENTITY_DEF, EDGE_DEF>> Builder json(final Class<T> clazz, final Path... filePaths) throws SchemaException {
            return json(clazz, (Object[]) filePaths);
        }

        protected <T extends ElementDefinitions<ENTITY_DEF, EDGE_DEF>> Builder json(final Class<T> clazz, final InputStream... inputStreams) throws SchemaException {
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

        protected <T extends ElementDefinitions<ENTITY_DEF, EDGE_DEF>> Builder json(final Class<T> clazz, final byte[]... jsonBytes) throws SchemaException {
            return json(clazz, (Object[]) jsonBytes);
        }

        protected <T extends ElementDefinitions<ENTITY_DEF, EDGE_DEF>> Builder json(final Class<T> clazz, final Object[] jsonItems) throws SchemaException {
            for (final Object jsonItem : jsonItems) {
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

                merge(elDefsTmp);
            }

            return this;
        }

        /**
         * Builds the {@link uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions} validates it and returns it.
         *
         * @return the build {@link uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions}.
         */
        protected ElementDefinitions<ENTITY_DEF, EDGE_DEF> build() {
            elementDefs.edges = Collections.unmodifiableMap(elementDefs.edges);
            elementDefs.entities = Collections.unmodifiableMap(elementDefs.entities);
            return elementDefs;
        }

        protected ElementDefinitions<ENTITY_DEF, EDGE_DEF> getElementDefs() {
            return elementDefs;
        }
    }
}
