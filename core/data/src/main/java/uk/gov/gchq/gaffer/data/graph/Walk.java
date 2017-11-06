/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.data.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static uk.gov.gchq.gaffer.commonutil.CollectionUtil.distinct;

/**
 * A {@code Walk} describes a graph traversal which begins at a vertex and comprises
 * of:
 * <ul><li>a sequence of {@link Edge} objects, where the source of one edge must
 * follow on from the destination of the previous</li>
 * <li>an optional sequence of {@link Entity} objects which are associated to each
 * of the vertices visited on the Walk. Vertices with no associated entities are
 * still represented, but only as empty collections.</li></ul>
 *
 * For example, a Walk through a simple graph could look like:
 *
 * <pre>
 * {@code
 * A --> B --> C
 *       \     \
 *       \     (Entity2, Entity3)
 *       \
 *       (Entity1)
 *
 * Edges:       A -> B, B -> C
 * Entities:    [], [Entity1], [Entity2, Entity3]
 * }
 * </pre>
 */
public class Walk implements Iterable<Set<Edge>> {

    private final List<Set<Edge>> edges;
    private final List<Entry<Object, Set<Entity>>> entities;

    /**
     * Private builder constructor.
     *
     * @param builder the builder
     */
    private Walk(final Builder builder) {
        this.entities = builder.entities;
        this.edges = builder.edges;
    }

    /**
     * Constructor used by Jackson.
     *
     * @param edges the edges to add to the walk
     * @param entities the entities to add to the walk
     */
    @JsonCreator
    public Walk(@JsonProperty("edges") final List<Set<Edge>> edges,
                @JsonProperty("entities") final List<Entry<Object, Set<Entity>>> entities) {
        this.edges = edges;
        this.entities = entities;
    }

    /**
     * Retrieve all of the entities associated with a particular vertex on the
     * walk.
     *
     * @param vertex the vertex of interest
     * @return a {@link Set} of all of the entities associated with the vertex
     */
    @JsonIgnore
    public Set<Entity> getEntitiesForVertex(final Object vertex) {
        return entities.stream()
                .filter(e -> e.getKey().equals(vertex))
                .map(e -> e.getValue())
                .flatMap(Set::stream)
                .collect(toSet());
    }

    /**
     * Get all of the entities at some distance from the start of the walk.
     *
     * @param n the distance from the start of the walk (in terms of the number
     *          of edges that would have to be traversed)
     * @return the entities at the specified distance
     */
    @JsonIgnore
    public Set<Entity> getEntitiesAtDistance(final int n) {
        return entities.get(n).getValue();
    }

    public List<Set<Edge>> getEdges() {
        return edges;
    }

    @JsonGetter("entities")
    public List<Entry<Object, Set<Entity>>> getEntitiesAsEntries() {
        return entities;
    }

    @JsonIgnore
    public List<Set<Entity>> getEntities() {
        return entities.stream().map(entry -> entry.getValue()).collect(toList());
    }

    /**
     * Get an ordered {@link List} of the vertices on the walk.
     *
     * This will include any repeated vertices.
     *
     * @return a list of the vertices on the walk
     */
    @JsonIgnore
    public List<Object> getVerticesOrdered() {
        return entities.stream().map(entry -> entry.getKey()).collect(toList());
    }

    /**
     * Get the {@link Set} of vertices on the walk.
     *
     * @return a set of the vertices on the walk.
     */
    @JsonIgnore
    public Set<Object> getVertexSet() {
        return entities.stream().map(entry -> entry.getKey()).collect(toSet());
    }

    @JsonIgnore
    public int length() {
        return edges.size();
    }

    /**
     * A walk is also a trail if it contains no repeated edges.
     *
     * @return {@code true} if the walk does not contain any repeated edges, otherwise
     * {@code false}
     */
    @JsonIgnore
    public boolean isTrail() {
        return Sets.newHashSet(edges).size() == edges.size();
    }

    /**
     * A walk is also a path if it contains no repeated vertices (i.e. does not
     * contain any loops).
     *
     * @return {@code true} if the walk does not contain any repeated vertices,
     * otherwise {@code false}
     */
    @JsonIgnore
    public boolean isPath() {
        return distinct(getVerticesOrdered());
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final Walk walk = (Walk) obj;

        return new EqualsBuilder()
                .append(edges, walk.edges)
                .append(entities, walk.entities)
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

    @Override
    public Iterator<Set<Edge>> iterator() {
        return edges.iterator();
    }

    public static final class Builder {

        private final Stack<Set<Edge>> edges;
        private final Stack<Entry<Object, Set<Entity>>> entities;

        public Builder() {
            this.edges = new Stack<>();
            this.entities = new Stack<>();
        }

        public Builder edge(final Edge edge) {
            if (entities.empty()) {
                entities.add(new AbstractMap.SimpleEntry<Object, Set<Entity>>(edge.getMatchedVertexValue(), Sets.newHashSet()));
            } else {
                verifyEdge(entities.peek().getKey(), edge);
            }

            edges.add(Sets.newHashSet(edge));
            entities.add(new AbstractMap.SimpleEntry<Object, Set<Entity>>(edge.getAdjacentMatchedVertexValue(), Sets.newHashSet()));

            return this;
        }

        public Builder edges(final Edge... edges) {
            if (!distinct(Streams.toStream(edges).map(Edge::getMatchedVertexValue).collect(toList()))
                    && !distinct(Streams.toStream(edges).map(Edge::getAdjacentMatchedVertexValue).collect(toList()))) {
                edgeSet(Sets.newHashSet(edges));
            } else {
                edgeList(Arrays.asList(edges));
            }
            return this;
        }

        public Builder edges(final Iterable<Edge> edges) {
            final List<Edge> edgeList = Streams.toStream(edges).collect(toList());

            if (!distinct(edgeList.stream().map(e -> e.getMatchedVertexValue()).collect(toList()))
                    && !distinct(edgeList.stream().map(e -> e.getAdjacentMatchedVertexValue()).collect(toList()))) {
                edgeSet(Sets.newHashSet(edges));
            } else {
                edgeList(edgeList);
            }
            return this;
        }

        private Builder edgeSet(final Set<Edge> edges) {
            final Object matchedVertexValue = edges.stream()
                    .findFirst()
                    .orElseThrow(IllegalAccessError::new)
                    .getMatchedVertexValue();
            final Object adjacentMatchedVertexValue = edges.stream()
                    .findFirst()
                    .orElseThrow(IllegalAccessError::new)
                    .getAdjacentMatchedVertexValue();

            if (entities.empty()) {
                entities.add(new AbstractMap.SimpleEntry<Object, Set<Entity>>(matchedVertexValue, Sets.newHashSet()));
            } else {
                final Object root = entities.peek().getKey();

                if (!Objects.equals(root, matchedVertexValue)) {
                    throw new IllegalArgumentException("Edge must continue the current walk.");
                }
            }

            this.edges.add(edges);
            entities.add(new AbstractMap.SimpleEntry<Object, Set<Entity>>(adjacentMatchedVertexValue, Sets.newHashSet()));

            return this;
        }

        private Builder edgeList(final List<Edge> edges) {
            edges.forEach(this::edge);
            return this;
        }

        public Builder entity(final Entity entity) {
            if (!edges.empty()) {
                final Object root = edges.peek().stream().findAny().orElseThrow(RuntimeException::new).getAdjacentMatchedVertexValue();

                verifyEntity(root, entity);
            }

            if (!entities.empty()) {
                final Object root = entities.peek().getKey();

                verifyEntity(root, entity);

                final Entry<Object, Set<Entity>> entry = entities.pop();
                final Set<Entity> currentEntities = entry.getValue();
                currentEntities.add(entity);
                entry.setValue(currentEntities);
                entities.push(entry);
            } else {
                entities.push(new AbstractMap.SimpleEntry<Object, Set<Entity>>(entity.getVertex(), Sets.newHashSet(entity)));

            }
            return this;
        }

        public Builder entities(final Iterable<Entity> entities) {
            if (Iterables.isEmpty(entities)) {
                return this;
            }

            if (distinct(Streams.toStream(entities).map(EntityId::getVertex).collect(toList()))) {
                throw new IllegalArgumentException("Entities must all have the same vertex.");
            }

            final Entity entity = entities.iterator().next();

            if (!edges.empty()) {
                final Object root = edges.peek().stream().findAny().orElseThrow(RuntimeException::new).getAdjacentMatchedVertexValue();
                verifyEntity(root, entity);
            }

            if (!this.entities.empty()) {
                final Object root = this.entities.peek().getKey();

                verifyEntity(root, entity);

                final Entry<Object, Set<Entity>> entry = this.entities.pop();
                final Set<Entity> currentEntities = entry.getValue();
                currentEntities.addAll(Lists.newArrayList(entities));
                entry.setValue(currentEntities);
                this.entities.push(entry);
            } else {
                this.entities.push(new AbstractMap.SimpleEntry<Object, Set<Entity>>(entity.getVertex(), Sets.newHashSet(entities)));
            }

            return this;
        }

        public Builder entities(final Entity... entities) {
            final List<Entity> entityList = Arrays.asList(entities);
            if (1 == entityList.size()) {
                entity(entityList.get(0));
            } else {
                entities(entityList);
            }
            return this;
        }

        private void verifyEntity(final Object source, final Entity entity) {
            if (!Objects.equals(source, entity.getVertex())) {
                throw new IllegalArgumentException("Entity must be added to correct vertex.");
            }
        }

        private void verifyEdge(final Object source, final Edge edge) {
            if (!Objects.equals(source, edge.getMatchedVertexValue())) {
                throw new IllegalArgumentException("Edge must continue the current walk.");
            }
        }

        public Walk build() {
            return new Walk(this);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("edges", edges)
                    .append("entities", entities)
                    .toString();
        }
    }
}
