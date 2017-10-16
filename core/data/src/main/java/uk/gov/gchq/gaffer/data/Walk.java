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

package uk.gov.gchq.gaffer.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
public class Walk implements Iterable<Set<Edge>> {

    private final List<Set<Edge>> edges;
    private final List<Entry<Object, Set<Entity>>> entities;

    public Walk(final Builder builder) {
        this.entities = builder.entities;
        this.edges = builder.edges;
    }

    @JsonIgnore
    public List<Entity> getEntitiesForVertex(final Object vertex) {
        return entities.stream()
                .filter(e -> e.getKey().equals(vertex))
                .map(e -> e.getValue())
                .flatMap(Set::stream)
                .collect(Collectors.toList());
    }

    @JsonIgnore
    public Set<Entity> getEntitiesAtDistance(final int n) {
        return entities.get(n).getValue();
    }

    public List<Set<Edge>> getEdges() {
        return edges;
    }

    @JsonIgnore
    public List<Entry<Object, Set<Entity>>> getEntitiesAsEntries() {
        return entities;
    }

    public List<Set<Entity>> getEntities() {
        return entities.stream().map(entry -> entry.getValue()).collect(Collectors.toList());
    }

    @JsonIgnore
    public List<Object> getVerticesOrdered() {
        return entities.stream().map(entry -> entry.getKey()).collect(Collectors.toList());
    }

    @JsonIgnore
    public Set<Object> getVertexSet() {
        return entities.stream().map(entry -> entry.getKey()).collect(Collectors.toSet());
    }

    public int length() {
        return edges.size();
    }

    public boolean isTrail() {
        return Sets.newHashSet(edges).size() == edges.size();
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

        public Builder(final Builder builder) {
            this.edges = builder.edges;
            this.entities = builder.entities;
        }

        public Builder edge(final Edge edge) {
            if (entities.empty()) {
                entities.add(new AbstractMap.SimpleEntry<Object, Set<Entity>>(edge.getMatchedVertexValue(), Sets.newHashSet()));
            } else {
                final Object root = entities.peek().getKey();

                if (!Objects.equals(root, edge.getMatchedVertexValue())) {
                    throw new IllegalArgumentException("Edge must continue the current walk.");
                }
            }

            edges.add(Sets.newHashSet(edge));
            entities.add(new AbstractMap.SimpleEntry<Object, Set<Entity>>(edge.getAdjacentMatchedVertexValue(), Sets.newHashSet()));

            return this;
        }

        public Builder edges(final Edge... edges) {
            final List<Edge> edgeList = Arrays.asList(edges);

            if (edgeList.stream().map(e -> e.getMatchedVertexValue()).distinct().count() == 1
                    && edgeList.stream().map(e -> e.getAdjacentMatchedVertexValue()).distinct().count() == 1) {
                edgeSet(Sets.newHashSet(edges));
            } else {
                edgeList(edgeList);
            }
            return this;
        }

        public Builder edges(final Iterable<Edge> edges) {
            final List<Edge> edgeList = Streams.toStream(edges).collect(Collectors.toList());

            if (edgeList.stream().map(e -> e.getMatchedVertexValue()).distinct().count() == 1
                    && edgeList.stream().map(e -> e.getAdjacentMatchedVertexValue()).distinct().count() == 1) {
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

                if (root != entity.getVertex()) {
                    throw new IllegalArgumentException("Entity must be added to correct vertex.");
                }
            }

            if (!entities.empty()) {
                final Object root = entities.peek().getKey();

                if (root != entity.getVertex()) {
                    throw new IllegalArgumentException("Entity must be added to correct vertex.");
                }

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

            if (Streams.toStream(entities).map(EntityId::getVertex).distinct().count() != 1) {
                throw new IllegalArgumentException("Entities must all have the same vertex.");
            }

            final Object vertex = entities.iterator().next().getVertex();

            if (!edges.empty()) {
                final Object root = edges.peek().stream().findAny().orElseThrow(RuntimeException::new).getAdjacentMatchedVertexValue();
                if (root != vertex) {
                    throw new IllegalArgumentException("Entity must be added to correct vertex.");
                }
            }

            if (!this.entities.empty()) {
                final Object root = this.entities.peek().getKey();

                if (root != vertex) {
                    throw new IllegalArgumentException("Entity must be added to correct vertex.");
                }
                final Entry<Object, Set<Entity>> entry = this.entities.pop();
                final Set<Entity> currentEntities = entry.getValue();
                currentEntities.addAll(Lists.newArrayList(entities));
                entry.setValue(currentEntities);
                this.entities.push(entry);
            } else {
                this.entities.push(new AbstractMap.SimpleEntry<Object, Set<Entity>>(vertex, Sets.newHashSet(entities)));
            }

            return this;
        }

        public Builder entities(final Entity... entities) {
            entities(Arrays.asList(entities));
            return this;
        }

        public int length() {
            return edges.size();
        }

        public Walk build() {
            return new Walk(this);
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            return new Walk.Builder(this);
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
