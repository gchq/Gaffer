/*
 * Copyright 2021 Crown Copyright
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
package uk.gov.gchq.gaffer.data.element.function;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.binaryoperator.First;
import uk.gov.gchq.koryphe.util.IterableUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

/**
 * A {@code ReduceRelatedElements} is a {@link KorypheFunction} which takes an {@link Iterable} of {@link Element}s and
 * combines all related elements using the provided aggregator functions.
 */
@Since("1.19.0")
@Summary("Reduces related elements")
public class ReduceRelatedElements extends KorypheFunction<Iterable<Element>, Iterable<Element>> {
    private BinaryOperator<Object> vertexAggregator = new First();

    private BinaryOperator<Object> visibilityAggregator;
    private String visibilityProperty;

    private Set<String> relatedVertexGroups;

    @Override
    public Iterable<Element> apply(final Iterable<Element> elements) {
        if (isNull(elements)) {
            return null;
        }

        // If no related vertex groups are provided then there is nothing to reduce
        if (isEmpty(relatedVertexGroups)) {
            return elements;
        }

        final Map<Object, RelatedVertices> relatedVertices = new HashMap<>();
        final List<Edge> edges = new ArrayList<>();
        final List<Entity> entities = new ArrayList<>();
        for (final Element element : elements) {
            if (nonNull(element)) {
                if (element instanceof Edge) {
                    final Edge edge = (Edge) element;
                    if (relatedVertexGroups.contains(edge.getGroup())) {
                        updateRelatedVertices(edge, relatedVertices);
                    } else {
                        edges.add(edge);
                    }
                } else {
                    final Entity entity = (Entity) element;
                    entities.add(entity);
                }
            }
        }

        for (final Edge edge : edges) {
            final Object source = edge.getSource();
            final Object dest = edge.getDestination();

            final RelatedVertices srcVertices = relatedVertices.get(source);
            final Object srcCombinedVertex = getCombinedVertex(source, relatedVertices.get(source));

            final RelatedVertices destVertices = relatedVertices.get(dest);
            final Object destCombinedVertex = getCombinedVertex(dest, relatedVertices.get(dest));

            edge.setIdentifiers(srcCombinedVertex, destCombinedVertex, edge.isDirected(), edge.getMatchedVertex());

            addRelatedVertexProperties(edge, srcVertices, "source", srcCombinedVertex);
            addRelatedVertexProperties(edge, destVertices, "destination", destCombinedVertex);
        }

        for (final Entity entity : entities) {
            final Object vertex = entity.getVertex();
            final RelatedVertices vertices = relatedVertices.get(vertex);
            final Object combinedVertex = getCombinedVertex(vertex, relatedVertices.get(vertex));
            entity.setVertex(combinedVertex);
            addRelatedVertexProperties(entity, vertices, "", combinedVertex);
        }

        return IterableUtil.concat(Arrays.asList(edges, entities));
    }

    private void addRelatedVertexProperties(final Element element, final RelatedVertices relatedVertices, final String prefix, final Object combinedVertex) {
        if (nonNull(relatedVertices)) {
            if (relatedVertices.size() > 1) {
                String propertyName;
                if (StringUtils.isNotEmpty(prefix)) {
                    propertyName = prefix + "RelatedVertices";
                } else {
                    propertyName = "relatedVertices";
                }

                Collection<Object> propertyValue = (Collection<Object>) element.getProperty(propertyName);
                if (isNull(propertyValue)) {
                    propertyValue = new HashSet<>(relatedVertices);
                    element.putProperty(propertyName, propertyValue);
                } else {
                    propertyValue.addAll(relatedVertices);
                }
                propertyValue.remove(combinedVertex);
            }
            if (nonNull(visibilityProperty)) {
                final Object aggVisibility = combineVisibilities(element.getProperty(visibilityProperty), relatedVertices.getVisibility());
                relatedVertices.setVisibility(aggVisibility);
            }
        }
    }

    private Object getCombinedVertex(final Object source, final RelatedVertices relatedVertices) {
        if (isEmpty(relatedVertices)) {
            return source;
        }

        Object combinedVertex = source;
        for (final Object relatedVertex : relatedVertices) {
            combinedVertex = vertexAggregator.apply(combinedVertex, relatedVertex);
        }

        return combinedVertex;
    }

    private void updateRelatedVertices(final Edge edge, final Map<Object, RelatedVertices> relatedVertices) {
        final Object source = edge.getSource();
        final Object dest = edge.getDestination();
        final Object visibility = edge.getProperty(visibilityProperty);

        RelatedVertices srcVertices = relatedVertices.get(source);
        RelatedVertices destVertices = relatedVertices.get(dest);
        if (nonNull(srcVertices) && nonNull(destVertices)) {
            if (srcVertices != destVertices) {  // check if the objects are the same (has the same address)
                srcVertices.add(dest);
                srcVertices.addAll(destVertices);
                for (final Object vertex : srcVertices) {
                    relatedVertices.put(vertex, srcVertices);
                }
            }
        } else if (nonNull(srcVertices)) {
            srcVertices.add(dest);
            relatedVertices.put(dest, srcVertices);
        } else if (nonNull(destVertices)) {
            srcVertices = destVertices;
            srcVertices.add(source);
            srcVertices.add(dest);
            relatedVertices.put(source, srcVertices);
            relatedVertices.put(dest, srcVertices);
        } else {
            srcVertices = new RelatedVertices();
            srcVertices.add(source);
            srcVertices.add(dest);
            relatedVertices.put(source, srcVertices);
            relatedVertices.put(dest, srcVertices);
        }

        if (nonNull(visibility)) {
            final Object aggVisibility = combineVisibilities(visibility, srcVertices.getVisibility());
            srcVertices.setVisibility(aggVisibility);
        }
    }

    private Object combineVisibilities(final Object visibility1, final Object visibility2) {
        if (isNull(visibility1)) {
            return visibility2;
        }

        if (isNull(visibility2)) {
            return visibility1;
        }

        if (visibility1.equals(visibility2)) {
            return visibility1;
        }

        if (isNull(visibilityAggregator)) {
            throw new IllegalArgumentException("No visibility aggregator provided, so visibilities cannot be combined.");
        }

        return visibilityAggregator.apply(visibility1, visibility2);
    }

    private static final class RelatedVertices extends HashSet<Object> {
        private static final long serialVersionUID = 2778585598526500913L;
        private Object visibility;

        public Object getVisibility() {
            return visibility;
        }

        public void setVisibility(final Object visibility) {
            this.visibility = visibility;
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public BinaryOperator<Object> getVisibilityAggregator() {
        return visibilityAggregator;
    }

    public void setVisibilityAggregator(final BinaryOperator<?> visibilityAggregator) {
        this.visibilityAggregator = (BinaryOperator<Object>) visibilityAggregator;
    }

    public String getVisibilityProperty() {
        return visibilityProperty;
    }

    public void setVisibilityProperty(final String visibilityProperty) {
        this.visibilityProperty = visibilityProperty;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public BinaryOperator<Object> getVertexAggregator() {
        return vertexAggregator;
    }

    public void setVertexAggregator(final BinaryOperator<?> vertexAggregator) {
        this.vertexAggregator = (BinaryOperator<Object>) vertexAggregator;
    }

    public Set<String> getRelatedVertexGroups() {
        return relatedVertexGroups;
    }

    public void setRelatedVertexGroups(final Set<String> relatedVertexGroups) {
        this.relatedVertexGroups = relatedVertexGroups;
    }
}
