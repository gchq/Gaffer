/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches;

import com.fasterxml.jackson.annotation.JsonInclude;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public abstract class CardinalityEntityGenerator<T> implements OneToManyElementGenerator<Element> {
    private final Function<Object, T> toSketch;

    private String group = "Cardinality";
    private String cardinalityPropertyName = "cardinality";
    private String countProperty;
    private String edgeGroupProperty;

    /**
     * The properties to copy from the Edge.
     * IMPORTANT - it does not clone the property values. You could end up with an object being shared between multiple elements.
     */
    private final Set<String> propertiesToCopy = new HashSet<>();

    public CardinalityEntityGenerator(final Function<Object, T> toSketch) {
        this.toSketch = toSketch;
    }

    @Override
    public Iterable<Element> _apply(final Element element) {
        if (isNull(element)) {
            return Collections.emptyList();
        }

        if (element instanceof Edge) {
            final Edge edge = ((Edge) element);
            final Entity sourceEntity = createEntity(edge.getSource(), edge.getDestination(), edge);
            final Entity destEntity = createEntity(edge.getDestination(), edge.getSource(), edge);

            final List<Element> elements = new ArrayList<>(3);
            elements.add(edge);
            if (nonNull(sourceEntity)) {
                elements.add(sourceEntity);
            }
            if (nonNull(destEntity)) {
                elements.add(destEntity);
            }
            return elements;
        }

        return Collections.singleton(element);
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public Set<String> getPropertiesToCopy() {
        return propertiesToCopy;
    }

    /**
     * <p>
     * Copies the properties from the edge to the new entities.
     * </p>
     * <p>
     * IMPORTANT - it does not clone the property values. You could end up with an object being shared between multiple elements.
     * </p>
     *
     * @param propertiesToCopy the properties to copy from the edge
     */
    public void setPropertiesToCopy(final Collection<String> propertiesToCopy) {
        requireNonNull(propertiesToCopy, "propertiesToCopy is required");
        this.propertiesToCopy.addAll(propertiesToCopy);
    }

    public CardinalityEntityGenerator propertyToCopy(final String propertyToCopy) {
        this.propertiesToCopy.add(propertyToCopy);
        return this;
    }

    /**
     * <p>
     * Copies the properties from the edge to the new entities.
     * </p>
     * <p>
     * IMPORTANT - it does not clone the property values. You could end up with an object being shared between multiple elements.
     * </p>
     *
     * @param propertiesToCopy the properties to copy from the edge
     * @return this
     */
    public CardinalityEntityGenerator propertiesToCopy(final String... propertiesToCopy) {
        Collections.addAll(this.propertiesToCopy, propertiesToCopy);
        return this;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(final String group) {
        this.group = group;
    }

    public CardinalityEntityGenerator group(final String group) {
        this.group = group;
        return this;
    }

    public String getCardinalityPropertyName() {
        return cardinalityPropertyName;
    }

    public void setCardinalityPropertyName(final String cardinalityPropertyName) {
        this.cardinalityPropertyName = cardinalityPropertyName;
    }

    public CardinalityEntityGenerator cardinalityPropertyName(final String cardinalityPropertyName) {
        this.cardinalityPropertyName = cardinalityPropertyName;
        return this;
    }

    public String getCountProperty() {
        return countProperty;
    }

    public void setCountProperty(final String countProperty) {
        this.countProperty = countProperty;
    }

    public CardinalityEntityGenerator countProperty(final String countProperty) {
        this.countProperty = countProperty;
        return this;
    }

    public String getEdgeGroupProperty() {
        return edgeGroupProperty;
    }

    public void setEdgeGroupProperty(final String edgeGroupProperty) {
        this.edgeGroupProperty = edgeGroupProperty;
    }


    public CardinalityEntityGenerator edgeGroupProperty(final String edgeGroupProperty) {
        this.edgeGroupProperty = edgeGroupProperty;
        return this;
    }

    private Entity createEntity(final Object vertex, final Object adjVertex, final Edge edge) {
        if (isNull(vertex)) {
            return null;
        }

        final Entity entity = new Entity.Builder()
                .group(group)
                .vertex(vertex)
                .property(cardinalityPropertyName, toSketch.apply(adjVertex))
                .build();
        for (final String key : propertiesToCopy) {
            final Object value = edge.getProperty(key);
            if (null != value) {
                entity.putProperty(key, value);
            }
        }
        if (null != countProperty) {
            entity.putProperty(countProperty, 1L);
        }
        if (null != edgeGroupProperty) {
            entity.putProperty(edgeGroupProperty, CollectionUtil.treeSet(edge.getGroup()));
        }
        return entity;
    }
}
