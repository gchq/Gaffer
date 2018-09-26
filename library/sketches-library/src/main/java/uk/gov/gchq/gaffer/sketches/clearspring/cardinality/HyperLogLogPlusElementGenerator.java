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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function.ToHyperLogLogPlus;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Since("1.8.0")
@Summary("Generates HyperLogLogPlus sketch Entities for each end of an Edge")
@JsonPropertyOrder(value = {"group", "hllpPropertyName", "edgeGroupPropertyName", "propertiesToCopy"}, alphabetic = true)
public class HyperLogLogPlusElementGenerator implements OneToManyElementGenerator<Element> {
    private static final ToHyperLogLogPlus TO_HHLP = new ToHyperLogLogPlus();

    private String group = "Cardinality";
    private String hllpPropertyName = "hllp";

    /**
     * The properties to copy from the Edge.
     * IMPORTANT - it does not clone the property values. You could end up with an object being shared between multiple elements.
     */
    private final Set<String> propertiesToCopy = new HashSet<>();

    /**
     * Transforms the new cardinality entities.
     */
    private ElementTransformer transformer = new ElementTransformer();

    @Override
    public Iterable<Element> _apply(final Element element) {
        if (element instanceof Edge) {
            final Edge edge = ((Edge) element);
            return Arrays.asList(
                    edge,
                    createEntity(edge.getSource(), edge.getDestination(), edge),
                    createEntity(edge.getDestination(), edge.getSource(), edge)
            );
        }

        return Collections.singleton(element);
    }

    public ElementTransformer getTransformer() {
        return transformer;
    }

    public void setTransformer(final ElementTransformer transformer) {
        this.transformer = transformer;
    }

    public HyperLogLogPlusElementGenerator transformer(final ElementTransformer transformer) {
        this.transformer = transformer;
        return this;
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

    public HyperLogLogPlusElementGenerator propertyToCopy(final String propertyToCopy) {
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
    public HyperLogLogPlusElementGenerator propertiesToCopy(final String... propertiesToCopy) {
        Collections.addAll(this.propertiesToCopy, propertiesToCopy);
        return this;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(final String group) {
        this.group = group;
    }

    public HyperLogLogPlusElementGenerator group(final String group) {
        this.group = group;
        return this;
    }

    public String getHllpPropertyName() {
        return hllpPropertyName;
    }

    public void setHllpPropertyName(final String hllpPropertyName) {
        this.hllpPropertyName = hllpPropertyName;
    }

    public HyperLogLogPlusElementGenerator hllpPropertyName(final String hllpPropertyName) {
        this.hllpPropertyName = hllpPropertyName;
        return this;
    }

    private Entity createEntity(final Object vertex, final Object adjVertex, final Edge edge) {
        final Entity entity = new Entity.Builder()
                .group(group)
                .vertex(vertex)
                .property(hllpPropertyName, TO_HHLP.apply(adjVertex))
                .property("EDGE_GROUP", edge.getGroup())
                .build();
        for (final String key : propertiesToCopy) {
            final Object value = edge.getProperty(key);
            if (null != value) {
                entity.putProperty(key, value);
            }
        }
        transformer.apply(entity);
        entity.removeProperty("EDGE_GROUP");
        return entity;
    }
}
