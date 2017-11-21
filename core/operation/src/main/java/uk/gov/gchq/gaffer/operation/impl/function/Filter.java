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
package uk.gov.gchq.gaffer.operation.impl.function;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl.IterableElement;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A <code>Filter</code> operation applies {@link ElementFilter}(s) to the provided
 * {@link Iterable} of {@link  Element}s and returns an {@link Iterable}.
 * The {@link ElementFilter} can be applied to a specific group of
 * {@link  uk.gov.gchq.gaffer.data.element.Edge} or {@link uk.gov.gchq.gaffer.data.element.Entity},
 * to any {@link  uk.gov.gchq.gaffer.data.element.Edge} or {@link uk.gov.gchq.gaffer.data.element.Entity},
 * or simply to any {@link Element} in the {@link Iterable}.
 * For more complex queries, a {@link Map} of groups to {@link ElementFilter}s can also be provided to either
 * {@link  uk.gov.gchq.gaffer.data.element.Edge}s or {@link uk.gov.gchq.gaffer.data.element.Entity}s.
 */
public class Filter implements Function,
        InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>,
        MultiInput<Element> {

    private Iterable<? extends Element> input;

    /**
     * Map of edge group to ElementFilter.
     */
    private Map<String, ElementFilter> edges;

    /**
     * Map of entity group to ElementFilter.
     */
    private Map<String, ElementFilter> entities;

    private ElementFilter globalElements;
    private ElementFilter globalEntities;
    private ElementFilter globalEdges;

    private Map<String, String> options;

    @Override
    public Filter shallowClone() throws CloneFailedException {
        return new Filter.Builder()
                .input(input)
                .entities(entities)
                .edges(edges)
                .globalElements(globalElements)
                .globalEdges(globalEdges)
                .globalEntities(globalEntities)
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    @Override
    public Map<String, ElementFilter> getEdges() {
        return edges;
    }

    public void setEdges(final Map<String, ElementFilter> edges) {
        this.edges = edges;
    }

    @Override
    public Map<String, ElementFilter> getEntities() {
        return entities;
    }

    public void setEntities(final Map<String, ElementFilter> entities) {
        this.entities = entities;
    }

    public ElementFilter getGlobalElements() {
        return globalElements;
    }

    public void setGlobalElements(final ElementFilter globalElements) {
        this.globalElements = globalElements;
    }

    public ElementFilter getGlobalEntities() {
        return globalEntities;
    }

    public void setGlobalEntities(final ElementFilter globalEntities) {
        this.globalEntities = globalEntities;
    }

    public ElementFilter getGlobalEdges() {
        return globalEdges;
    }

    public void setGlobalEdges(final ElementFilter globalEdges) {
        this.globalEdges = globalEdges;
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new IterableElement();
    }


    public static final class Builder
            extends Operation.BaseBuilder<Filter, Builder>
            implements InputOutput.Builder<Filter, Iterable<? extends Element>, Iterable<? extends Element>, Builder>,
            MultiInput.Builder<Filter, Element, Builder> {
        public Builder() {
            super(new Filter());
        }


        public Builder entity(final String group) {
            return entity(group, new ElementFilter());
        }

        public Builder entity(final String group, final ElementFilter elementFilter) {
            if (null == _getOp().entities) {
                _getOp().entities = new HashMap<>();
            }
            _getOp().entities.put(group, elementFilter);
            return _self();
        }

        public Builder entities(final Map<String, ElementFilter> entities) {
            _getOp().entities = entities;
            return _self();
        }

        public Builder entities(final Collection<String> groups) {
            for (final String group : groups) {
                entity(group);
            }
            return _self();
        }

        public Builder edge(final String group) {
            return edge(group, new ElementFilter());
        }

        public Builder edge(final String group, final ElementFilter elementFilter) {
            if (null == _getOp().edges) {
                _getOp().edges = new HashMap<>();
            }
            _getOp().edges.put(group, elementFilter);
            return _self();
        }

        public Builder edges(final Collection<String> groups) {
            for (final String group : groups) {
                edge(group);
            }
            return _self();
        }

        public Builder edges(final Map<String, ElementFilter> edges) {
            _getOp().edges = edges;
            return _self();
        }

        public Builder globalElements(final ElementFilter globalElements) {
            _getOp().globalElements = globalElements;
            return _self();
        }

        public Builder globalEntities(final ElementFilter globalEntities) {
            _getOp().globalEntities = globalEntities;
            return _self();
        }

        public Builder globalEdges(final ElementFilter globalEdges) {
            _getOp().globalEdges = globalEdges;
            return _self();
        }
    }
}
