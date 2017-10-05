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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl.IterableElement;
import uk.gov.gchq.gaffer.operation.util.AggregatePair;

import java.util.HashMap;
import java.util.Map;

/**
 * An <code>Aggregate</code> operation applies {@link uk.gov.gchq.gaffer.data.element.function.ElementAggregator}(s) to the provided
 * {@link Iterable} of {@link Element}s by their group, and returns an {@link Iterable}.
 */
public class Aggregate implements Function,
        InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>,
        MultiInput<Element> {
    private Iterable<? extends Element> input;
    private Map<String, String> options;

    /**
     * A map of the {@link uk.gov.gchq.gaffer.data.element.Edge} group to an {@link AggregatePair}, which can contain groupBy properties in a {@link String} array,
     * and an {@link uk.gov.gchq.gaffer.data.element.function.ElementAggregator}.
     */
    private Map<String, AggregatePair> edges;
    /**
     * A map of the {@link uk.gov.gchq.gaffer.data.element.Entity} group to an {@link AggregatePair}, which can contain groupBy properties in a {@link String} array,
     * and an {@link uk.gov.gchq.gaffer.data.element.function.ElementAggregator}.
     */
    private Map<String, AggregatePair> entities;

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new IterableElement();
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new Aggregate.Builder()
                .input(input)
                .options(options)
                .edges(edges)
                .entities(entities)
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
    public Map<String, AggregatePair> getEdges() {
        return edges;
    }

    public void setEdges(final Map<String, AggregatePair> edges) {
        this.edges = edges;
    }

    @Override
    public Map<String, AggregatePair> getEntities() {
        return entities;
    }

    public void setEntities(final Map<String, AggregatePair> entities) {
        this.entities = entities;
    }

    public static final class Builder
            extends Operation.BaseBuilder<Aggregate, Builder>
            implements InputOutput.Builder<Aggregate, Iterable<? extends Element>, Iterable<? extends Element>, Builder>,
            MultiInput.Builder<Aggregate, Element, Builder> {
        public Builder() {
            super(new Aggregate());
        }

        public Builder edge(final String group, final AggregatePair pair) {
            if (null == _getOp().edges) {
                _getOp().edges = new HashMap<>();
            }
            _getOp().edges.put(group, pair);
            return _self();
        }

        public Builder edges(final Map<String, AggregatePair> edges) {
            _getOp().edges = edges;
            return _self();
        }

        public Builder entity(final String group, final AggregatePair pair) {
            if (null == _getOp().entities) {
                _getOp().entities = new HashMap<>();
            }
            _getOp().entities.put(group, pair);
            return _self();
        }

        public Builder entities(final Map<String, AggregatePair> entities) {
            _getOp().entities = entities;
            return _self();
        }
    }
}
