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
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl.IterableElement;

import java.util.HashMap;
import java.util.Map;

/**
 * A <code>Transform</code> operation applies provided {@link ElementTransformer}(s) to the provided {@link Iterable} of {@link Element}s,
 * and returns an {@link Iterable}.
 * If only one group is to be queried, simply provide that group and the relevant {@link ElementTransformer}.
 * For multiple groups, a {@link Map} of {@link uk.gov.gchq.gaffer.data.element.Edge}s, or {@link uk.gov.gchq.gaffer.data.element.Entity}s
 * to their relevant {@link ElementTransformer}s can be provided.
 */
public class Transform implements Function,
        InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>,
        MultiInput<Element> {

    private Iterable<? extends Element> input;
    private Map<String, String> options;

    /**
     * Map of {@link uk.gov.gchq.gaffer.data.element.Edge} Group to {@link ElementTransformer} to be applied to that group
     */
    private Map<String, ElementTransformer> edges;

    /**
     * Map of {@link uk.gov.gchq.gaffer.data.element.Entity} Group to {@link ElementTransformer} to be applied to that group
     */
    private Map<String, ElementTransformer> entities;

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
        return new Transform.Builder()
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
    public Map<String, ElementTransformer> getEdges() {
        return edges;
    }

    public void setEdges(final Map<String, ElementTransformer> edges) {
        this.edges = edges;
    }

    @Override
    public Map<String, ElementTransformer> getEntities() {
        return entities;
    }

    public void setEntities(final Map<String, ElementTransformer> entities) {
        this.entities = entities;
    }

    public static final class Builder
            extends Operation.BaseBuilder<Transform, Builder>
            implements InputOutput.Builder<Transform, Iterable<? extends Element>, Iterable<? extends Element>, Builder>,
            MultiInput.Builder<Transform, Element, Builder> {
        public Builder() {
            super(new Transform());
        }

        public Builder entity(final String group, final ElementTransformer elementTransformer) {
            if (null == _getOp().entities) {
                _getOp().entities = new HashMap<>();
            }
            _getOp().entities.put(group, elementTransformer);
            return _self();
        }

        public Builder entities(final Map<String, ElementTransformer> entities) {
            _getOp().entities = entities;
            return _self();
        }

        public Builder edge(final String group, final ElementTransformer elementTransformer) {
            if (null == _getOp().edges) {
                _getOp().edges = new HashMap<>();
            }
            _getOp().edges.put(group, elementTransformer);
            return _self();
        }

        public Builder edges(final Map<String, ElementTransformer> edges) {
            _getOp().edges = edges;
            return _self();
        }
    }
}
