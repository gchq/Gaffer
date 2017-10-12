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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A {@code Path} class is used to retrieve all of the paths in a graph starting
 * from one of a set of provided {@link EntitySeed}s, with a maximum length.
 */
public class Path implements
        InputOutput<Iterable<? extends EntitySeed>, Iterable<Iterable<Edge>>>,
        MultiInput<EntitySeed> {

    private List<GetElements> operations;
    private Iterable<? extends EntitySeed> input;
    private Map<String, String> options;

    @Override
    public Iterable<? extends EntitySeed> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends EntitySeed> input) {
        this.input = input;
    }

    public List<GetElements> getOperations() {
        return operations;
    }

    public void setOperations(final List<GetElements> operations) {
        this.operations = operations;
    }

    @Override
    public TypeReference<Iterable<Iterable<Edge>>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableIterableEdge();
    }

    @Override
    public Path shallowClone() throws CloneFailedException {
        return new Path.Builder()
                .input(input)
                .operations(operations)
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

    public static final class Builder
            extends Operation.BaseBuilder<Path, Builder>
            implements InputOutput.Builder<Path, Iterable<? extends EntitySeed>, Iterable<Iterable<Edge>>, Builder>,
            MultiInput.Builder<Path, EntitySeed, Builder> {

        public Builder() {
            super(new Path());
        }

        public Builder operations(final Iterable<GetElements> operations) {
            if (null != operations) {
                _getOp().setOperations(Lists.newArrayList(operations));
            }
            return _self();
        }

        public Builder operations(final GetElements... operations) {
            _getOp().setOperations(Arrays.asList(operations));
            return _self();
        }
    }
}
