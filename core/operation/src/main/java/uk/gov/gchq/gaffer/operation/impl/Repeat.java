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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@JsonPropertyOrder(value = {"class", "input", "operation", "times"}, alphabetic = true)
public class Repeat implements InputOutput<Object, Object>,
        Operations<Operation> {
    private Operation operation;
    private int times;
    private Object input;
    private Map<String, String> options;

    public Repeat() {

    }

    public Repeat(final Operation operation, final int times) {
        this.operation = operation;
        this.times = times;
    }

    public void setOperation(final Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setTimes(final int times) {
        this.times = times;
    }

    public int getTimes() {
        return times;
    }

    @Override
    public Object getInput() {
        return input;
    }

    @Override
    public void setInput(final Object input) {
        this.input = input;
    }

    @Override
    public TypeReference<Object> getOutputTypeReference() {
        return new TypeReferenceImpl.Object();
    }

    @Override
    public Repeat shallowClone() throws CloneFailedException {
        return new Repeat.Builder()
                .input(input)
                .operation(operation)
                .times(times)
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
    public Collection<Operation> getOperations() {
        final List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < times; i++) {
            ops.add(operation);
        }
        return ops;
    }

    public static final class Builder
            extends Operation.BaseBuilder<Repeat, Builder>
            implements InputOutput.Builder<Repeat, Object, Object, Builder> {
        public Builder() {
            super(new Repeat());
        }

        public Builder operation(final Operation op) {
            _getOp().setOperation(op);
            return _self();
        }

        public Builder times(final int times) {
            _getOp().setTimes(times);
            return _self();
        }
    }
}
