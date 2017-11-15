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
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.util.Map;
import java.util.function.Function;

public class IterableMap<I_ITEM, O_ITEM> implements
        InputOutput<Iterable<? extends I_ITEM>, Iterable<? extends O_ITEM>>,
        MultiInput<I_ITEM> {

    private Iterable<? extends I_ITEM> input;
    private Map<String, String> options;
    @Required
    private Function<I_ITEM, O_ITEM> function;

    public IterableMap() {
        // Empty
    }

    @Override
    public Iterable<? extends I_ITEM> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends I_ITEM> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends O_ITEM>> getOutputTypeReference() {
        return TypeReferenceImpl.createIterableT();
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new IterableMap.Builder<I_ITEM, O_ITEM>()
                .input(input)
                .function(function)
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

    public Function<I_ITEM, O_ITEM> getFunction() {
        return function;
    }

    public void setFunction(final Function<I_ITEM, O_ITEM> function) {
        this.function = function;
    }

    public static final class Builder<I_ITEM, O_ITEM> extends
            Operation.BaseBuilder<IterableMap<I_ITEM, O_ITEM>, Builder<I_ITEM, O_ITEM>> implements
            InputOutput.Builder<IterableMap<I_ITEM, O_ITEM>, Iterable<? extends I_ITEM>, Iterable<? extends O_ITEM>, Builder<I_ITEM, O_ITEM>>,
            MultiInput.Builder<IterableMap<I_ITEM, O_ITEM>, I_ITEM, Builder<I_ITEM, O_ITEM>> {
        public Builder() { super(new IterableMap<>()); }

        public Builder<I_ITEM, O_ITEM> function(final Function<I_ITEM, O_ITEM> func) {
            _getOp().setFunction(func);
            return _self();
        }
    }
}
