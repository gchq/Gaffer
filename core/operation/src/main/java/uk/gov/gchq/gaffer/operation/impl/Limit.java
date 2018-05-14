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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code Limit} operation takes in an {@link Iterable} of items
 * and limits the iterable to a given number of items. It simply wraps the input
 * iterable in a {@link uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable} so
 * the data is not stored in memory.
 *
 * @see Limit.Builder
 */
@JsonPropertyOrder(value = {"class", "input", "resultsLimit"}, alphabetic = true)
@Since("1.0.0")
@Summary("Limits the number of items")
public class Limit<T> implements
        InputOutput<Iterable<? extends T>, Iterable<? extends T>>,
        MultiInput<T> {
    @Required
    protected Integer resultLimit;
    private Iterable<? extends T> input;
    private boolean truncate = true;
    private Map<String, String> options;

    public Limit() {
    }

    public Limit(final Integer resultLimit) {
        this.resultLimit = resultLimit;
    }

    public Limit(final Integer resultLimit, final boolean truncate) {
        this.resultLimit = resultLimit;
        this.truncate = truncate;
    }

    public Integer getResultLimit() {
        return resultLimit;
    }

    public void setResultLimit(final Integer resultLimit) {
        this.resultLimit = resultLimit;
    }

    public Boolean getTruncate() {
        return truncate;
    }

    public void setTruncate(final boolean truncate) {
        this.truncate = truncate;
    }

    @Override
    public Iterable<? extends T> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends T> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends T>> getOutputTypeReference() {
        return TypeReferenceImpl.createIterableT();
    }

    @Override
    public Limit<T> shallowClone() {
        return new Limit.Builder<T>()
                .resultLimit(resultLimit)
                .input(input)
                .truncate(truncate)
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

    public static final class Builder<T>
            extends Operation.BaseBuilder<Limit<T>, Builder<T>>
            implements InputOutput.Builder<Limit<T>, Iterable<? extends T>, Iterable<? extends T>, Builder<T>>,
            MultiInput.Builder<Limit<T>, T, Builder<T>> {
        public Builder() {
            super(new Limit<>());
        }

        public Builder<T> resultLimit(final Integer resultLimit) {
            _getOp().setResultLimit(resultLimit);
            return _self();
        }

        public Builder<T> truncate(final Boolean truncate) {
            _getOp().setTruncate(truncate);
            return _self();
        }
    }
}
