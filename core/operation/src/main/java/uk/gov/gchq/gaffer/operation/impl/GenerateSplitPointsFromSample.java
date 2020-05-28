/*
 * Copyright 2020 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * The {@code GenerateSplitPointsFromSample} operation takes an {@link Iterable} sample
 * and generates a {@link List} of split points from that. The sample and
 * split points are normally UTF strings but this may differ for different
 * Stores.
 * </p>
 * <p>
 * You can manually set the number of splits using the numSplits field. If
 * you don't set it then the Gaffer Store should calculate a number of splits
 * for you.
 * </p>
 *
 * @param <T> the type of splits
 * @see GenerateSplitPointsFromSample.Builder
 */
@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("1.12.0")
@Summary("Generates split points from the supplied Iterable")
public class GenerateSplitPointsFromSample<T> implements
        Operation,
        InputOutput<Iterable<? extends T>, List<T>>,
        MultiInput<T> {

    private Iterable<? extends T> input;
    private Integer numSplits;
    private Map<String, String> options;

    @Override
    public ValidationResult validate() {
        final ValidationResult result = InputOutput.super.validate();
        if (null != numSplits && numSplits < 1) {
            result.addError("numSplits must be null or greater than 0");
        }
        return result;
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
    public TypeReference<List<T>> getOutputTypeReference() {
        return new TypeReferenceImpl.List<>();
    }

    @Override
    public GenerateSplitPointsFromSample<T> shallowClone() throws CloneFailedException {
        return new GenerateSplitPointsFromSample.Builder<T>()
                .input(input)
                .numSplits(numSplits)
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

    public Integer getNumSplits() {
        return numSplits;
    }

    public void setNumSplits(final Integer numSplits) {
        this.numSplits = numSplits;
    }

    public static class Builder<T> extends Operation.BaseBuilder<GenerateSplitPointsFromSample<T>, GenerateSplitPointsFromSample.Builder<T>>
            implements InputOutput.Builder<GenerateSplitPointsFromSample<T>, Iterable<? extends T>, List<T>, GenerateSplitPointsFromSample.Builder<T>>,
            MultiInput.Builder<GenerateSplitPointsFromSample<T>, T, GenerateSplitPointsFromSample.Builder<T>> {

        public Builder() {
            super(new GenerateSplitPointsFromSample<>());
        }

        public GenerateSplitPointsFromSample.Builder<T> numSplits(final Integer numSplits) {
            _getOp().setNumSplits(numSplits);
            return this;
        }
    }
}
