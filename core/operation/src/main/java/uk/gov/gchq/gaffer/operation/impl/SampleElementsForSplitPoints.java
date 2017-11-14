/*
 * Copyright 2016 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;
import java.util.Map;


/**
 * <p>
 * The {@code SampleDataForSplitPoints} operation is for sampling an {@link Iterable}
 * of {@link Element}s and generating split points. The operation returns a
 * {@link List} of the split points. The split points are normally UTF strings
 * but this may differ for different Stores.
 * </p>
 * <p>
 * By default, all elements in the iterable will used to generate splits.
 * If you wish to only sample the iterable you can set the proportionToSample
 * field to a value between 0 and 1.
 * </p>
 * <p>
 * You can manually set the number of splits using the numSplits field. If
 * you don't set it then the Gaffer Store should calculate a number of splits
 * for you.
 * </p>
 * <p>
 * If you want to only use the first few elements in the iterable then you
 * can chain this operation after a {@link Limit} Operation.
 * </p>
 * <p>
 * Depending on the Store you run this operation against there may be a limit
 * to the number of elements you are allowed to include in the sample.
 * </p>
 *
 * @param <T> the type of splits
 * @see SampleElementsForSplitPoints.Builder
 */
public class SampleElementsForSplitPoints<T> implements
        Operation,
        InputOutput<Iterable<? extends Element>, List<T>>,
        MultiInput<Element> {

    private Iterable<? extends Element> input;
    private Integer numSplits;
    private float proportionToSample = 1f;
    private Map<String, String> options;

    @Override
    public ValidationResult validate() {
        final ValidationResult result = InputOutput.super.validate();
        if (null != numSplits && numSplits < 1) {
            result.addError("numSplits must be null or greater than 0");
        }
        if (proportionToSample > 1 || proportionToSample < 0) {
            result.addError("proportionToSample must within range: [0, 1]");
        }
        return result;
    }

    @Override
    public SampleElementsForSplitPoints<T> shallowClone() {
        return new SampleElementsForSplitPoints.Builder<T>()
                .input(input)
                .numSplits(numSplits)
                .proportionToSample(proportionToSample)
                .options(options)
                .build();
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

    public float getProportionToSample() {
        return proportionToSample;
    }

    public void setProportionToSample(final float proportionToSample) {
        this.proportionToSample = proportionToSample;
    }

    @Override
    public TypeReference<List<T>> getOutputTypeReference() {
        return new TypeReferenceImpl.List<>();
    }

    public static class Builder<T> extends Operation.BaseBuilder<SampleElementsForSplitPoints<T>, Builder<T>>
            implements InputOutput.Builder<SampleElementsForSplitPoints<T>, Iterable<? extends Element>, List<T>, Builder<T>>,
            MultiInput.Builder<SampleElementsForSplitPoints<T>, Element, Builder<T>> {
        public Builder() {
            super(new SampleElementsForSplitPoints<>());
        }

        public Builder<T> numSplits(final Integer numSplits) {
            _getOp().setNumSplits(numSplits);
            return this;
        }

        public Builder<T> proportionToSample(final float proportionToSample) {
            _getOp().setProportionToSample(proportionToSample);
            return _self();
        }
    }
}
