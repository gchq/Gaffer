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
package uk.gov.gchq.gaffer.spark.operation.scalardd;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.exception.CloneFailedException;
import org.apache.spark.rdd.RDD;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Map;

@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("1.12.0")
@Summary("Sets split points on a store derived from sampling an RDD of elements")
public class SplitStoreFromRDDOfElements implements
        Operation,
        Input<RDD<Element>> {

    private RDD<Element> input;
    private Double fractionToSample;
    private Integer maxSampleSize;
    private Integer numSplits;
    private Map<String, String> options;

    @Override
    public ValidationResult validate() {

        final ValidationResult result = Input.super.validate();

        if (null != fractionToSample && (fractionToSample <= 0 || fractionToSample > 1)) {
            result.addError("fractionToSample must be null or between 0 exclusive and 1 inclusive");
        }
        if (null != maxSampleSize && maxSampleSize < 1) {
            result.addError("maxSampleSize must be null or greater than 0");
        }
        if (null != numSplits && numSplits < 1) {
            result.addError("numSplits must be null or greater than 0");
        }

        return result;
    }

    @Override
    public RDD<Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final RDD<Element> input) {
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

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new SplitStoreFromRDDOfElements.Builder()
                .input(input)
                .fractionToSample(fractionToSample)
                .maxSampleSize(maxSampleSize)
                .numSplits(numSplits)
                .options(options)
                .build();
    }

    public Double getFractionToSample() {
        return fractionToSample;
    }

    public void setFractionToSample(final Double fractionToSample) {
        this.fractionToSample = fractionToSample;
    }

    public Integer getMaxSampleSize() {
        return maxSampleSize;
    }

    public void setMaxSampleSize(final Integer maxSampleSize) {
        this.maxSampleSize = maxSampleSize;
    }

    public Integer getNumSplits() {
        return numSplits;
    }

    public void setNumSplits(final Integer numSplits) {
        this.numSplits = numSplits;
    }

    public static class Builder extends BaseBuilder<SplitStoreFromRDDOfElements, Builder>
            implements Input.Builder<SplitStoreFromRDDOfElements, RDD<Element>, Builder>,
            Operation.Builder<SplitStoreFromRDDOfElements, Builder> {
        public Builder() {
            super(new SplitStoreFromRDDOfElements());
        }

        public SplitStoreFromRDDOfElements.Builder fractionToSample(final Double fractionToSample) {
            _getOp().setFractionToSample(fractionToSample);
            return this;
        }

        public SplitStoreFromRDDOfElements.Builder maxSampleSize(final Integer maxSampleSize) {
            _getOp().setMaxSampleSize(maxSampleSize);
            return this;
        }

        public SplitStoreFromRDDOfElements.Builder numSplits(final Integer numSplits) {
            _getOp().setNumSplits(numSplits);
            return this;
        }
    }
}
