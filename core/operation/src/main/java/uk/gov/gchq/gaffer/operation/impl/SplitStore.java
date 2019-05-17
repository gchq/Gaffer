/*
 * Copyright 2016-2019 Crown Copyright
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
<<<<<<< HEAD
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
=======
>>>>>>> parent of 8ae5ffa172... gh-2157 generated equals/hashcode and fixed erroring tests

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * The {@code SplitStore} operation is for splitting a store
 * based on a sequence file of split points.
 *
 * @see SplitStore.Builder
 * @deprecated use {@link SplitStoreFromFile} instead
 */
@Deprecated
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.0.0")
@Summary("Splits a store based on a sequence file of split points")
public class SplitStore implements Operation {
    @Required
    private String inputPath;
    private Map<String, String> options;

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(final String inputPath) {
        this.inputPath = inputPath;
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
    public SplitStore shallowClone() {
        return new SplitStore.Builder()
                .inputPath(inputPath)
                .options(options)
                .build();
    }

<<<<<<< HEAD
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final SplitStore that = (SplitStore) o;

        return new EqualsBuilder()
                .append(inputPath, that.inputPath)
                .append(options, that.options)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputPath)
                .append(options)
                .toHashCode();
    }

=======
>>>>>>> parent of 8ae5ffa172... gh-2157 generated equals/hashcode and fixed erroring tests
    public static class Builder extends Operation.BaseBuilder<SplitStore, Builder> {
        public Builder() {
            super(new SplitStore());
        }

        public Builder inputPath(final String inputPath) {
            _getOp().setInputPath(inputPath);
            return _self();
        }
    }
}
