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

import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code CountGroups} operation takes in {@link Element}s and collects
 * counts for the number of entity and edge groups used. To avoid counting all
 * elements in the store, this operation has a limit, which can be set to
 * skip counting the remaining groups.
 *
 * @see CountGroups.Builder
 */
@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("1.0.0")
@Summary("Counts the different element groups")
public class CountGroups implements
        InputOutput<Iterable<? extends Element>, GroupCounts>,
        MultiInput<Element> {
    private Iterable<? extends Element> input;
    private Integer limit;
    private Map<String, String> options;

    public CountGroups() {
    }

    public CountGroups(final Integer limit) {
        this.limit = limit;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(final Integer limit) {
        this.limit = limit;
    }

    @Override
    public TypeReference<GroupCounts> getOutputTypeReference() {
        return new TypeReferenceImpl.CountGroups();
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
    public CountGroups shallowClone() {
        return new CountGroups.Builder()
                .input(input)
                .limit(limit)
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

    public static class Builder
            extends Operation.BaseBuilder<CountGroups, Builder>
            implements InputOutput.Builder<CountGroups, Iterable<? extends Element>, GroupCounts, Builder>,
            MultiInput.Builder<CountGroups, Element, Builder> {

        public Builder() {
            super(new CountGroups());
        }

        /**
         * @param limit the limit of group counts to calculate.
         * @return this Builder
         * @see CountGroups#setLimit(Integer)
         */
        public Builder limit(final Integer limit) {
            _getOp().setLimit(limit);
            return this;
        }
    }
}
