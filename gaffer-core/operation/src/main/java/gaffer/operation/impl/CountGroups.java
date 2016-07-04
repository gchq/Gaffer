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

package gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import gaffer.data.GroupCounts;
import gaffer.data.element.Element;
import gaffer.operation.AbstractOperation;
import java.util.List;

/**
 * A <code>CountGroups</code> operation takes in {@link Element}s and collects
 * counts for the number of entity and edge groups used. To avoid counting all
 * elements in the store, this operation has a limit, which can be set to
 * skip counting the remaining groups.
 *
 * @see CountGroups.Builder
 */
public class CountGroups extends AbstractOperation<Iterable<Element>, GroupCounts> {
    private Integer limit;

    public CountGroups() {
        this(null);
    }

    public CountGroups(final Integer limit) {
        this.limit = limit;
    }

    /**
     * @return the input {@link Iterable} of {@link Element}s to be validated.
     */
    public Iterable<Element> getElements() {
        return getInput();
    }

    /**
     * @param elements the input {@link Iterable} of {@link Element}s to be validated.
     */
    public void setElements(final Iterable<Element> elements) {
        setInput(elements);
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(final Integer limit) {
        this.limit = limit;
    }

    @JsonIgnore
    @Override
    public Iterable<Element> getInput() {
        return super.getInput();
    }

    /**
     * @return the input {@link List} of {@link Element}s to be validated.
     */
    @JsonProperty(value = "elements")
    List<Element> getElementList() {
        final Iterable<Element> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    /**
     * @param elements the input {@link List} of {@link Element}s to be validated.
     */
    @JsonProperty(value = "elements")
    void setElementList(final List<Element> elements) {
        setInput(elements);
    }

    public static class Builder extends AbstractOperation.Builder<CountGroups, Iterable<Element>, GroupCounts> {

        public Builder() {
            super(new CountGroups());
        }

        /**
         * @param elements the input {@link Iterable} of {@link Element}s to be set on the operation.
         * @return this Builder
         * @see CountGroups#setElements(Iterable)
         */
        public Builder elements(final Iterable<Element> elements) {
            op.setElements(elements);
            return this;
        }

        /**
         * @param limit the limit of group counts to calculate.
         * @return this Builder
         * @see CountGroups#setLimit(Integer)
         */
        public Builder limit(final Integer limit) {
            op.setLimit(limit);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
