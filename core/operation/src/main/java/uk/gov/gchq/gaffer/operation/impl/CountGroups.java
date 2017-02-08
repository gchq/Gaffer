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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.List;

/**
 * A <code>CountGroups</code> operation takes in {@link Element}s and collects
 * counts for the number of entity and edge groups used. To avoid counting all
 * elements in the store, this operation has a limit, which can be set to
 * skip counting the remaining groups.
 *
 * @see CountGroups.Builder
 */
public class CountGroups extends AbstractOperation<CloseableIterable<Element>, GroupCounts> {
    private Integer limit;

    public CountGroups() {
        this(null);
    }

    public CountGroups(final Integer limit) {
        this.limit = limit;
    }

    /**
     * @return the input {@link CloseableIterable} of {@link Element}s to be validated.
     */
    public CloseableIterable<Element> getElements() {
        return getInput();
    }

    /**
     * @param elements the input {@link Iterable} of {@link Element}s to be validated.
     */
    public void setElements(final Iterable<Element> elements) {
        setElements(new WrappedCloseableIterable<Element>(elements));
    }

    /**
     * @param elements the input {@link CloseableIterable} of {@link Element}s to be validated.
     */
    public void setElements(final CloseableIterable<Element> elements) {
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
    public CloseableIterable<Element> getInput() {
        return super.getInput();
    }

    @JsonProperty
    @Override
    public void setInput(final CloseableIterable<Element> input) {
        super.setInput(input);
    }

    /**
     * @return the input {@link List} of {@link Element}s to be validated.
     */
    @JsonProperty(value = "elements")
    List<Element> getElementList() {
        final CloseableIterable<Element> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    /**
     * @param elements the input {@link List} of {@link Element}s to be validated.
     */
    @JsonProperty(value = "elements")
    void setElementList(final List<Element> elements) {
        setInput(new WrappedCloseableIterable<>(elements));
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.CountGroups();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractOperation.BaseBuilder<CountGroups, CloseableIterable<Element>, GroupCounts, CHILD_CLASS> {

        public BaseBuilder() {
            super(new CountGroups());
        }

        /**
         * @param elements the input {@link Iterable} of {@link Element}s to be set on the operation.
         * @return this Builder
         * @see CountGroups#setElements(Iterable)
         */
        public CHILD_CLASS elements(final Iterable<Element> elements) {
            op.setElements(elements);
            return self();
        }

        /**
         * @param elements the input {@link CloseableIterable} of {@link Element}s to be set on the operation.
         * @return this Builder
         * @see CountGroups#setElements(CloseableIterable)
         */
        public CHILD_CLASS elements(final CloseableIterable<Element> elements) {
            op.setElements(elements);
            return self();
        }

        /**
         * @param limit the limit of group counts to calculate.
         * @return this Builder
         * @see CountGroups#setLimit(Integer)
         */
        public CHILD_CLASS limit(final Integer limit) {
            op.setLimit(limit);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
