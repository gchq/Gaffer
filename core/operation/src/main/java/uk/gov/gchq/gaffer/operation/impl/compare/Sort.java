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
package uk.gov.gchq.gaffer.operation.impl.compare;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Comparator;

/**
 * A <code>Sort</code> operation can be used to sort a {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s using a provided {@link java.util.Comparator} object.
 * This operation can be executed in two modes:
 * <ul><li>property comparator - a {@link java.util.Comparator} is provided, along with a property name. The supplied comparator is applied to all values of the specified property, and the input is sorted according to the {@link java.util.Comparator} implementation.</li><li>element comparator - an {@link uk.gov.gchq.gaffer.data.element.Element} {@link java.util.Comparator} is provided, and is applied to all elements in the input {@link java.lang.Iterable}. the input is sorted according to the {@link java.util.Comparator} implementation.</li></ul>
 *
 * @see uk.gov.gchq.gaffer.operation.impl.compare.Sort.Builder
 */
public class Sort implements
        Operation,
        InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>,
        MultiInput<Element> {

    private Iterable<? extends Element> input;
    private Comparator<Element> elementComparator;
    private Comparator propertyComparator;
    private String propertyName;
    private long resultLimit = Long.MAX_VALUE;
    private boolean reversed;
    private boolean includeNulls;

    public Sort() {
        // Empty
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Comparator getPropertyComparator() {
        return propertyComparator;
    }

    public void setPropertyComparator(final Comparator propertyComparator) {
        this.propertyComparator = propertyComparator;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Comparator<Element> getElementComparator() {
        return elementComparator;
    }

    public void setElementComparator(final Comparator<Element> elementComparator) {
        this.elementComparator = elementComparator;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(final String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    public long getResultLimit() {
        return resultLimit;
    }

    public void setResultLimit(final long resultLimit) {
        this.resultLimit = resultLimit;
    }

    public boolean isReversed() {
        return reversed;
    }

    public void setReversed(final boolean reversed) {
        this.reversed = reversed;
    }

    public boolean isIncludeNulls() {
        return includeNulls;
    }

    public void setIncludeNulls(final boolean includeNulls) {
        this.includeNulls = includeNulls;
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableElement();
    }

    public static final class Builder
            extends BaseBuilder<Sort, Sort.Builder>
            implements InputOutput.Builder<Sort, Iterable<? extends Element>, Iterable<? extends Element>, Sort.Builder>,
            MultiInput.Builder<Sort, Element, Sort.Builder> {
        public Builder() {
            super(new Sort());
        }

        public Sort.Builder elementComparator(final Comparator<Element> comparator) {
            _getOp().setElementComparator(comparator);
            return _self();
        }

        public Sort.Builder propertyComparator(final Comparator comparator) {
            _getOp().setPropertyComparator(comparator);
            return _self();
        }

        public Sort.Builder propertyName(final String propertyName) {
            _getOp().setPropertyName(propertyName);
            return _self();
        }

        public Sort.Builder resultLimit(final long resultLimit) {
            _getOp().setResultLimit(resultLimit);
            return _self();
        }

        public Sort.Builder reversed(final boolean reversed) {
            _getOp().setReversed(reversed);
            return _self();
        }

        public Sort.Builder includeNulls(final boolean includeNulls) {
            _getOp().setIncludeNulls(includeNulls);
            return _self();
        }
    }
}
