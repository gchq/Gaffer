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
        MultiInput<Element>,
        ElementComparison {

    private Iterable<? extends Element> input;
    private Comparator<Element> comparator;
    private long resultLimit = Long.MAX_VALUE;
    private boolean reversed;

    @Override
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Comparator<Element> getComparator() {
        return comparator;
    }

    public void setComparator(final Comparator<Element> comparator) {
        this.comparator = comparator;
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

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableElement();
    }

    public static final class Builder
            extends BaseBuilder<Sort, Builder>
            implements InputOutput.Builder<Sort, Iterable<? extends Element>, Iterable<? extends Element>, Sort.Builder>,
            MultiInput.Builder<Sort, Element, Builder> {
        public Builder() {
            super(new Sort());
        }

        public Builder comparator(final Comparator<Element> comparator) {
            _getOp().setComparator(comparator);
            return _self();
        }

        public Builder resultLimit(final long resultLimit) {
            _getOp().setResultLimit(resultLimit);
            return this;
        }

        public Builder reverse(final boolean reverse) {
            _getOp().setReversed(reverse);
            return this;
        }
    }
}
