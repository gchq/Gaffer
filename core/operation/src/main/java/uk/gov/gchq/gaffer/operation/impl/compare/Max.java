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
 * A <code>Max</code> operation is intended as a terminal operation for retrieving the "maximum" element from an {@link java.lang.Iterable}.
 * This operation can be executed in two modes:
 * <ul><li>property comparator - a {@link java.util.Comparator} is provided, along with a property name. The supplied comparator is applied to all values of the specified property, and the element containing the maximum value (as specified by the {@link java.util.Comparator}) is returned.</li><li>element comparator - an {@link uk.gov.gchq.gaffer.data.element.Element} {@link java.util.Comparator} is provided, and is applied to all elements in the input {@link java.lang.Iterable}. The maximum element (as specified by the {@link java.util.Comparator} is returned.</li></ul>
 *
 * @see uk.gov.gchq.gaffer.operation.impl.compare.Max.Builder
 */
public class Max implements
        Operation,
        InputOutput<Iterable<? extends Element>, Element>,
        MultiInput<Element> {

    private Iterable<? extends Element> input;
    private Comparator<Element> elementComparator;
    private Comparator propertyComparator;
    private String propertyName;

    public Max() {
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

    @Override
    public TypeReference<Element> getOutputTypeReference() {
        return new TypeReferenceImpl.Element();
    }

    public static final class Builder
            extends Operation.BaseBuilder<Max, Max.Builder>
            implements InputOutput.Builder<Max, Iterable<? extends Element>, Element, Max.Builder>,
            MultiInput.Builder<Max, Element, Max.Builder> {
        public Builder() {
            super(new Max());
        }

        public Max.Builder elementComparator(final Comparator<Element> comparator) {
            _getOp().setElementComparator(comparator);
            return _self();
        }

        public Max.Builder propertyComparator(final Comparator comparator) {
            _getOp().setPropertyComparator(comparator);
            return _self();
        }

        public Max.Builder propertyName(final String propertyName) {
            _getOp().setPropertyName(propertyName);
            return _self();
        }
    }
}
