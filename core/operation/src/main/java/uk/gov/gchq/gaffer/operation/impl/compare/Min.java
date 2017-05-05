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

public class Min implements
        Operation,
        InputOutput<Iterable<? extends Element>, Element>,
        MultiInput<Element> {

    private Iterable<? extends Element> input;
    private Comparator<Element> elementComparator;
    private Comparator<Object> propertyComparator;
    private String propertyName;

    public Min() {
        // Empty
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Comparator<Object> getPropertyComparator() {
        return propertyComparator;
    }

    public void setPropertyComparator(final Comparator<Object> propertyComparator) {
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
            extends Operation.BaseBuilder<Min, Min.Builder>
            implements InputOutput.Builder<Min, Iterable<? extends Element>, Element, Min.Builder>,
            MultiInput.Builder<Min, Element, Min.Builder> {
        public Builder() {
            super(new Min());
        }

        public Min.Builder elementComparator(final Comparator<Element> comparator) {
            _getOp().setElementComparator(comparator);
            return _self();
        }

        public Min.Builder propertyComparator(final Comparator<Object> comparator) {
            _getOp().setPropertyComparator(comparator);
            return _self();
        }

        public Min.Builder propertyName(final String propertyName) {
            _getOp().setPropertyName(propertyName);
            return _self();
        }
    }
}
