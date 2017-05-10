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
import com.google.common.collect.Sets;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.comparison.ElementComparator;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.ElementComparison;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Set;

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
        MultiInput<Element>,
        ElementComparison {

    private Iterable<? extends Element> input;
    private ElementComparator comparator;

    public Max() {
        // Empty
    }

    @Override
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public ElementComparator getComparator() {
        return comparator;
    }

    public void setComparator(final ElementComparator comparator) {
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

    @Override
    public TypeReference<Element> getOutputTypeReference() {
        return new TypeReferenceImpl.Element();
    }

    @Override
    public Set<Pair<String, String>> getComparablePair() {
        return (comparator instanceof ElementPropertyComparator)
                ? _getComparablePair((ElementPropertyComparator) comparator)
                : Sets.newHashSet();
    }

    public static final class Builder
            extends Operation.BaseBuilder<Max, Max.Builder>
            implements InputOutput.Builder<Max, Iterable<? extends Element>, Element, Max.Builder>,
            MultiInput.Builder<Max, Element, Max.Builder> {
        public Builder() {
            super(new Max());
        }


        public Max.Builder comparator(final ElementComparator comparator) {
            _getOp().setComparator(comparator);
            return _self();
        }
    }
}
