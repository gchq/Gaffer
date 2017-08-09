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
import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Comparator;
import java.util.List;

/**
 * A <code>Min</code> operation is intended as a terminal operation for
 * retrieving the "minimum" element from an {@link java.lang.Iterable} of Elements.
 * /**
 * The {@link uk.gov.gchq.gaffer.data.element.Element}s are compared using the provided
 * {@link java.util.Comparator}s. Either implement your own comparators or use the
 * {@link uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator}.
 * <p>
 * The provided element comparators will be use sequentially to compare the operation
 * input iterable to find the minimum.
 * </p>
 *
 * @see uk.gov.gchq.gaffer.operation.impl.compare.Min.Builder
 * @see uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator
 */
public class Min implements
        Operation,
        InputOutput<Iterable<? extends Element>, Element>,
        MultiInput<Element>,
        ElementComparison {

    private Iterable<? extends Element> input;
    @Required
    private List<Comparator<Element>> comparators;

    @Override
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public List<Comparator<Element>> getComparators() {
        return comparators;
    }

    public void setComparators(final List<Comparator<Element>> comparators) {
        this.comparators = comparators;
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

        @SafeVarargs
        public final Min.Builder comparators(final Comparator<Element>... comparators) {
            _getOp().setComparators(Lists.newArrayList(comparators));
            return _self();
        }

        public Builder comparators(final List<Comparator<Element>> comparators) {
            _getOp().setComparators(comparators);
            return _self();
        }
    }
}
