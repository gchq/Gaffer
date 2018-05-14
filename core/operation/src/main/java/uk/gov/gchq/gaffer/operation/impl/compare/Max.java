/*
 * Copyright 2017-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * A {@code Max} operation is intended as a terminal operation for
 * retrieving the "maximum" element from an {@link java.lang.Iterable} of Elements.
 * /**
 * The {@link uk.gov.gchq.gaffer.data.element.Element}s are compared using the provided
 * {@link java.util.Comparator}s. Either implement your own comparators or use the
 * {@link uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator}.
 * <p>
 * The provided element comparators will be use sequentially to compare the operation
 * input iterable to find the maximum.
 * </p>
 *
 * @see uk.gov.gchq.gaffer.operation.impl.compare.Max.Builder
 * @see uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator
 */
@JsonPropertyOrder(value = {"class", "input", "comparators"}, alphabetic = true)
@Since("1.0.0")
@Summary("Extracts the maximum element based on provided Comparators")
public class Max implements
        InputOutput<Iterable<? extends Element>, Element>,
        MultiInput<Element>,
        ElementComparison {

    private Iterable<? extends Element> input;

    @Required
    private List<Comparator<Element>> comparators;
    private Map<String, String> options;

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

    @Override
    public Max shallowClone() {
        return new Max.Builder()
                .input(input)
                .comparators(comparators)
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

    public static final class Builder
            extends Operation.BaseBuilder<Max, Max.Builder>
            implements InputOutput.Builder<Max, Iterable<? extends Element>, Element, Max.Builder>,
            MultiInput.Builder<Max, Element, Max.Builder> {
        public Builder() {
            super(new Max());
        }

        @SafeVarargs
        public final Builder comparators(final Comparator<Element>... comparators) {
            _getOp().setComparators(Lists.newArrayList(comparators));
            return _self();
        }

        public Builder comparators(final List<Comparator<Element>> comparators) {
            _getOp().setComparators(comparators);
            return _self();
        }
    }
}
