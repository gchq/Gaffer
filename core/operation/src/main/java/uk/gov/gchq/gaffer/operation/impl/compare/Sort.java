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
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * A {@code Sort} operation can be used to sort a {@link java.lang.Iterable}
 * of {@link uk.gov.gchq.gaffer.data.element.Element}s using provided
 * {@link java.util.Comparator}s. Either implement your own comparators or use the
 * {@link uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator}.
 * <p>
 * The provided element comparators will be use sequentially to sort the operation
 * input iterable.
 * </p>
 * <p>
 * There is also a resultLimit option that will only keep the top 'X' results.
 * This avoids having to load a large number of Elements into memory, if you
 * only just want the first few results.
 * </p>
 *
 * @see uk.gov.gchq.gaffer.operation.impl.compare.Sort.Builder
 * @see uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator
 */
@JsonPropertyOrder(value = {"class", "input", "comparators"}, alphabetic = true)
@Since("1.0.0")
public class Sort implements
        InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>,
        MultiInput<Element>,
        ElementComparison {

    private Iterable<? extends Element> input;
    @Required
    private List<Comparator<Element>> comparators;
    private Integer resultLimit = null;
    private boolean deduplicate = true;
    private Map<String, String> options;

    @Override
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class", defaultImpl = ElementPropertyComparator.class)
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

    public Integer getResultLimit() {
        return resultLimit;
    }

    public void setResultLimit(final Integer resultLimit) {
        this.resultLimit = resultLimit;
    }

    public boolean isDeduplicate() {
        return deduplicate;
    }

    public void setDeduplicate(final boolean deduplicate) {
        this.deduplicate = deduplicate;
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableElement();
    }

    @Override
    public Sort shallowClone() {
        return new Sort.Builder()
                .input(input)
                .comparators(comparators)
                .resultLimit(resultLimit)
                .deduplicate(deduplicate)
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
            extends BaseBuilder<Sort, Builder>
            implements InputOutput.Builder<Sort, Iterable<? extends Element>, Iterable<? extends Element>, Sort.Builder>,
            MultiInput.Builder<Sort, Element, Builder> {
        public Builder() {
            super(new Sort());
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

        public Builder resultLimit(final Integer resultLimit) {
            _getOp().setResultLimit(resultLimit);
            return this;
        }

        public Builder deduplicate(final boolean deduplicate) {
            _getOp().setDeduplicate(deduplicate);
            return this;
        }
    }
}
