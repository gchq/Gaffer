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
package uk.gov.gchq.gaffer.operation.impl.function;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operation.Builder;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl.IterableElement;
import java.util.Map;

/**
 * A <code>Filter</code> operation applies a provided {@link ElementFilter} to the provided {@link Iterable} of {@link Element}s,
 * and returns an {@link Iterable}.
 */
public class Filter implements
        Operation,
        InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>,
        MultiInput<Element> {

    private Iterable<? extends Element> input;
    private ElementFilter elementFilter;
    private Map<String, String> options;

    @Override
    public Filter shallowClone() throws CloneFailedException {
        return new Filter.Builder()
                .input(input)
                .elementFilter(elementFilter)
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

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new IterableElement();
    }

    public ElementFilter getElementFilter() {
        return elementFilter;
    }

    public void setElementFilter(final ElementFilter elementFilter) {
        this.elementFilter = elementFilter;
    }

    public static final class Builder
    extends Operation.BaseBuilder<Filter, Builder>
    implements InputOutput.Builder<Filter, Iterable<? extends Element>, Iterable<? extends Element>, Builder>,
            MultiInput.Builder<Filter, Element, Builder> {
        public Builder() { super(new Filter()); }

        public Builder elementFilter(final ElementFilter elementFilter) {
            _getOp().setElementFilter(elementFilter);
            return _self();
        }
    }
}
