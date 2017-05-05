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
package uk.gov.gchq.gaffer.mapstore.operation;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

public class CountAllElementsDefaultView implements
        Operation,
        InputOutput<Iterable<? extends Element>, Long>,
        MultiInput<Element> {
    private Iterable<? extends Element> input;

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Long> getOutputTypeReference() {
        return new TypeReferenceImpl.Long();
    }

    public static final class Builder
            extends Operation.BaseBuilder<CountAllElementsDefaultView, Builder>
            implements InputOutput.Builder<CountAllElementsDefaultView, Iterable<? extends Element>, Long, Builder>,
            MultiInput.Builder<CountAllElementsDefaultView, Element, Builder> {
        public Builder() {
            super(new CountAllElementsDefaultView());
        }
    }
}
