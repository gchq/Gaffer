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
package uk.gov.gchq.gaffer.operation.impl.output;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Map;
import java.util.function.Function;

public class ToMap implements
        Operation,
        InputOutput<Iterable<? extends Element>, Iterable<Map<String, Object>>>,
        MultiInput<Element> {

    private Function<Iterable<? extends Element>, Iterable<Map<String, Object>>> elementGenerator;
    private Iterable<? extends Element> input;

    public ToMap() {
    }

    public ToMap(final Function<Iterable<? extends Element>, Iterable<Map<String, Object>>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Function<Iterable<? extends Element>, Iterable<Map<String, Object>>> getElementGenerator() {
        return elementGenerator;
    }

    void setElementGenerator(final Function<Iterable<? extends Element>, Iterable<Map<String, Object>>> elementGenerator) {
        this.elementGenerator = elementGenerator;
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
    public TypeReference<Iterable<Map<String, Object>>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableMap();
    }

    public static final class Builder extends BaseBuilder<ToMap, Builder>
            implements InputOutput.Builder<ToMap, Iterable<? extends Element>, Iterable<Map<String, Object>>, Builder>,
            MultiInput.Builder<ToMap, Element, Builder> {
        public Builder() {
            super(new ToMap());
        }

        /**
         * @param generator the {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         */
        public ToMap.Builder generator(final Function<Iterable<? extends Element>, Iterable<Map<String, Object>>> generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }
    }
}
