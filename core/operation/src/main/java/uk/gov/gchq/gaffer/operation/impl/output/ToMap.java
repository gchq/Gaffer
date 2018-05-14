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
package uk.gov.gchq.gaffer.operation.impl.output;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.MapGenerator;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code ToMap} operation takes in an {@link java.lang.Iterable} of items
 * and uses a {@link uk.gov.gchq.gaffer.data.generator.MapGenerator} to convert
 * each item into a {@link java.util.Map} of key-value pairs.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.output.ToMap.Builder
 */
@JsonPropertyOrder(value = {"class", "input", "elementGenerator"}, alphabetic = true)
@Since("1.0.0")
@Summary("Converts elements to a Map of key-value pairs")
public class ToMap implements
        InputOutput<Iterable<? extends Element>, Iterable<? extends Map<String, Object>>>,
        MultiInput<Element> {

    @Required
    private MapGenerator elementGenerator;
    private Iterable<? extends Element> input;
    private Map<String, String> options;

    public ToMap() {
    }

    public ToMap(final MapGenerator elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public MapGenerator getElementGenerator() {
        return elementGenerator;
    }

    void setElementGenerator(final MapGenerator elementGenerator) {
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
    public TypeReference<Iterable<? extends Map<String, Object>>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableMap();
    }

    @Override
    public ToMap shallowClone() {
        return new ToMap.Builder()
                .generator(elementGenerator)
                .input(input)
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

    public static final class Builder extends BaseBuilder<ToMap, Builder>
            implements InputOutput.Builder<ToMap, Iterable<? extends Element>, Iterable<? extends Map<String, Object>>, Builder>,
            MultiInput.Builder<ToMap, Element, Builder> {
        public Builder() {
            super(new ToMap());
        }

        /**
         * @param generator the {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         */
        public ToMap.Builder generator(final MapGenerator generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }
    }
}
