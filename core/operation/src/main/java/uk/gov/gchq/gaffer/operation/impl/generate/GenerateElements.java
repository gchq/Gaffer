/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.generate;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;
import java.util.function.Function;

/**
 * An {@code GenerateElements} operation generates an {@link Iterable} of
 * {@link uk.gov.gchq.gaffer.data.element.Element}s from an {@link Iterable} of objects.
 *
 * @param <OBJ> the type of objects in the input iterable.
 * @see uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements.Builder
 */
@JsonPropertyOrder(value = {"class", "input", "elementGenerator"}, alphabetic = true)
@Since("1.0.0")
@Summary("Generates elements from objects using provided generators")
public class GenerateElements<OBJ> implements
        InputOutput<Iterable<? extends OBJ>, Iterable<? extends Element>>,
        MultiInput<OBJ> {
    @Required
    private Function<Iterable<? extends OBJ>, Iterable<? extends Element>> elementGenerator;
    private Iterable<? extends OBJ> input;
    private Map<String, String> options;

    public GenerateElements() {
    }

    /**
     * Constructs a {@code GenerateElements} operation with a {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to
     * convert objects into {@link uk.gov.gchq.gaffer.data.element.Element}s. This constructor takes in no input objects and could
     * by used in a operation chain where the objects are provided by the previous operation.
     *
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    public GenerateElements(final Function<Iterable<? extends OBJ>, Iterable<? extends Element>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     * {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Function<Iterable<? extends OBJ>, Iterable<? extends Element>> getElementGenerator() {
        return elementGenerator;
    }

    /**
     * Only used for json serialisation
     *
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    void setElementGenerator(final Function<Iterable<? extends OBJ>, Iterable<? extends Element>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @Override
    public Iterable<? extends OBJ> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends OBJ> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableElement();
    }

    @Override
    public GenerateElements<OBJ> shallowClone() {
        return new GenerateElements.Builder<OBJ>()
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

    public static class Builder<OBJ> extends Operation.BaseBuilder<GenerateElements<OBJ>, Builder<OBJ>>
            implements InputOutput.Builder<GenerateElements<OBJ>, Iterable<? extends OBJ>, Iterable<? extends Element>, Builder<OBJ>>,
            MultiInput.Builder<GenerateElements<OBJ>, OBJ, Builder<OBJ>> {
        public Builder() {
            super(new GenerateElements<>());
        }

        /**
         * @param generator the {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         */
        public Builder<OBJ> generator(final Function<Iterable<? extends OBJ>, Iterable<? extends Element>> generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }
    }
}
