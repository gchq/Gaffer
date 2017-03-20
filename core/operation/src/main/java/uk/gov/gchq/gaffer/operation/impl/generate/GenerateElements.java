/*
 * Copyright 2016 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.IterableInputIterableOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.function.Function;

/**
 * An <code>GenerateElements</code> operation generates an {@link CloseableIterable} of
 * {@link uk.gov.gchq.gaffer.data.element.Element}s from an {@link CloseableIterable} of objects.
 *
 * @param <OBJ> the type of objects in the input iterable.
 * @see uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements.Builder
 */
public class GenerateElements<OBJ> implements
        Operation,
        IterableInputIterableOutput<OBJ, Element> {
    private Function<Iterable<OBJ>, Iterable<Element>> elementGenerator;
    private Iterable<OBJ> input;

    public GenerateElements() {
    }

    /**
     * Constructs a <code>GenerateElements</code> operation with a {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to
     * convert objects into {@link uk.gov.gchq.gaffer.data.element.Element}s. This constructor takes in no input objects and could
     * by used in a operation chain where the objects are provided by the previous operation.
     *
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    public GenerateElements(final Function<Iterable<OBJ>, Iterable<Element>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     * {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Function<Iterable<OBJ>, Iterable<Element>> getElementGenerator() {
        return elementGenerator;
    }

    /**
     * Only used for json serialisation
     *
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    void setElementGenerator(final Function<Iterable<OBJ>, Iterable<Element>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @Override
    public Iterable<OBJ> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<OBJ> input) {
        this.input = input;
    }

    @Override
    public TypeReference<CloseableIterable<Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableElement();
    }

    public static class Builder<OBJ> extends Operation.BaseBuilder<GenerateElements<OBJ>, Builder<OBJ>>
            implements IterableInputIterableOutput.Builder<GenerateElements<OBJ>, OBJ, Element, Builder<OBJ>> {
        public Builder() {
            super(new GenerateElements<>());
        }

        /**
         * @param generator the {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         */
        public Builder<OBJ> generator(final Function<Iterable<OBJ>, Iterable<Element>> generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }
    }
}
