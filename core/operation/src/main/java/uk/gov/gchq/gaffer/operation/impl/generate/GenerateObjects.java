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
 * An <code>GenerateObjects</code> operation generates an {@link java.lang.Iterable} of objects from an
 * {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s.
 *
 * @param <OBJ> the type of objects in the output iterable.
 * @see uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects.Builder
 */
public class GenerateObjects<OBJ> implements
        Operation,
        IterableInputIterableOutput<Element, OBJ> {
    private Function<Iterable<Element>, Iterable<OBJ>> elementGenerator;
    private Iterable<Element> input;

    public GenerateObjects() {
    }

    /**
     * Constructs a <code>GenerateObjects</code> operation with an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to
     * convert {@link uk.gov.gchq.gaffer.data.element.Element}s into objects. This constructor takes in no input
     * {@link uk.gov.gchq.gaffer.data.element.Element}s and could by used in a operation chain where the elements are provided by
     * the previous operation.
     *
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s into objects
     */
    public GenerateObjects(final Function<Iterable<Element>, Iterable<OBJ>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert
     * {@link uk.gov.gchq.gaffer.data.element.Element}s into objects
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Function<Iterable<Element>, Iterable<OBJ>> getElementGenerator() {
        return elementGenerator;
    }

    /**
     * Only used for json serialisation
     *
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s into objects
     */
    void setElementGenerator(final Function<Iterable<Element>, Iterable<OBJ>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @Override
    public Iterable<Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<Element> input) {
        this.input = input;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @Override
    public Object[] createInputArray() {
        return IterableInputIterableOutput.super.createInputArray();
    }

    @Override
    public TypeReference<CloseableIterable<OBJ>> getOutputTypeReference() {
        return TypeReferenceImpl.createCloseableIterableT();
    }

    public static class Builder<OBJ> extends Operation.BaseBuilder<GenerateObjects<OBJ>, Builder<OBJ>>
            implements IterableInputIterableOutput.Builder<GenerateObjects<OBJ>, Element, OBJ, Builder<OBJ>> {
        public Builder() {
            super(new GenerateObjects<>());
        }

        /**
         * @param generator the {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         */
        public Builder<OBJ> generator(final Function<Iterable<Element>, Iterable<OBJ>> generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }
    }
}
