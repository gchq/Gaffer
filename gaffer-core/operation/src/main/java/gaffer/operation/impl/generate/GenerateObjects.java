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

package gaffer.operation.impl.generate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.generator.ElementGenerator;
import gaffer.operation.AbstractOperation;

import java.util.List;

/**
 * An <code>GenerateObjects</code> operation generates an {@link java.lang.Iterable} of objects from an
 * {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s.
 *
 * @param <OBJ> the type of objects in the output iterable.
 * @see gaffer.operation.impl.generate.GenerateObjects.Builder
 */
public class GenerateObjects<ELEMENT_TYPE extends Element, OBJ> extends AbstractOperation<Iterable<ELEMENT_TYPE>, Iterable<OBJ>> {
    private ElementGenerator<OBJ> elementGenerator;

    public GenerateObjects() {
        super();
    }

    /**
     * Constructs a <code>GenerateObjects</code> operation with an {@link gaffer.data.generator.ElementGenerator} to
     * convert {@link gaffer.data.element.Element}s into objects. This constructor takes in no input
     * {@link gaffer.data.element.Element}s and could by used in a operation chain where the elements are provided by
     * the previous operation.
     *
     * @param elementGenerator an {@link gaffer.data.generator.ElementGenerator} to convert
     *                         {@link gaffer.data.element.Element}s into objects
     */
    public GenerateObjects(final ElementGenerator<OBJ> elementGenerator) {
        super();
        this.elementGenerator = elementGenerator;
    }

    /**
     * Constructs a <code>GenerateObjects</code> operation with input {@link gaffer.data.element.Element}s and an
     * {@link gaffer.data.generator.ElementGenerator} to convert the elements into objects.
     *
     * @param elements         an {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s to be converted
     * @param elementGenerator an {@link gaffer.data.generator.ElementGenerator} to convert
     *                         {@link gaffer.data.element.Element}s into objects
     */
    public GenerateObjects(final Iterable<ELEMENT_TYPE> elements, final ElementGenerator<OBJ> elementGenerator) {
        super(elements);
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return an {@link gaffer.data.generator.ElementGenerator} to convert
     * {@link gaffer.data.element.Element}s into objects
     */
    public ElementGenerator<OBJ> getElementGenerator() {
        return elementGenerator;
    }

    /**
     * @return an {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s to be converted
     */
    public Iterable<ELEMENT_TYPE> getElements() {
        return getInput();
    }

    @JsonIgnore
    @Override
    public Iterable<ELEMENT_TYPE> getInput() {
        return super.getInput();
    }

    /**
     * @param elementGenerator an {@link gaffer.data.generator.ElementGenerator} to convert
     *                         {@link gaffer.data.element.Element}s into objects
     */
    // used for json serialisation
    void setElementGenerator(final ElementGenerator<OBJ> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return a {@link java.util.List} of {@link gaffer.data.element.Element}s to be converted
     */
    @JsonProperty(value = "elements")
    List<ELEMENT_TYPE> getElementList() {
        final Iterable<ELEMENT_TYPE> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    /**
     * @param elements a {@link java.util.List} of {@link gaffer.data.element.Element}s to be converted
     */
    @JsonProperty(value = "elements")
    void setElementList(final List<ELEMENT_TYPE> elements) {
        setInput(elements);
    }

    public static class Builder<ELEMENT_TYPE extends Element, OBJ>
            extends AbstractOperation.Builder<GenerateObjects<ELEMENT_TYPE, OBJ>, Iterable<ELEMENT_TYPE>, Iterable<OBJ>> {
        public Builder() {
            super(new GenerateObjects<ELEMENT_TYPE, OBJ>());
        }

        /**
         * @param elements the {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s to set as the input to the operation
         * @return this Builder
         * @see gaffer.operation.Operation#setInput(Object)
         */
        public Builder<ELEMENT_TYPE, OBJ> elements(final Iterable<ELEMENT_TYPE> elements) {
            op.setInput(elements);
            return this;
        }

        /**
         * @param generator the {@link gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         * @see gaffer.operation.impl.generate.GenerateObjects#setElementGenerator(gaffer.data.generator.ElementGenerator)
         */
        public Builder<ELEMENT_TYPE, OBJ> generator(final ElementGenerator<OBJ> generator) {
            op.setElementGenerator(generator);
            return this;
        }

        @Override
        public Builder<ELEMENT_TYPE, OBJ> view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public Builder<ELEMENT_TYPE, OBJ> option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
