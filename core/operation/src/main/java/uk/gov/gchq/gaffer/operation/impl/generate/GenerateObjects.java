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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.List;

/**
 * An <code>GenerateObjects</code> operation generates an {@link java.lang.Iterable} of objects from an
 * {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s.
 *
 * @param <OBJ> the type of objects in the output iterable.
 * @see uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects.Builder
 */
public class GenerateObjects<ELEMENT_TYPE extends Element, OBJ> extends AbstractOperation<CloseableIterable<ELEMENT_TYPE>, CloseableIterable<OBJ>> {
    private ElementGenerator<OBJ> elementGenerator;

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
    public GenerateObjects(final ElementGenerator<OBJ> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    /**
     * Constructs a <code>GenerateObjects</code> operation with input {@link uk.gov.gchq.gaffer.data.element.Element}s and an
     * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert the elements into objects.
     *
     * @param elements         an {@link CloseableIterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be converted
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s into objects
     */
    public GenerateObjects(final CloseableIterable<ELEMENT_TYPE> elements, final ElementGenerator<OBJ> elementGenerator) {
        super(elements);
        this.elementGenerator = elementGenerator;
    }

    /**
     * Constructs a <code>GenerateObjects</code> operation with input {@link uk.gov.gchq.gaffer.data.element.Element}s and an
     * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert the elements into objects.
     *
     * @param elements         an {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be converted
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s into objects
     */
    public GenerateObjects(final Iterable<ELEMENT_TYPE> elements, final ElementGenerator<OBJ> elementGenerator) {
        this(new WrappedCloseableIterable<>(elements), elementGenerator);
    }

    /**
     * @return an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert
     * {@link uk.gov.gchq.gaffer.data.element.Element}s into objects
     */
    public ElementGenerator<OBJ> getElementGenerator() {
        return elementGenerator;
    }

    /**
     * @return an {@link CloseableIterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be converted
     */
    public CloseableIterable<ELEMENT_TYPE> getElements() {
        return getInput();
    }

    @JsonIgnore
    @Override
    public CloseableIterable<ELEMENT_TYPE> getInput() {
        return super.getInput();
    }

    @JsonProperty
    @Override
    public void setInput(final CloseableIterable<ELEMENT_TYPE> input) {
        super.setInput(input);
    }

    public void setInput(final Iterable<ELEMENT_TYPE> elements) {
        super.setInput(new WrappedCloseableIterable<>(elements));
    }

    /**
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s into objects
     */
    // used for json serialisation
    void setElementGenerator(final ElementGenerator<OBJ> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return a {@link java.util.List} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be converted
     */
    @JsonProperty(value = "elements")
    List<ELEMENT_TYPE> getElementList() {
        final Iterable<ELEMENT_TYPE> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    /**
     * @param elements a {@link java.util.List} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be converted
     */
    @JsonProperty(value = "elements")
    void setElementList(final List<ELEMENT_TYPE> elements) {
        setInput(new WrappedCloseableIterable<>(elements));
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableObj();
    }

    public abstract static class BaseBuilder<ELEMENT_TYPE extends Element, OBJ, CHILD_CLASS extends BaseBuilder<ELEMENT_TYPE, OBJ, ?>>
            extends AbstractOperation.BaseBuilder<GenerateObjects<ELEMENT_TYPE, OBJ>,
            CloseableIterable<ELEMENT_TYPE>,
            CloseableIterable<OBJ>,
            CHILD_CLASS> {
        public BaseBuilder() {
            super(new GenerateObjects<ELEMENT_TYPE, OBJ>());
        }

        /**
         * @param elements the {@link CloseableIterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to set as the input to the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setInput(Object)
         */
        public CHILD_CLASS elements(final CloseableIterable<ELEMENT_TYPE> elements) {
            op.setInput(elements);
            return self();
        }

        /**
         * @param elements the {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to set as the input to the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setInput(Object)
         */
        public CHILD_CLASS elements(final Iterable<ELEMENT_TYPE> elements) {
            op.setInput(new WrappedCloseableIterable<>(elements));
            return self();
        }

        /**
         * @param generator the {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects#setElementGenerator(uk.gov.gchq.gaffer.data.generator.ElementGenerator)
         */
        public CHILD_CLASS generator(final ElementGenerator<OBJ> generator) {
            op.setElementGenerator(generator);
            return self();
        }
    }

    public static final class Builder<ELEMENT_TYPE extends Element, OBJ>
            extends BaseBuilder<ELEMENT_TYPE, OBJ, Builder<ELEMENT_TYPE, OBJ>> {
        @Override
        protected Builder<ELEMENT_TYPE, OBJ> self() {
            return this;
        }
    }
}
