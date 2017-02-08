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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
 * An <code>GenerateElements</code> operation generates an {@link CloseableIterable} of
 * {@link uk.gov.gchq.gaffer.data.element.Element}s from an {@link CloseableIterable} of objects.
 *
 * @param <OBJ> the type of objects in the input iterable.
 * @see uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements.Builder
 */
public class GenerateElements<OBJ> extends AbstractOperation<CloseableIterable<OBJ>, CloseableIterable<Element>> {
    private ElementGenerator<OBJ> elementGenerator;

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
    public GenerateElements(final ElementGenerator<OBJ> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    /**
     * Constructs a <code>GenerateElements</code> operation with input objects and a
     * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert the objects into {@link uk.gov.gchq.gaffer.data.element.Element}s.
     *
     * @param objects          an {@link java.lang.Iterable} of objects to be converted
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    public GenerateElements(final Iterable<OBJ> objects, final ElementGenerator<OBJ> elementGenerator) {
        this(new WrappedCloseableIterable<>(objects), elementGenerator);
    }

    /**
     * Constructs a <code>GenerateElements</code> operation with input objects and a
     * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert the objects into {@link uk.gov.gchq.gaffer.data.element.Element}s.
     *
     * @param objects          an {@link CloseableIterable} of objects to be converted
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    public GenerateElements(final CloseableIterable<OBJ> objects, final ElementGenerator<OBJ> elementGenerator) {
        super(objects);
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return the input objects to be converted into {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    public CloseableIterable<OBJ> getObjects() {
        return getInput();
    }

    /**
     * @return an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     * {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    public ElementGenerator<OBJ> getElementGenerator() {
        return elementGenerator;
    }

    @JsonIgnore
    @Override
    public CloseableIterable<OBJ> getInput() {
        return super.getInput();
    }

    @JsonProperty
    @Override
    public void setInput(final CloseableIterable<OBJ> elements) {
        super.setInput(elements);
    }

    public void setInput(final Iterable<OBJ> elements) {
        super.setInput(new WrappedCloseableIterable<OBJ>(elements));
    }

    /**
     * @param elementGenerator an {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    // used for json serialisation
    void setElementGenerator(final ElementGenerator<OBJ> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return a {@link java.util.List} of input objects to be converted into {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonProperty(value = "objects")
    List<OBJ> getObjectsList() {
        final Iterable<OBJ> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    /**
     * @param objs a {@link java.util.List} of input objects to be converted into {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    @JsonProperty(value = "objects")
    void setObjectsList(final List<OBJ> objs) {
        setInput(new WrappedCloseableIterable<OBJ>(objs));
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableElement();
    }

    public abstract static class BaseBuilder<OBJ, CHILD_CLASS extends BaseBuilder<OBJ, ?>>
            extends AbstractOperation.BaseBuilder<GenerateElements<OBJ>, CloseableIterable<OBJ>, CloseableIterable<Element>, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GenerateElements<OBJ>());
        }

        /**
         * @param objects the {@link java.lang.Iterable} of objects to set as the input to the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setInput(Object)
         */
        public CHILD_CLASS objects(final Iterable<OBJ> objects) {
            op.setInput(new WrappedCloseableIterable(objects));
            return self();
        }

        /**
         * @param objects the {@link CloseableIterable} of objects to set as the input to the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setInput(Object)
         */
        public CHILD_CLASS objects(final CloseableIterable<OBJ> objects) {
            op.setInput(objects);
            return self();
        }

        /**
         * @param generator the {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements#setElementGenerator(uk.gov.gchq.gaffer.data.generator.ElementGenerator)
         */
        public CHILD_CLASS generator(final ElementGenerator<OBJ> generator) {
            op.setElementGenerator(generator);
            return self();
        }
    }

    public static final class Builder<OBJ> extends BaseBuilder<OBJ, Builder<OBJ>> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
