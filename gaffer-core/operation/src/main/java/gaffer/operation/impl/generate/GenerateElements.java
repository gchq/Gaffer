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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.generator.ElementGenerator;
import gaffer.operation.AbstractOperation;

import java.util.List;

/**
 * An <code>GenerateElements</code> operation generates an {@link java.lang.Iterable} of
 * {@link gaffer.data.element.Element}s from an {@link java.lang.Iterable} of objects.
 *
 * @param <OBJ> the type of objects in the input iterable.
 * @see gaffer.operation.impl.generate.GenerateElements.Builder
 */
public class GenerateElements<OBJ> extends AbstractOperation<Iterable<OBJ>, Iterable<Element>> {
    private ElementGenerator<OBJ> elementGenerator;

    public GenerateElements() {
    }

    /**
     * Constructs a <code>GenerateElements</code> operation with a {@link gaffer.data.generator.ElementGenerator} to
     * convert objects into {@link gaffer.data.element.Element}s. This constructor takes in no input objects and could
     * by used in a operation chain where the objects are provided by the previous operation.
     *
     * @param elementGenerator an {@link gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link gaffer.data.element.Element}s
     */
    public GenerateElements(final ElementGenerator<OBJ> elementGenerator) {
        super();
        this.elementGenerator = elementGenerator;
    }

    /**
     * Constructs a <code>GenerateElements</code> operation with input objects and a
     * {@link gaffer.data.generator.ElementGenerator} to convert the objects into {@link gaffer.data.element.Element}s.
     *
     * @param objects          an {@link java.lang.Iterable} of objects to be converted
     * @param elementGenerator an {@link gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link gaffer.data.element.Element}s
     */
    public GenerateElements(final Iterable<OBJ> objects, final ElementGenerator<OBJ> elementGenerator) {
        super(objects);
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return the input objects to be converted into {@link gaffer.data.element.Element}s
     */
    public Iterable<OBJ> getObjects() {
        return getInput();
    }

    /**
     * @return an {@link gaffer.data.generator.ElementGenerator} to convert objects into
     * {@link gaffer.data.element.Element}s
     */
    public ElementGenerator<OBJ> getElementGenerator() {
        return elementGenerator;
    }

    @JsonIgnore
    @Override
    public Iterable<OBJ> getInput() {
        return super.getInput();
    }

    /**
     * @param elementGenerator an {@link gaffer.data.generator.ElementGenerator} to convert objects into
     *                         {@link gaffer.data.element.Element}s
     */
    // used for json serialisation
    void setElementGenerator(final ElementGenerator<OBJ> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    /**
     * @return a {@link java.util.List} of input objects to be converted into {@link gaffer.data.element.Element}s
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonProperty(value = "objects")
    List<OBJ> getObjectsList() {
        final Iterable<OBJ> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    /**
     * @param objs a {@link java.util.List} of input objects to be converted into {@link gaffer.data.element.Element}s
     */
    @JsonProperty(value = "objects")
    void setObjectsList(final List<OBJ> objs) {
        setInput(objs);
    }

    public static class Builder<OBJ> extends AbstractOperation.Builder<GenerateElements<OBJ>, Iterable<OBJ>, Iterable<Element>> {
        public Builder() {
            super(new GenerateElements<OBJ>());
        }

        /**
         * @param objects the {@link java.lang.Iterable} of objects to set as the input to the operation
         * @return this Builder
         * @see gaffer.operation.Operation#setInput(Object)
         */
        public Builder<OBJ> objects(final Iterable<OBJ> objects) {
            op.setInput(objects);
            return this;
        }

        /**
         * @param generator the {@link gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         * @see gaffer.operation.impl.generate.GenerateElements#setElementGenerator(gaffer.data.generator.ElementGenerator)
         */
        public Builder<OBJ> generator(final ElementGenerator<OBJ> generator) {
            op.setElementGenerator(generator);
            return this;
        }

        @Override
        public Builder<OBJ> view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public Builder<OBJ> option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
