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

package gaffer.operation.impl.add;

import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractValidatable;
import gaffer.operation.VoidOutput;

/**
 * An <code>AddElements</code> operation is a {@link gaffer.operation.Validatable} operation for adding elements.
 * This is a core operation that all stores should be able to handle.
 * This operation requires an {@link Iterable} of {@link gaffer.data.element.Element}s to be added. Handlers should
 * throw an {@link gaffer.operation.OperationException} if unsuccessful.
 * For normal operation handlers the operation {@link gaffer.data.elementdefinition.view.View} will be ignored.
 *
 * @see gaffer.operation.impl.add.AddElements.Builder
 */
public class AddElements extends AbstractValidatable<Void> implements VoidOutput<Iterable<Element>> {
    /**
     * Constructs an <code>AddElements</code> with no {@link gaffer.data.element.Element}s to add. This could be used
     * in an operation chain where the elements are provided by the previous operation.
     */
    public AddElements() {
        super();
    }

    /**
     * Constructs an <code>AddElements</code> with the given {@link java.lang.Iterable} of
     * {@link gaffer.data.element.Element}s to be added.
     *
     * @param elements the {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s to be added.
     */
    public AddElements(final Iterable<Element> elements) {
        super(elements);
    }

    public static class Builder extends AbstractValidatable.Builder<AddElements, Void> {
        public Builder() {
            super(new AddElements());
        }

        @Override
        public Builder elements(final Iterable<Element> elements) {
            super.elements(elements);
            return this;
        }

        @Override
        public Builder skipInvalidElements(final boolean skipInvalidElements) {
            super.skipInvalidElements(skipInvalidElements);
            return this;
        }

        @Override
        public Builder validate(final boolean validate) {
            super.validate(validate);
            return this;
        }

        @Override
        public Builder view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
