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

package uk.gov.gchq.gaffer.operation.impl.add;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.AbstractValidatable;
import uk.gov.gchq.gaffer.operation.VoidOutput;

/**
 * An <code>AddElements</code> operation is a {@link gaffer.operation.Validatable} operation for adding elements.
 * This is a core operation that all stores should be able to handle.
 * This operation requires an {@link Iterable} of {@link gaffer.data.element.Element}s to be added. Handlers should
 * throw an {@link gaffer.operation.OperationException} if unsuccessful.
 * For normal operation handlers the operation {@link gaffer.data.elementdefinition.view.View} will be ignored.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.add.AddElements.Builder
 */
public class AddElements extends AbstractValidatable<Void> implements VoidOutput<CloseableIterable<Element>> {
    /**
     * Constructs an <code>AddElements</code> with no {@link gaffer.data.element.Element}s to add. This could be used
     * in an operation chain where the elements are provided by the previous operation.
     */
    public AddElements() {
        super();
    }

    /**
     * Constructs an <code>AddElements</code> with the given {@link CloseableIterable} of
     * {@link gaffer.data.element.Element}s to be added.
     *
     * @param elements the {@link CloseableIterable} of {@link gaffer.data.element.Element}s to be added.
     */
    public AddElements(final CloseableIterable<Element> elements) {
        super(elements);
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

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractValidatable.BaseBuilder<AddElements, Void, CHILD_CLASS> {
        public BaseBuilder() {
            super(new AddElements());
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
