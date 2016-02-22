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

package gaffer.data;

import gaffer.data.element.Element;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.data.elementdefinition.view.View;

/**
 * An <code>ValidatedElements</code> extends {@link gaffer.data.TransformIterable} and uses an
 * {@link gaffer.data.ElementValidator} to validate the {@link gaffer.data.element.Element}s.
 * It does not transform the element items - just simply returns them if they are valid.
 * <p>
 * So the resultant {@link java.lang.Iterable} will only contain {@link gaffer.data.element.Element}s that have passed
 * the {@link gaffer.data.elementdefinition.schema.DataSchema} {@link gaffer.function.FilterFunction}s or
 * {@link gaffer.data.elementdefinition.view.View} {@link gaffer.function.FilterFunction}s.
 */
public class ValidatedElements extends TransformIterable<Element, Element> {

    /**
     * Constructs an <code>TransformIterable</code> with the given {@link java.lang.Iterable} of
     * {@link gaffer.data.element.Element}s, a {@link gaffer.data.elementdefinition.schema.DataSchema} containing the
     * {@link gaffer.function.FilterFunction}s to use to validate the {@link gaffer.data.element.Element}s and a
     * skipInvalid flag to determine whether invalid items should be skipped.
     *
     * @param elements    the input {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s
     * @param dataSchema  the {@link gaffer.data.elementdefinition.schema.DataSchema} containing the
     *                    {@link gaffer.function.FilterFunction}s to use to validate the {@link gaffer.data.element.Element}s.
     * @param skipInvalid if true invalid items should be skipped
     */
    public ValidatedElements(final Iterable<Element> elements, final DataSchema dataSchema, final boolean skipInvalid) {
        super(elements, new ElementValidator(dataSchema), skipInvalid);
    }

    /**
     * Constructs an <code>TransformIterable</code> with the given {@link java.lang.Iterable} of
     * {@link gaffer.data.element.Element}s, a {@link gaffer.data.elementdefinition.view.View} containing the
     * {@link gaffer.function.FilterFunction}s to use to validate the {@link gaffer.data.element.Element}s and a
     * skipInvalid flag to determine whether invalid items should be skipped.
     *
     * @param elements    the input {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s
     * @param view        the {@link gaffer.data.elementdefinition.view.View} containing the
     *                    {@link gaffer.function.FilterFunction}s to use to validate the {@link gaffer.data.element.Element}s.
     * @param skipInvalid if true invalid items should be skipped
     */
    public ValidatedElements(final Iterable<Element> elements, final View view, final boolean skipInvalid) {
        super(elements, new ElementValidator(view), skipInvalid);
    }

    @Override
    protected void handleInvalidItem(final Element item) throws IllegalArgumentException {
        final String elementDescription = null != item ? item.toString() : "<unknown>";
        throw new IllegalArgumentException("Element of type " + elementDescription + " is not valid.");

    }

    @Override
    protected Element transform(final Element item) {
        return item;
    }
}
