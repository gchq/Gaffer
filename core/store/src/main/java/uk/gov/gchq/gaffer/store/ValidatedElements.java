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

package uk.gov.gchq.gaffer.store;

import uk.gov.gchq.gaffer.commonutil.iterable.TransformIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;

/**
 * An {@code ValidatedElements} extends {@link TransformIterable} and uses an
 * {@link ElementValidator} to validate the {@link Element}s.
 * It does not transform the element items - just simply returns them if they are valid.
 * <p>
 * So the resultant {@link Iterable} will only contain {@link Element}s that have passed
 * the {@link Schema} {@link java.util.function.Predicate}s or
 * {@link View} {@link java.util.function.Predicate}s.
 */
public class ValidatedElements extends TransformIterable<Element, Element> {
    /**
     * Constructs an {@code TransformIterable} with the given {@link Iterable} of
     * {@link Element}s, a {@link Schema} containing the
     * {@link java.util.function.Predicate}s to use to validate the {@link Element}s and a
     * skipInvalid flag to determine whether invalid items should be skipped.
     *
     * @param elements    the input {@link Iterable} of {@link Element}s
     * @param schema      the {@link Schema} containing the
     *                    {@link java.util.function.Predicate}s to use to validate the {@link Element}s.
     * @param skipInvalid if true invalid items should be skipped
     */
    public ValidatedElements(final Iterable<? extends Element> elements, final Schema schema, final boolean skipInvalid) {
        super((Iterable) elements, new ElementValidator(schema), skipInvalid);
    }

    /**
     * Constructs an {@code TransformIterable} with the given {@link Iterable} of
     * {@link Element}s, a {@link View} containing the
     * {@link java.util.function.Predicate}s to use to validate the {@link Element}s and a
     * skipInvalid flag to determine whether invalid items should be skipped.
     *
     * @param elements    the input {@link Iterable} of {@link Element}s
     * @param view        the {@link View} containing the
     *                    {@link java.util.function.Predicate}s to use to validate the {@link Element}s.
     * @param skipInvalid if true invalid items should be skipped
     */
    public ValidatedElements(final Iterable<? extends Element> elements, final View view, final boolean skipInvalid) {
        super((Iterable) elements, new ElementValidator(view), skipInvalid);
    }

    @Override
    protected void handleInvalidItem(final Element item) {
        final ValidationResult result = getValidator().validateWithValidationResult(item);
        final String elementDescription = null != item ? item.toString() : "<unknown>";
        String validationResultErrors;
        if (result.isValid()) {
            validationResultErrors = "";
        } else {
            validationResultErrors = " \n" + result.getErrorString();
        }
        throw new IllegalArgumentException("Element of type " + elementDescription + " is not valid." + validationResultErrors);

    }

    @Override
    protected Element transform(final Element item) {
        return item;
    }

    @Override
    public void close() {

    }
}
