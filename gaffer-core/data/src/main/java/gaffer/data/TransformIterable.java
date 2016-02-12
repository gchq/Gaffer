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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A <code>TransformIterable</code> allows {@link java.lang.Iterable}s to be lazily validated and transformed without
 * loading the entire iterable into memory. The easiest way to use this class is to create an anonymous inner class.
 *
 * @param <INPUT>  The input iterable type.
 * @param <OUTPUT> the output iterable type.
 */
public abstract class TransformIterable<INPUT, OUTPUT> implements Iterable<OUTPUT> {
    private final Iterable<INPUT> input;
    private final Validator<INPUT> validator;
    private final boolean skipInvalid;

    /**
     * Constructs an <code>TransformIterable</code> with the given input {@link java.lang.Iterable} and no validation.
     *
     * @param input the input {@link java.lang.Iterable}
     */
    public TransformIterable(final Iterable<INPUT> input) {
        this(input, new AlwaysValid<INPUT>(), false);
    }

    /**
     * Constructs an <code>TransformIterable</code> with the given input {@link java.lang.Iterable} and
     * {@link gaffer.data.Validator}. Invalid items will throw an {@link java.lang.IllegalArgumentException} to be thrown.
     *
     * @param input     the input {@link java.lang.Iterable}
     * @param validator the {@link gaffer.data.Validator}
     */
    public TransformIterable(final Iterable<INPUT> input, final Validator<INPUT> validator) {
        this(input, validator, false);
    }

    /**
     * Constructs an <code>TransformIterable</code> with the given input {@link java.lang.Iterable},
     * {@link gaffer.data.Validator} and a skipInvalid flag to determine whether invalid items should be skipped.
     *
     * @param input       the input {@link java.lang.Iterable}
     * @param validator   the {@link gaffer.data.Validator}
     * @param skipInvalid if true invalid items should be skipped
     */
    public TransformIterable(final Iterable<INPUT> input, final Validator<INPUT> validator, final boolean skipInvalid) {
        this.input = input;
        this.validator = validator;
        this.skipInvalid = skipInvalid;
    }

    /**
     * @return an {@link java.util.Iterator} that lazy transforms the INPUT items to OUTPUT items
     */
    public Iterator<OUTPUT> iterator() {
        return new Iterator<OUTPUT>() {
            private final Iterator<INPUT> inputItr = input.iterator();

            private OUTPUT nextElement;
            private Boolean hasNext;

            @Override
            public boolean hasNext() {
                if (null == hasNext) {
                    if (inputItr.hasNext()) {
                        final INPUT possibleNext = inputItr.next();
                        if (validator.validate(possibleNext)) {
                            nextElement = transform(possibleNext);
                            hasNext = true;
                        } else if (skipInvalid) {
                            hasNext();
                        } else {
                            handleInvalidItem(possibleNext);
                        }
                    } else {
                        hasNext = false;
                        nextElement = null;
                    }
                }

                return Boolean.TRUE.equals(hasNext);
            }

            @Override
            public OUTPUT next() {
                if (null == hasNext) {
                    if (!hasNext()) {
                        throw new NoSuchElementException("Reached the end of the iterator");
                    }
                }

                final OUTPUT elementToReturn = nextElement;
                nextElement = null;
                hasNext = null;

                return elementToReturn;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Cannot call remove on a " + getIterableClass().getSimpleName() + " iterator");
            }
        };
    }

    /**
     * Transforms the INPUT item into an OUTPUT item
     *
     * @param item the INPUT item to be transformed
     * @return the transformed OUTPUT item
     */
    protected abstract OUTPUT transform(final INPUT item);

    /**
     * Handles an invalid item. Simply throws an {@link java.lang.IllegalArgumentException} explaining that the item is
     * invalid. Override this method to handle invalid items differently.
     *
     * @param item the invalid INPUT item
     * @throws IllegalArgumentException always thrown unless this method is overridden.
     */
    protected void handleInvalidItem(final INPUT item) throws IllegalArgumentException {
        final String itemDescription = null != item ? item.toString() : "<unknown>";
        throw new IllegalArgumentException("Next " + itemDescription + " in iterable is not valid.");
    }

    private Class<? extends TransformIterable> getIterableClass() {
        return getClass();
    }
}
