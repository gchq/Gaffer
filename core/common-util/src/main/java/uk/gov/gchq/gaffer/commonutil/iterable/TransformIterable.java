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

package uk.gov.gchq.gaffer.commonutil.iterable;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A {@code TransformIterable} allows {@link java.lang.Iterable}s to be lazily validated and transformed without
 * loading the entire iterable into memory. The easiest way to use this class is to create an anonymous inner class.
 *
 * @param <I> The input iterable type.
 * @param <O> the output iterable type.
 */
public abstract class TransformIterable<I, O> implements CloseableIterable<O> {
    private final Iterable<? extends I> input;
    private final Validator<I> validator;
    private final boolean skipInvalid;
    private final boolean autoClose;

    /**
     * Constructs an {@code TransformIterable} with the given input {@link java.lang.Iterable} and no validation.
     *
     * @param input the input {@link java.lang.Iterable}
     */
    public TransformIterable(final Iterable<? extends I> input) {
        this(input, new AlwaysValid<>(), false);
    }

    /**
     * Constructs an {@code TransformIterable} with the given input {@link java.lang.Iterable} and
     * {@link Validator}. Invalid items will throw an {@link java.lang.IllegalArgumentException} to be thrown.
     *
     * @param input     the input {@link java.lang.Iterable}
     * @param validator the {@link Validator}
     */
    public TransformIterable(final Iterable<? extends I> input, final Validator<I> validator) {
        this(input, validator, false);
    }

    /**
     * Constructs an {@code TransformIterable} with the given input {@link java.lang.Iterable},
     * {@link Validator} and a skipInvalid flag to determine whether invalid items should be skipped.
     *
     * @param input       the input {@link java.lang.Iterable}
     * @param validator   the {@link Validator}
     * @param skipInvalid if true invalid items should be skipped
     */
    public TransformIterable(final Iterable<? extends I> input, final Validator<I> validator, final boolean skipInvalid) {
        this(input, validator, skipInvalid, true);
    }

    /**
     * Constructs an {@code TransformIterable} with the given parameters
     *
     * @param input       the input {@link java.lang.Iterable}
     * @param validator   the {@link Validator}
     * @param skipInvalid if true invalid items should be skipped
     * @param autoClose   if true then the input iterable will be closed when any iterators reach the end.
     */
    public TransformIterable(final Iterable<? extends I> input, final Validator<I> validator, final boolean skipInvalid, final boolean autoClose) {
        if (null == input) {
            throw new IllegalArgumentException("Input iterable is required");
        }
        this.input = input;
        this.validator = validator;
        this.skipInvalid = skipInvalid;
        this.autoClose = autoClose;
    }


    /**
     * @return an {@link java.util.Iterator} that lazy transforms the I items to O items
     */
    @Override
    public CloseableIterator<O> iterator() {
        return new CloseableIterator<O>() {
            @Override
            public void close() {
                CloseableUtil.close(inputItr);
            }

            private final Iterator<? extends I> inputItr = input.iterator();

            private O nextElement;
            private Boolean hasNext;

            @Override
            public boolean hasNext() {
                if (null == hasNext) {
                    while (inputItr.hasNext()) {
                        final I possibleNext = inputItr.next();
                        if (validator.validate(possibleNext)) {
                            nextElement = transform(possibleNext);
                            hasNext = true;
                            return Boolean.TRUE.equals(hasNext);
                        } else if (skipInvalid) {
                            continue;
                        } else {
                            handleInvalidItem(possibleNext);
                        }
                    }
                    hasNext = false;
                    nextElement = null;
                }

                final boolean hasNextResult = Boolean.TRUE.equals(hasNext);
                if (autoClose && !hasNextResult) {
                    close();
                }

                return hasNextResult;
            }

            @Override
            public O next() {
                if ((null == hasNext) && (!hasNext())) {
                    throw new NoSuchElementException("Reached the end of the iterator");
                }

                final O elementToReturn = nextElement;
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

    @Override
    public void close() {
        CloseableUtil.close(input);
    }

    /**
     * Transforms the I item into an O item
     *
     * @param item the I item to be transformed
     * @return the transformed O item
     */
    protected abstract O transform(final I item);

    /**
     * Handles an invalid item. Simply throws an {@link java.lang.IllegalArgumentException} explaining that the item is
     * invalid. Override this method to handle invalid items differently.
     *
     * @param item the invalid I item
     * @throws IllegalArgumentException always thrown unless this method is overridden.
     */
    protected void handleInvalidItem(final I item) {
        final String itemDescription = null != item ? item.toString() : "<unknown>";
        throw new IllegalArgumentException("Next " + itemDescription + " in iterable is not valid.");
    }

    protected Iterable<? extends I> getInput() {
        return this.input;
    }

    protected Validator<I> getValidator() {
        return validator;
    }

    private Class<? extends TransformIterable> getIterableClass() {
        return getClass();
    }
}
