/*
 * Copyright 2016-2022 Crown Copyright
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

import org.apache.commons.lang3.ArrayUtils;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static java.util.Objects.isNull;

/**
 * A {@code ChainedIterable} is a {@link java.io.Closeable}
 * {@link java.lang.Iterable} composed of other {@link java.lang.Iterable}s.
 *
 * As a client iterates through this iterable, the child iterables are consumed
 * sequentially.
 *
 * @param <T> the type of items in the iterable.
 */
@Deprecated
public class ChainedIterable<T> implements Closeable, Iterable<T> {

    private final Iterable<? extends Iterable<? extends T>> iterables;

    public ChainedIterable(final Iterable<? extends T>... iterables) {
        this(ArrayUtils.isEmpty(iterables) ? null : Arrays.asList(iterables));
    }

    public ChainedIterable(final Iterable<? extends Iterable<? extends T>> iterables) {
        if (isNull(iterables)) {
            throw new IllegalArgumentException("iterables are required");
        }
        this.iterables = iterables;
    }

    @Override
    public Iterator<T> iterator() {
        return new ChainedIterator<>(iterables.iterator());
    }

    @Override
    public void close() {
        for (final Iterable<? extends T> iterable : iterables) {
            CloseableUtil.close(iterable);
        }
    }

    private static class ChainedIterator<T> implements Closeable, Iterator<T> {

        private final Iterator<? extends Iterable<? extends T>> iterablesIterator;

        private Iterator<? extends T> currentIterator = Collections.emptyIterator();

        ChainedIterator(final Iterator<? extends Iterable<? extends T>> iterablesIterator) {
            this.iterablesIterator = iterablesIterator;
        }

        @Override
        public boolean hasNext() {
            return getIterator().hasNext();
        }

        @Override
        public T next() {
            return getIterator().next();
        }

        @Override
        public void remove() {
            currentIterator.remove();
        }

        @Override
        public void close() {
            CloseableUtil.close(currentIterator);
            while (iterablesIterator.hasNext()) {
                CloseableUtil.close(iterablesIterator.next());
            }
        }

        private Iterator<? extends T> getIterator() {
            while (!currentIterator.hasNext()) {
                CloseableUtil.close(currentIterator);
                if (iterablesIterator.hasNext()) {
                    currentIterator = iterablesIterator.next().iterator();
                } else {
                    break;
                }
            }

            return currentIterator;
        }
    }
}
