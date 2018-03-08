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
 * A {@code ChainedIterable} is an iterable composed of other {@link java.lang.Iterable}s.
 *
 * As a client iterates through this iterable, the child iterables are consumed
 * sequentially.
 *
 * @param <T> the type of items in the iterable.
 */
public class ChainedIterable<T> implements CloseableIterable<T> {
    private final Iterable<T>[] itrs;
    private final int n;

    public ChainedIterable(final Iterable... itrs) {
        if (null == itrs || 0 == itrs.length) {
            throw new IllegalArgumentException("At least 1 iterable is required.");
        }
        this.itrs = itrs;
        n = this.itrs.length;
    }

    @Override
    public void close() {
        for (final Iterable<T> itr : itrs) {
            CloseableUtil.close(itr);
        }
    }

    @Override
    public CloseableIterator<T> iterator() {
        return new IteratorWrapper();
    }

    private class IteratorWrapper implements CloseableIterator<T> {
        private final Iterator<T>[] iterators = new Iterator[itrs.length];
        private int index = 0;

        @Override
        public boolean hasNext() {
            return -1 != getNextIndex();
        }

        @Override
        public T next() {
            index = getNextIndex();
            if (-1 == index) {
                throw new NoSuchElementException();
            }

            return getIterator(index).next();
        }

        private int getNextIndex() {
            boolean hasNext = getIterator(index).hasNext();
            int nextIndex = index;
            while (!hasNext) {
                nextIndex = nextIndex + 1;
                if (nextIndex < n) {
                    hasNext = getIterator(nextIndex).hasNext();
                } else {
                    nextIndex = -1;
                    break;
                }
            }

            return nextIndex;
        }

        @Override
        public void remove() {
            getIterator(index).remove();
        }

        private Iterator<T> getIterator(final int i) {
            if (null == iterators[i]) {
                iterators[i] = itrs[i].iterator();
            }

            return iterators[i];
        }

        @Override
        public void close() {
            for (final Iterator<T> itr : iterators) {
                CloseableUtil.close(itr);
            }
            ChainedIterable.this.close();
        }
    }
}
