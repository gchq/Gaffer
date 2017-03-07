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

package uk.gov.gchq.gaffer.commonutil.iterable;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ChainedIterable<T> implements Iterable<T> {
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
    public Iterator<T> iterator() {
        return new IteratorWrapper();
    }

    private class IteratorWrapper implements Iterator<T> {
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
    }
}
