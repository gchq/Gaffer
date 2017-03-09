/*
 * Copyright 2016-2017 Crown Copyright
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

public class LimitedCloseableIterator<T> implements CloseableIterator<T> {
    private final CloseableIterator<T> iterator;
    private final Integer end;
    private int index = 0;

    public LimitedCloseableIterator(final Iterator<T> iterator, final int start, final Integer end) {
        this(new WrappedCloseableIterator<>(iterator), start, end);
    }

    public LimitedCloseableIterator(final CloseableIterator<T> iterator, final int start, final Integer end) {
        if (null != end && start > end) {
            throw new IllegalArgumentException("start should be less than end");
        }

        if (null == iterator) {
            this.iterator = new EmptyCloseableIterator<>();
        } else {
            this.iterator = iterator;
        }
        this.end = end;

        while (index < start && hasNext()) {
            next();
        }
    }

    @Override
    public void close() {
        iterator.close();
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = (null == end || index < end) && iterator.hasNext();
        if (!hasNext) {
            close();
        }

        return hasNext;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        index++;
        return iterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
