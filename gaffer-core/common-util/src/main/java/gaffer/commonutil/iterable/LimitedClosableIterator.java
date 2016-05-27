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

package gaffer.commonutil.iterable;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class LimitedClosableIterator<T> implements CloseableIterator<T> {
    private final CloseableIterator<T> iterator;
    private final int end;
    private int index = 0;

    public LimitedClosableIterator(final Iterator<T> iterator, final int start, final int end) {
        this(new WrappedClosableIterator<>(iterator), start, end);
    }

    public LimitedClosableIterator(final CloseableIterator<T> iterator, final int start, final int end) {
        if (null == iterator) {
            this.iterator = new EmptyCloseableIterator<>();
        } else {
            this.iterator = iterator;
        }
        this.end = end;

        while (index < start && hasNext()) {
            next();
            index++;
        }
    }

    @Override
    public void close() {
        iterator.close();
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = index < end && iterator.hasNext();
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
}
