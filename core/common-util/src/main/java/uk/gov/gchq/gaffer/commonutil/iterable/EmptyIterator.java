/*
 * Copyright 2016-2020 Crown Copyright
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

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An {@code EmptyCloseableIterator} is a {@link java.io.Closeable}
 * {@link java.util.Iterator} which contains no objects. This is
 * achieved by forcing the {@link java.util.Iterator#hasNext()} method
 * to always return false.
 *
 * @param <T> the type of items in the iterator.
 */
public class EmptyIterator<T> implements Closeable, Iterator<T> {
    @Override
    public void close() {
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
