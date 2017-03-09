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

import org.apache.commons.io.IOUtils;
import java.io.Closeable;
import java.util.Iterator;

public class WrappedCloseableIterator<T> implements CloseableIterator<T> {
    private final Iterator<T> iterator;

    public WrappedCloseableIterator() {
        this(null);
    }

    public WrappedCloseableIterator(final Iterator<T> iterator) {
        if (null == iterator) {
            this.iterator = new EmptyCloseableIterator<>();
        } else {
            this.iterator = iterator;
        }
    }

    @Override
    public void close() {
        if (iterator instanceof Closeable) {
            IOUtils.closeQuietly((Closeable) iterator);
        }
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public void remove() {
        iterator.remove();
    }
}
