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

import org.apache.commons.lang.builder.ToStringBuilder;
import java.util.Iterator;

public class WrappedCloseableIterable<T> implements CloseableIterable<T> {
    private final Iterable<T> iterable;

    public WrappedCloseableIterable() {
        this(null);
    }

    public WrappedCloseableIterable(final Iterable<T> iterable) {
        if (null == iterable) {
            this.iterable = new EmptyClosableIterable<>();
        } else {
            this.iterable = iterable;
        }
    }

    @Override
    public void close() {
        if (iterable instanceof CloseableIterable) {
            ((CloseableIterable) iterable).close();
        }
    }

    @Override
    public CloseableIterator<T> iterator() {
        final Iterator<T> iterator = iterable.iterator();
        if (iterator instanceof CloseableIterator) {
            return ((CloseableIterator<T>) iterator);
        }

        return new WrappedCloseableIterator<>(iterator);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("iterable", iterable)
                .toString();
    }
}
