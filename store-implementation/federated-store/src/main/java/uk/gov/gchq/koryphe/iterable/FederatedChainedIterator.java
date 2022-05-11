/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.koryphe.iterable;

import uk.gov.gchq.koryphe.util.CloseableUtil;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;

import static java.util.Objects.nonNull;

/**
 * @param <T> the type of items in the iterator
 */
@Deprecated
public class FederatedChainedIterator<T> implements Closeable, Iterator<T> {
    private final Iterator<? extends Iterable<? extends T>> iterablesIterator;
    private Iterator<? extends T> currentIterator = Collections.emptyIterator();

    public FederatedChainedIterator(final Iterator<? extends Iterable<? extends T>> iterablesIterator) {
        if (null == iterablesIterator) {
            throw new IllegalArgumentException("iterables are required");
        }
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
                Object next = iterablesIterator.next();
                if (nonNull(next) && next instanceof Iterable) {
                    currentIterator = (Iterator<? extends T>) ((Iterable<?>) next).iterator();
                } else if (nonNull(next) && !(next instanceof Iterable)) {
                    throw new IllegalStateException(String.format("Iterator of Iterator contains non-iterable class: %s object: %s", next.getClass(), next));
                }
            } else {
                break;
            }
        }
        return currentIterator;
    }
}
