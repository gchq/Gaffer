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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A {@link CachingIterable} is a {@link CloseableIterable} that attempts to
 * cache the iterable the first time it is read. Subsequently, when iterator is
 * called, an iterator to the cached iterable is returned. The caching is disabled
 * if the iterable size is greater than a provided max size. By default this is
 * 100,000.
 *
 * @param <T> the type of the iterable.
 */
public class CachingIterable<T> implements CloseableIterable<T> {
    public static final int DEFAULT_MAX_SIZE = 100000;

    private final Iterable<T> iterable;
    private final int maxSize;
    private boolean tooLarge;

    private Iterable<T> cachedIterable;

    public CachingIterable(final Iterable<T> iterable) {
        this(iterable, DEFAULT_MAX_SIZE);
    }

    public CachingIterable(final Iterable<T> iterable, final int maxSize) {
        if (null == iterable) {
            this.iterable = Collections.emptyList();
            this.cachedIterable = Collections.emptyList();
        } else if (iterable instanceof Collection) {
            this.iterable = iterable;
            this.cachedIterable = iterable;
        } else {
            this.iterable = iterable;
        }
        this.maxSize = maxSize;
        this.tooLarge = false;
    }

    @Override
    public CloseableIterator<T> iterator() {
        if (null != cachedIterable) {
            return new WrappedCloseableIterator<>(cachedIterable.iterator());
        }

        if (tooLarge) {
            return new WrappedCloseableIterator<>(iterable.iterator());
        }

        return new CachingIterator();
    }

    @Override
    public void close() {
        CloseableUtil.close(iterable);
    }

    private class CachingIterator implements CloseableIterator<T> {
        private final Iterator<T> iterator = iterable.iterator();
        private List<T> cache = new ArrayList<>(Math.min(DEFAULT_MAX_SIZE, maxSize));
        private boolean closed = false;

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }

            if (iterator.hasNext()) {
                return true;
            }

            // Reached the end of the iterable.
            // Store the cache to others to use.
            // This will be null if the cache wasn't big enough.
            cachedIterable = cache;
            close();
            return false;
        }

        @Override
        public T next() {
            if (closed) {
                throw new NoSuchElementException("Iterator has been closed");
            }

            final T next = iterator.next();
            if (null != cache) {
                if (cache.size() < maxSize) {
                    cache.add(next);
                } else {
                    tooLarge = true;
                    cache = null;
                }
            }
            return next;
        }

        @Override
        public void close() {
            closed = true;
            CloseableUtil.close(iterator);
            CachingIterable.this.close();
        }
    }
}
