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

import com.fasterxml.jackson.annotation.JsonIgnore;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;

import java.io.Closeable;
import java.util.Iterator;

/**
 * A {@code LimitedIterable} is a {@link java.io.Closeable}
 * {@link java.lang.Iterable} which is limited to a maximum size.
 *
 * @param <T> the type of items in the iterable.
 */
public class LimitedIterable<T> implements Closeable, Iterable<T> {
    private final Iterable<T> iterable;
    private final int start;
    private final Integer end;
    private final Boolean truncate;

    public LimitedIterable(final Iterable<T> iterable, final int start, final Integer end) {
        this(iterable, start, end, true);
    }

    public LimitedIterable(final Iterable<T> iterable, final int start, final Integer end, final Boolean truncate) {
        if (null != end && start > end) {
            throw new IllegalArgumentException("The start pointer must be less than the end pointer.");
        }

        if (null == iterable) {
            this.iterable = new EmptyIterable<>();
        } else {
            this.iterable = iterable;
        }

        this.start = start;
        this.end = end;
        this.truncate = truncate;

    }

    @JsonIgnore
    public int getStart() {
        return start;
    }

    @JsonIgnore
    public Integer getEnd() {
        return end;
    }

    @Override
    public void close() {
        CloseableUtil.close(iterable);
    }

    @Override
    public Iterator<T> iterator() {
        return new LimitedIterator<>(iterable.iterator(), start, end, truncate);
    }
}
