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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An {@code EmptyIterable} is an {@link java.io.Closeable} {@link java.lang.Iterable}
 * which is backed by a {@link EmptyIterator}, and contains no objects.
 *
 * This is useful when a {@link java.lang.Iterable} is required, but there is no data present.
 *
 * @param <T> the type of items in the iterable.
 */
public class EmptyIterable<T> implements Closeable, Iterable<T> {

    @Override
    public void close() {
    }

    @Override
    public Iterator<T> iterator() {
        return new EmptyIterator<>();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return new EqualsBuilder()
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .toHashCode();
    }
}
