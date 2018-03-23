/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Extension to {@link ArrayBlockingQueue} to allow consumers to iterate over
 * the queue, consuming the data, without being blocked.
 *
 * @param <T> the type of object in the queue
 */
public class ConsumableBlockingQueue<T> extends ArrayBlockingQueue<T> {
    private static final long serialVersionUID = 4048319404021495269L;

    public ConsumableBlockingQueue(final int maxSize) {
        super(maxSize);
    }

    @Override
    @Nonnull
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return !isEmpty();
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more items");
                }
                return poll();
            }
        };
    }

    /**
     * Warning - this will convert the entire queue to an array to check if the
     * items are equal so use it with with caution.
     *
     * @param obj the object to compare
     * @return true if equal, otherwise false.
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final ConsumableBlockingQueue queue = (ConsumableBlockingQueue) obj;

        return new EqualsBuilder()
                .append(toArray(), queue.toArray())
                .isEquals();
    }

    /**
     * Warning - this will convert the entire queue to an array to get a hashcode,
     * so use it with caution.
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 47)
                .append(toArray())
                .toHashCode();
    }

    /**
     * Warning - this will convert the entire queue to an array to get a string,
     * so use with caution.
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("items", toArray())
                .toString();
    }
}
