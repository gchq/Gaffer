/*
 * Copyright 2017-2024 Crown Copyright
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

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.OneOrMore;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.koryphe.iterable.ChainedIterable;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * <p>
 * An {@link java.lang.Iterable} which can sort, limit and deduplicate its elements.
 * Sorting is achieved with a provided {@link Comparator}.
 * </p>
 * <p>
 * This iterable is backed by a {@link TreeMap},
 * with items being dropped from the end of the backing TreeMap. To allow
 * duplicates and to be stored the TreeMap is a map of item to collection of items.
 * The tree map uses the comparator for equality checking, so the collection of
 * items will contain items with a comparator score of 0, but potentially not equal
 * using the equals method.
 * </p>
 *
 * @param <E> the type of object to store in the {@link LimitedInMemorySortedIterable}.
 */
public class LimitedInMemorySortedIterable<E> implements Iterable<E> {

    private final Comparator<E> comparator;
    private final boolean deduplicate;
    private final Integer limit;
    private final TreeMap<E, OneOrMore<E>> backingMap;
    private int size;

    public LimitedInMemorySortedIterable(final Comparator<E> comparator) {
        this(comparator, null);
    }

    public LimitedInMemorySortedIterable(final Comparator<E> comparator, final Integer limit) {
        this(comparator, limit, false);
    }

    public LimitedInMemorySortedIterable(final Comparator<E> comparator, final Integer limit, final boolean deduplicate) {
        if (isNull(comparator)) {
            throw new IllegalArgumentException("Comparator is required");
        }
        if (nonNull(limit) && 1 > limit) {
            throw new IllegalArgumentException("Limit cannot be less than or equal to 0");
        }

        this.comparator = comparator;
        this.deduplicate = deduplicate;
        this.limit = limit;
        this.backingMap = new TreeMap<>(comparator);
        size = 0;
    }

    public boolean add(final E e) {
        boolean result = false;

        final OneOrMore<E> values = backingMap.get(e);
        // Skip the item if we are deduplicating and the item already exists.
        boolean skipItem = (deduplicate && nonNull(values) && values.contains(e));
        if (!skipItem) {
            if (nonNull(limit) && size >= limit) {
                // Check the item against the last item.
                final Map.Entry<E, OneOrMore<E>> last = backingMap.lastEntry();
                // Checks if the last items key is greater than e
                if (0 < comparator.compare(last.getKey(), e)) {
                    // Checks if the items value contains a collection or a single item
                    if (1 < last.getValue().size()) {
                        // Items value contains a collection.
                        // Remove item from collection.
                        last.getValue().removeAnyItem();
                    } else {
                        // Items value contains a single item.
                        // Remove item from backingMap.
                        backingMap.remove(last.getKey());
                    }
                    size--;
                    // e is bigger than the lastEntry.
                } else {
                    // Skip adding the item.
                    skipItem = true;
                }
            }

            if (!skipItem) {
                if (null == values) {
                    backingMap.put(e, new OneOrMore<>(deduplicate, e));
                    size++;
                    result = true;
                } else if (values.add(e)) {
                    size++;
                    result = true;
                }
            }
        }

        return result;
    }

    public boolean addAll(final Iterable<E> items) {
        boolean result = false;
        for (final E item : items) {
            if (add(item)) {
                result = true;
            }
        }
        return result;
    }

    public int size() {
        return size;
    }

    @Override
    @SuppressWarnings("PMD.UseTryWithResources")
    public Iterator<E> iterator() {
        if (backingMap.isEmpty()) {
            return Collections.emptyIterator();
        }

        @SuppressWarnings("unchecked")
        final Iterable<? extends E>[] values = Iterables.toArray(backingMap.values(), Iterable.class);
        if (ArrayUtils.isEmpty(values)) {
            return Collections.emptyIterator();
        } else {
            ChainedIterable<E> iterable = null;
            try {
                iterable = new ChainedIterable<E>(values);
                return iterable.iterator();
            } finally {
                CloseableUtil.close(iterable);
            }
        }
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }

        final LimitedInMemorySortedIterable<?> that = (LimitedInMemorySortedIterable<?>) obj;

        return new EqualsBuilder()
                .append(size, that.size)
                .append(limit, that.limit)
                .append(deduplicate, that.deduplicate)
                .append(backingMap, that.backingMap)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(size)
                .append(limit)
                .append(deduplicate)
                .append(backingMap)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("size", size)
                .append("limit", limit)
                .append("deduplicate", deduplicate)
                .append("backingMap", backingMap)
                .toString();
    }
}
