/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.commonutil.collection;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * <p>
 * A {@link SortedSet} which maintains the ordering
 * of its elements, according to either their natural order or an explicit {@link Comparator}.
 * </p>
 * <p>
 * This collection is backed by a {@link TreeSet},
 * restricted to a maximum size (defaulted to {@link Long#MAX_VALUE},
 * with items being dropped from the end of the backing collection.
 * </p>
 * <p>
 * Most methods (with the exception of the addition logic) are delegated to the
 * backing collection.
 * </p>
 *
 * @param <E> the type of object to store in the {@link LimitedSortedSet}.
 */
public class LimitedSortedSet<E> implements SortedSet<E> {
    private final Comparator<E> comparator;

    private TreeSet<E> backingSet;
    private int limit;

    public LimitedSortedSet(final Comparator<E> comparator, final int limit) {
        if (null == comparator) {
            throw new IllegalArgumentException("Comparator is required");
        }
        if (limit < 1) {
            throw new IllegalArgumentException("Limit should be more than 0");
        }
        this.comparator = comparator;
        this.limit = limit;
        this.backingSet = new TreeSet<>(comparator);
    }

    @Override
    public int size() {
        return backingSet.size();
    }

    @Override
    public boolean isEmpty() {
        return backingSet.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        return backingSet.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return backingSet.iterator();
    }

    @Override
    public Object[] toArray() {
        return backingSet.toArray();
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        return backingSet.toArray(a);
    }

    @Override
    public boolean add(final E e) {
        if (backingSet.size() < limit) {
            return backingSet.add(e);
        } else if (comparator.compare(backingSet.last(), e) > 0) {
            backingSet.remove(backingSet.last());
            return backingSet.add(e);
        }
        return false;
    }

    @Override
    public boolean remove(final Object o) {
        return backingSet.remove(o);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        return backingSet.containsAll(c);
    }

    @Override
    public boolean addAll(final Collection<? extends E> c) {
        return c.stream()
                .map(this::add)
                .reduce(Boolean::logicalOr)
                .orElse(false);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        return backingSet.retainAll(c);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        return backingSet.removeAll(c);
    }

    @Override
    public void clear() {
        backingSet.clear();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final LimitedSortedSet<?> that = (LimitedSortedSet<?>) obj;

        return new EqualsBuilder()
                .append(limit, that.limit)
                .append(backingSet, that.backingSet)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(limit)
                .append(backingSet)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("limit", limit)
                .append("backingSet", backingSet)
                .toString();
    }

    @Override
    public Comparator<? super E> comparator() {
        return comparator;
    }

    @Override
    public SortedSet<E> subSet(final E fromElement, final E toElement) {
        return backingSet.subSet(fromElement, toElement);
    }

    @Override
    public SortedSet<E> headSet(final E toElement) {
        return backingSet.headSet(toElement);
    }

    @Override
    public SortedSet<E> tailSet(final E fromElement) {
        return backingSet.tailSet(fromElement);
    }

    @Override
    public E first() {
        return backingSet.first();
    }

    @Override
    public E last() {
        return backingSet.last();
    }
}
