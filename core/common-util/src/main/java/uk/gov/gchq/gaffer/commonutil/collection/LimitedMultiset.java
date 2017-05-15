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

import com.google.common.collect.BoundType;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;

/**
 * A {@link com.google.common.collect.SortedMultiset} which maintains the ordering
 * of its elements, according to either their natural order or an explicit {@link Comparator}.
 *
 * This collection is backed by a {@link com.google.common.collect.TreeMultiset},
 * restricted to a maximum size (defaulted to {@link Long#MAX_VALUE},
 * with items being dropped from the end of the backing collection.
 *
 * Most methods (with the exception of the addition logic) are delegated to the
 * backing collection.
 *
 * @param <E> the type of object to store in the {@link uk.gov.gchq.gaffer.commonutil.collection.LimitedMultiset}.
 */
public class LimitedMultiset<E> implements SortedMultiset<E>, Cloneable, java.io.Serializable {
    private static final long serialVersionUID = -6090845611480684148L;

    private TreeMultiset<E> backingSet;
    private long limit;

    public LimitedMultiset() {
        this.backingSet = TreeMultiset.create((Comparator<? super E>) null);
        this.limit = Long.MAX_VALUE;
    }

    public LimitedMultiset(final Comparator<E> comparator, final long limit) {
        this.backingSet = TreeMultiset.create(comparator);
        this.limit = limit;
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
    public SortedMultiset<E> descendingMultiset() {
        return backingSet.descendingMultiset();
    }

    @Override
    public SortedMultiset<E> headMultiset(final E upperBound, final BoundType boundType) {
        return backingSet.headMultiset(upperBound, boundType);
    }

    @Override
    public SortedMultiset<E> subMultiset(final E lowerBound, final BoundType lowerBoundType, final E upperBound, final BoundType upperBoundType) {
        return backingSet.subMultiset(lowerBound, lowerBoundType, upperBound, upperBoundType);
    }

    @Override
    public SortedMultiset<E> tailMultiset(final E lowerBound, final BoundType boundType) {
        return backingSet.tailMultiset(lowerBound, boundType);
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
        } else {
            if (backingSet.comparator()
                          .compare(backingSet.lastEntry()
                                             .getElement(), e) > 0) {
                backingSet.remove(backingSet.lastEntry()
                                            .getElement());
                return backingSet.add(e);
            }
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
    public int count(@Nullable final Object o) {
        return backingSet.count(o);
    }

    @Override
    public int add(@Nullable final E e, final int count) {
        final int prevCount = backingSet.count(e);
        for (int i = 0; i < count; i++) {
            if (backingSet.size() < limit) {
                backingSet.add(e);
            } else {
                if (backingSet.comparator()
                              .compare(backingSet.lastEntry()
                                                 .getElement(), e) > 0) {
                    backingSet.remove(backingSet.lastEntry()
                                                .getElement());
                    backingSet.add(e);
                }
            }
        }
        return prevCount;
    }

    @Override
    public int remove(@Nullable final Object o, final int i) {
        return backingSet.remove(o, i);
    }

    @Override
    public int setCount(final E e, final int i) {
        throw new UnsupportedOperationException("The setCount(E, int) method is not supported on a LimitedMultiset." +
                " Please add elements using the add(E) or addAll(java.util.Collection<E>) methods.");
    }

    @Override
    public boolean setCount(final E e, final int i, final int i1) {
        throw new UnsupportedOperationException("The setCount(E, int, int) method is not supported on a LimitedMultiset." +
                " Please add elements using the add(E) or addAll(java.util.Collection<E>) methods.");
    }

    @Override
    public Comparator<? super E> comparator() {
        return backingSet.comparator();
    }

    @Override
    public Entry<E> firstEntry() {
        return backingSet.firstEntry();
    }

    @Override
    public Entry<E> lastEntry() {
        return backingSet.lastEntry();
    }

    public E first() {
        return backingSet.firstEntry().getElement();
    }

    public E last() {
        return backingSet.lastEntry().getElement();
    }

    @Override
    public Entry<E> pollFirstEntry() {
        return backingSet.pollFirstEntry();
    }

    @Override
    public Entry<E> pollLastEntry() {
        return backingSet.pollLastEntry();
    }

    public E pollFirst() {
        return backingSet.pollFirstEntry().getElement();
    }

    public E pollLast() {
        return backingSet.pollLastEntry().getElement();
    }

    @Override
    public SortedSet<E> elementSet() {
        return backingSet.elementSet();
    }

    @Override
    public Set<Entry<E>> entrySet() {
        return backingSet.entrySet();
    }

    @Override
    public LimitedMultiset<E> clone() {
        LimitedMultiset<E> clone;
        try {
            clone = (LimitedMultiset<E>) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new InternalError(e);
        }

        clone.backingSet = TreeMultiset.create(backingSet.comparator());
        clone.backingSet.addAll(backingSet);

        clone.limit = limit;
        return clone;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final LimitedMultiset<?> that = (LimitedMultiset<?>) obj;

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
}
