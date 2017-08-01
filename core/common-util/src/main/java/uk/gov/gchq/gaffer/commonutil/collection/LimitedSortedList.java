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
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * An {@link Iterable} which can sort, limit and deduplicate its elements.
 * Sorting is achieved with a provided {@link Comparator}.
 * </p>
 * <p>
 * This collection is backed by a {@link LinkedList},
 * with items being dropped from the end of the backing collection.
 * </p>
 *
 * @param <E> the type of object to store in the {@link LimitedSortedList}.
 */
public class LimitedSortedList<E> implements List<E> {
    // Dummy value to associate with an Object in the backing Map
    private static final Object PRESENT = new Object();

    private final Comparator<E> comparator;
    private final boolean deduplicate;
    private final Integer limit;
    private LinkedList<E> backingList;

    // Used to deduplicate elements if required
    private HashMap<E, Object> backingHashSet;

    public LimitedSortedList(final Comparator<E> comparator) {
        this(comparator, null);
    }

    public LimitedSortedList(final Comparator<E> comparator, final Integer limit) {
        this(comparator, limit, false);
    }

    public LimitedSortedList(final Comparator<E> comparator, final Integer limit, final boolean deduplicate) {
        this(comparator, limit, deduplicate, new LinkedList<>());
    }

    public LimitedSortedList(final Comparator<E> comparator, final Integer limit, final boolean deduplicate, final LinkedList<E> backingList) {
        if (null == comparator) {
            throw new IllegalArgumentException("Comparator is required");
        }
        if (null != limit && limit < 1) {
            throw new IllegalArgumentException("Limit should be more than 0");
        }
        if (null == backingList) {
            throw new IllegalArgumentException("backingList is required");
        }
        this.comparator = comparator;
        this.limit = limit;
        this.deduplicate = deduplicate;
        this.backingList = backingList;

        if (deduplicate) {
            backingHashSet = new HashMap<>();
        }
    }

    public boolean add(final E e) {
        if (deduplicate && backingHashSet.containsKey(e)) {
            return false;
        }

        if (null == limit || backingList.size() < limit) {
            orderedAdd(e);
            return true;
        } else {
            if (comparator.compare(backingList.peekLast(), e) > 0) {
                final E removedItem = backingList.removeLast();
                if (deduplicate) {
                    backingHashSet.remove(removedItem);
                }
                orderedAdd(e);
                return true;
            }
        }

        return false;
    }

    private void orderedAdd(final E e) {
        int index = 0;
        for (final E existingE : backingList) {
            if (comparator.compare(existingE, e) >= 0) {
                break;
            }
            index++;
        }
        backingList.add(index, e);
        if (deduplicate) {
            backingHashSet.put(e, PRESENT);
        }
    }

    @Override
    public boolean remove(final Object o) {
        if (deduplicate) {
            backingHashSet.remove(o);
        }
        return backingList.remove(o);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        if (deduplicate) {
            backingHashSet.keySet().containsAll(c);
        }
        return backingList.containsAll(c);
    }

    @Override
    public boolean addAll(final Collection<? extends E> c) {
        boolean result = false;
        for (final E e : c) {
            if (add(e)) {
                result = true;
            }
        }

        return result;
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        if (deduplicate) {
            for (final Object o : c) {
                backingHashSet.remove(o);
            }
        }
        return backingList.removeAll(c);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        if (deduplicate) {
            backingHashSet.clear();
            for (final Object o : c) {
                backingHashSet.put((E) o, PRESENT);
            }
        }
        return backingList.retainAll(c);
    }

    @Override
    public void clear() {
        backingList.clear();
    }

    @Override
    public int size() {
        return backingList.size();
    }

    @Override
    public boolean isEmpty() {
        return backingList.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        if (deduplicate) {
            backingHashSet.containsKey(o);
        }

        return backingList.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return backingList.iterator();
    }

    @Override
    public Object[] toArray() {
        return backingList.toArray();
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        return backingList.toArray(a);
    }

    @Override
    public E get(final int index) {
        return backingList.get(index);
    }

    @Override
    public E set(final int index, final E element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(final int index, final E element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E remove(final int index) {
        final E o = backingList.remove(index);
        if (deduplicate) {
            backingHashSet.remove(o);
        }

        return o;
    }

    @Override
    public int indexOf(final Object o) {
        return backingList.indexOf(o);
    }

    @Override
    public int lastIndexOf(final Object o) {
        return backingList.lastIndexOf(o);
    }

    @Override
    public ListIterator<E> listIterator() {
        return backingList.listIterator();
    }

    @Override
    public ListIterator<E> listIterator(final int index) {
        return backingList.listIterator(index);
    }

    @Override
    public List<E> subList(final int fromIndex, final int toIndex) {
        return backingList.subList(fromIndex, toIndex);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final LimitedSortedList<?> that = (LimitedSortedList<?>) obj;

        return new EqualsBuilder()
                .append(limit, that.limit)
                .append(deduplicate, that.deduplicate)
                .append(backingList, that.backingList)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(limit)
                .append(deduplicate)
                .append(backingList)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("limit", limit)
                .append("deduplicate", deduplicate)
                .append("backingList", backingList)
                .toString();
    }
}
