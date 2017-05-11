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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

public class LimitedTreeSet<E> implements NavigableSet<E>, Cloneable, java.io.Serializable {
    private static final long serialVersionUID = -6090845611480684148L;
    private long limit;
    private TreeSet<E> backingSet;

    public LimitedTreeSet() {
        this.backingSet = new TreeSet<>();
        this.limit = Long.MAX_VALUE;
    }

    public LimitedTreeSet(final Comparator<E> comparator, final long limit) {
        this.backingSet = new TreeSet<>(comparator);
        this.limit = limit;
    }

    @Override
    public E lower(final E e) {
        return backingSet.lower(e);
    }

    @Override
    public E floor(final E e) {
        return backingSet.floor(e);
    }

    @Override
    public E ceiling(final E e) {
        return backingSet.ceiling(e);
    }

    @Override
    public E higher(final E e) {
        return backingSet.higher(e);
    }

    @Override
    public E pollFirst() {
        return backingSet.pollFirst();
    }

    @Override
    public E pollLast() {
        return backingSet.pollLast();
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
        } else {
            if (backingSet.comparator()
                          .compare(backingSet.last(), e) > 0) {
                backingSet.remove(backingSet.last());
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
                .map(backingSet::add)
                .anyMatch(Boolean.TRUE::equals);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        return backingSet.retainAll(c);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {
        backingSet.clear();
    }

    @Override
    public NavigableSet<E> descendingSet() {
        return backingSet.descendingSet();
    }

    @Override
    public Iterator<E> descendingIterator() {
        return backingSet.descendingIterator();
    }

    @Override
    public NavigableSet<E> subSet(final E fromElement, final boolean fromInclusive, final E toElement, final boolean toInclusive) {
        return backingSet.subSet(fromElement, fromInclusive, toElement, toInclusive);
    }

    @Override
    public NavigableSet<E> headSet(final E toElement, final boolean inclusive) {
        return backingSet.headSet(toElement, inclusive);
    }

    @Override
    public NavigableSet<E> tailSet(final E fromElement, final boolean inclusive) {
        return backingSet.tailSet(fromElement, inclusive);
    }

    @Override
    public Comparator<? super E> comparator() {
        return backingSet.comparator();
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

    @Override
    public Object clone() {
        LimitedTreeSet<E> clone;
        try {
            clone = (LimitedTreeSet<E>) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new InternalError(e);
        }

        clone.backingSet = new TreeSet<>(backingSet);
        clone.limit = limit;
        return clone;
    }
}
