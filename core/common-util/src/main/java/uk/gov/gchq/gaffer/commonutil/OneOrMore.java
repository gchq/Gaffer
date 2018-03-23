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

package uk.gov.gchq.gaffer.commonutil;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * A {@code OneOrMore} in an {@link Iterable} that allows items
 * to be added. It wraps an {@link ArrayList} or {@link HashSet} depending
 * on whether deduplication is enabled.
 * This class is designed to be used in the case when the iterable quite often
 * just contains a single item. In the case only a single item is required, the class
 * doesn't instantiate the Collection it just holds onto the single item. If additional
 * items are added then a new backing collection is created to hold the multiple
 * values.
 *
 * @param <T> the iterable type.
 */
public class OneOrMore<T> implements Iterable<T> {
    private final Function<T, Collection<T>> newCollection;
    private final boolean deduplicate;

    private Collection<T> collection;
    private T singleItem;

    public OneOrMore() {
        this(true);
    }

    public OneOrMore(final boolean deduplicate) {
        this(deduplicate, null);
    }

    public OneOrMore(final boolean deduplicate, final T item) {
        this.deduplicate = deduplicate;
        this.singleItem = item;
        if (deduplicate) {
            newCollection = k -> {
                final Collection<T> collection = new HashSet<>();
                collection.add(k);
                return collection;
            };
        } else {
            newCollection = k -> {
                final Collection<T> collection = new ArrayList<>();
                collection.add(k);
                return collection;
            };
        }
    }

    public boolean add(final T item) {
        if (null == collection) {
            if (null == singleItem) {
                singleItem = item;
                return true;
            }

            if (deduplicate && singleItem.equals(item)) {
                return false;
            }

            collection = newCollection.apply(singleItem);
            singleItem = null;
        }

        return collection.add(item);
    }

    public boolean addAll(final Collection<? extends T> items) {
        boolean result = false;
        for (final T item : items) {
            if (add(item)) {
                result = true;
            }
        }
        return result;
    }

    public void removeAnyItem() {
        if (null == collection) {
            singleItem = null;
        } else {
            if (deduplicate) {
                collection.remove(collection.iterator().next());
            } else {
                ((List<T>) collection).remove(collection.size() - 1);
            }
        }
    }

    public int size() {
        if (null == collection) {
            if (null != singleItem) {
                return 1;
            }
            return 0;
        }

        return collection.size();
    }

    public boolean isEmpty() {
        if (null == collection) {
            return null == singleItem;
        }

        return collection.isEmpty();
    }

    public boolean contains(final Object o) {
        if (null == collection) {
            return null != singleItem && singleItem.equals(o);

        }
        return collection.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        if (null == collection) {
            if (null == singleItem) {
                return Iterators.emptyIterator();
            }

            return Iterators.singletonIterator(singleItem);
        }
        return collection.iterator();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final OneOrMore<?> that = (OneOrMore<?>) obj;

        return new EqualsBuilder()
                .append(deduplicate, that.deduplicate)
                .append(singleItem, that.singleItem)
                .append(collection, that.collection)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 31)
                .append(deduplicate)
                .append(singleItem)
                .append(collection)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("deduplicate", deduplicate)
                .append("singleItem", singleItem)
                .append("collection", collection)
                .toString();
    }
}
