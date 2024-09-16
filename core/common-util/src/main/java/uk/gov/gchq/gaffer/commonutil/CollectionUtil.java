/*
 * Copyright 2016-2023 Crown Copyright
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

import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public final class CollectionUtil {

    private CollectionUtil() {
        // Private constructor to prevent instantiation.
    }

    public static Iterable[] toIterableArray(final Collection<? extends Iterable> collection) {
        if (null == collection) {
            return null;
        }

        return collection.toArray(new Iterable[collection.size()]);
    }

    public static <T> TreeSet<T> treeSet(final T... items) {
        final TreeSet<T> treeSet = new TreeSet<>();
        if (null != items) {
            for (final T item : items) {
                if (null != item) {
                    treeSet.add(item);
                }
            }
        }

        return treeSet;
    }

    public static <K, V> void toMapWithClassKeys(final Map<String, V> mapAsStrings, final Map<Class<? extends K>, V> map) throws ClassNotFoundException {
        for (final Map.Entry<String, V> entry : mapAsStrings.entrySet()) {
            map.put(
                    (Class) Class.forName(SimpleClassNameIdResolver.getClassName(entry.getKey())),
                    entry.getValue()
            );
        }
    }

    public static <K, V> Map<Class<? extends K>, V> toMapWithClassKeys(final Map<String, V> mapAsStrings) throws ClassNotFoundException {
        final Map<Class<? extends K>, V> map = new HashMap<>(mapAsStrings.size());
        toMapWithClassKeys(mapAsStrings, map);
        return map;
    }

    public static <K, V> void toMapWithStringKeys(final Map<Class<? extends K>, V> map, final Map<String, V> mapAsStrings) {
        for (final Map.Entry<Class<? extends K>, V> entry : map.entrySet()) {
            mapAsStrings.put(
                    entry.getKey().getName(),
                    entry.getValue()
            );
        }
    }

    public static <K, V> Map<String, V> toMapWithStringKeys(final Map<Class<? extends K>, V> map) {
        final Map<String, V> mapAsStrings = new HashMap<>();
        toMapWithStringKeys(map, mapAsStrings);
        return mapAsStrings;
    }

    public static boolean containsAny(final Collection collection, final Object... objects) {
        boolean result = false;
        if (null != collection && null != objects) {
            for (final Object object : objects) {
                if (collection.contains(object)) {
                    result = true;
                    break;
                }
            }
        }

        return result;
    }

    public static boolean anyMissing(final Collection collection, final Object... objects) {
        boolean result = false;
        if (null == collection || collection.isEmpty()) {
            if (null != objects && 0 < objects.length) {
                result = true;
            }
        } else if (null != objects) {
            for (final Object object : objects) {
                if (!collection.contains(object)) {
                    result = true;
                    break;
                }
            }
        }

        return result;
    }

    /**
     * Determine whether all of the items in the given {@link Collection} are unique.
     *
     * @param collection the collection to check
     * @param <T>        the type of object contained in the collection
     * @return {@code true} if all the items in the Collection are unique (as determined
     * by {@link Object#equals(Object)}, otherwise {@code false}
     */
    public static <T> boolean distinct(final Collection<T> collection) {
        final Set<T> set = new HashSet<>();

        for (final T t : collection) {
            if (!set.add(t)) {
                return false;
            }
        }

        return true;
    }
}
