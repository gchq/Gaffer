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

package uk.gov.gchq.gaffer.data.graph.entity;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * An {@code EntityMaps} object contains a number of {@link EntityMap}
 * objects and can be used to represent the changes in an EntityMap over time
 * or to track the entities of a graph over some other metric.
 */
public interface EntityMaps extends Iterable<EntityMap> {

    /**
     * Add a new {@link EntityMap}.
     *
     * @param entityMap the entityMap to add
     */
    default void add(final EntityMap entityMap) {
        asList().add(entityMap);
    }

    /**
     * Retrieve the nth {@link EntityMap}.
     *
     * @param n the index of the adjacency map to retrieve
     * @return the nth entityMap
     */
    default EntityMap get(final int n) {
        return asList().get(n);
    }

    /**
     * Return the number of {@link EntityMap} present in the entityMaps object.
     * <p>
     * Depending on the context, this could refer to the number of hops present,
     * or the number of timesteps etc.
     *
     * @return the size of the entityMaps object
     */
    default int size() {
        return asList().size();
    }

    /**
     * Return {@code true} if this EntityMaps object is empty, otherwise {@code
     * false}.
     *
     * @return the empty state of this object
     */
    default boolean empty() {
        return asList().isEmpty();
    }

    /**
     * Print the {@code EntityMap} object in an easily readable format.
     *
     * @return a prettily printed {@link String} representation of the entityMap
     * object.
     */
    default String prettyPrint() {
        return this.getClass().getName() + '@' + Integer.toHexString(this.hashCode()) + Arrays.toString(asList().toArray());
    }

    /**
     * Get a representation of the current EntityMaps object as a {@link List}.
     *
     * @return a {@link List} representation of the current entityMaps object
     */
    List<EntityMap> asList();

    @Override
    default Iterator<EntityMap> iterator() {
        return asList().iterator();
    }
}
