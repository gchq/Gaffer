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
package uk.gov.gchq.gaffer.time;

import java.time.Instant;
import java.util.Collection;
import java.util.SortedSet;

/**
 * This represents a set of timestamps.
 */
public interface TimestampSet {

    /**
     * Adds the provided timestamp to the set.
     *
     * @param instant The timestamp to be added.
     */
    void add(Instant instant);

    /**
     * Adds all the provided timestamps to the set.
     *
     * @param instants The {@link Collection} of timestamps to be added.
     */
    void add(Collection<Instant> instants);

    /**
     * Returns all the timestamps in the set, sorted in their natural order.
     *
     * @return All the timestamps in the set, sorted in their natural order.
     */
    SortedSet<Instant> getTimestamps();

    /**
     * Returns the number of distinct timestamps in the set.
     *
     * @return The number of distinct timestamps in the set.
     */
    long getNumberOfTimestamps();

    /**
     * The earliest timestamp in the set.
     *
     * @return The earliest timestamp in the set.
     */
    Instant getEarliest();

    /**
     * The latest timestamp in the set.
     *
     * @return The latest timestamp in the set.
     */
    Instant getLatest();
}
