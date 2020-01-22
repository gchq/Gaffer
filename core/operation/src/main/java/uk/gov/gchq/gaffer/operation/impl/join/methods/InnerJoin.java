/*
 * Copyright 2018-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.join.methods;


import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code InnerJoin} is a join function which only returns keys and matching values.
 */
public class InnerJoin extends JoinFunction {

    /**
     * Returns a list containing a {@code MapTuple} for each matching value associated
     * with that key. Keys with no matches result in an empty list
     * @param key The key
     * @param matches a list containing the matches
     * @param keyName the name of the keyed side (LEFT or RIGHT)
     * @param matchingValuesName the corresponding value side (LEFT or RIGHT)
     * @return A list of matching MapTuples
     */
    @Override
    protected List<MapTuple> joinFlattened(final Object key, final List matches, final String keyName, final String matchingValuesName) {
        List<MapTuple> resultList = new ArrayList<>();
        MapTuple<String> matchingPair;

        for (final Object matched : matches) {
            matchingPair = new MapTuple<>();
            matchingPair.put(keyName, key);
            matchingPair.put(matchingValuesName, matched);
            resultList.add(matchingPair);
        }

        return resultList;
    }

    /**
     * Returns a {@code MapTuple} if the key has matches. If not, null is returned.
     * @param key The key
     * @param matches a list containing the matches
     * @param keyName the name of the keyed side (LEFT or RIGHT)
     * @param matchingValuesName the corresponding value side (LEFT or RIGHT)
     * @return The MapTuple is the key has matches (null if not).
     */
    @Override
    protected MapTuple joinAggregated(final Object key, final List matches, final String keyName, final String matchingValuesName) {
        if (!matches.isEmpty()) {
            MapTuple<String> allMatchingValues = new MapTuple<>();
            allMatchingValues.put(keyName, key);
            allMatchingValues.put(matchingValuesName, matches);
            return allMatchingValues;
        } else {
            return null;
        }
    }
}
