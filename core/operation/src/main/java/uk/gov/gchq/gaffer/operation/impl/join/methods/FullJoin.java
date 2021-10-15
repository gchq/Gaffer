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
 * A Full Join returns the LEFT and RIGHT regardless of whether they match
 */
public class FullJoin extends JoinFunction {

    /**
     * Generates a {@code MapTuple} for each match. If a key doesn't match,
     * null will be put in the value.
     * @param key The key
     * @param matches a list containing the matches
     * @param keyName the name of the keyed side (LEFT or RIGHT)
     * @param matchingValuesName the corresponding value side (LEFT or RIGHT)
     * @return A list containing tuples for each match
     */
    @Override
    protected List<MapTuple> joinFlattened(final Object key, final List matches, final String keyName, final String matchingValuesName) {
        List<MapTuple> resultList = new ArrayList<>();

        if (matches.isEmpty()) {
            MapTuple<String> unMatchedPair = new MapTuple<>();
            unMatchedPair.put(keyName, key);
            unMatchedPair.put(matchingValuesName, null);
            resultList.add(unMatchedPair);
        } else {
            MapTuple<String> matchingPair;

            for (final Object matched : matches) {
                matchingPair = new MapTuple<>();
                matchingPair.put(keyName, key);
                matchingPair.put(matchingValuesName, matched);
                resultList.add(matchingPair);
            }
        }

        return resultList;
    }


    /**
     * Creates a {@code MapTuple} containing the key and all the matches
     * @param key The key
     * @param matches values matching the key
     * @param keyName the name of the key (LEFT or RIGHT)
     * @param matchingValuesName the name of the value (LEFT or RIGHT)
     * @return A MapTuple with the key and matching values
     */
    @Override
    protected MapTuple joinAggregated(final Object key, final List matches, final String keyName, final String matchingValuesName) {
        MapTuple<String> matchingValues = new MapTuple<>();
        matchingValues.put(keyName, key);
        matchingValues.put(matchingValuesName, matches);
        return matchingValues;
    }
}
