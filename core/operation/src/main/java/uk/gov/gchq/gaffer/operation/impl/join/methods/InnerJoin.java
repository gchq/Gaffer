/*
 * Copyright 2018-2019 Crown Copyright
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


import uk.gov.gchq.gaffer.operation.impl.join.match.Match;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code InnerJoin} is a join function which returns matched items from an iterable and list.
 */
public class InnerJoin extends JoinFunction {

    /**
     * Calculates a 1-to-1 mapping pair for each match
     * @param keys keys to use to match
     * @param keyName name of the key (LEFT or RIGHT)
     * @param matchingValuesName name of the value (LEFT or RIGHT)
     * @param match The {@code Match} to use to identify matches
     * @return the 1-to-1 mapping pair
     */
    @Override
    protected List<MapTuple> calculateFlattenedMatches(final Iterable keys, final String keyName, final String matchingValuesName, final Match match) {
        List<MapTuple> resultList = new ArrayList<>();

        for (final Object keyObj : keys) {
            List matching = match.matching(keyObj);

            for (final Object matched : matching) {
                MapTuple<String> matchingPair = new MapTuple<>();
                matchingPair.put(keyName, keyObj);
                matchingPair.put(matchingValuesName, matched);

                resultList.add(matchingPair);
            }
        }

        return resultList;
    }

    /**
     * Calculates a 1-to-many mapping pair for each key. All matches identified
     * are returned in a list
     * @param keys keys to use to match
     * @param keyName name of the key (LEFT or RIGHT)
     * @param matchingValuesName name of the value (LEFT or RIGHT)
     * @param match The {@code Match} to use to identify matches
     * @return the 1-to-many mapping pair
     */
    @Override
    protected List<MapTuple> calculateAggregatedMatches(final Iterable keys, final String keyName, final String matchingValuesName, final Match match) {
        List<MapTuple> resultList = new ArrayList<>();

        for (final Object keyObj : keys) {
            List matching = match.matching(keyObj);

            if (!matching.isEmpty()) {
                MapTuple<String> allMatchingValues = new MapTuple<>();
                allMatchingValues.put(keyName, keyObj);
                allMatchingValues.put(matchingValuesName, matching);
                resultList.add(allMatchingValues);
            }
        }

        return resultList;
    }
}
