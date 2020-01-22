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

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import uk.gov.gchq.gaffer.operation.impl.join.match.Match;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Used by the Join Operation to join two Lists together.
 */
public abstract class JoinFunction {

    public List<MapTuple> join(final Iterable left, final Iterable right, final Match match, final MatchKey matchKey, final Boolean flatten) {
        final String keyName; // For LEFT keyed Joins it's LEFT and vice versa for RIGHT.
        final String matchingValuesName; // the matching values name (opposite of keyName)
        final Iterable keys; // The key iterate over

        keyName = matchKey.name();
        if (matchKey.equals(MatchKey.LEFT)) {
            matchingValuesName = MatchKey.RIGHT.name();
            keys = left;
            match.init(right);
        } else {
            matchingValuesName = MatchKey.LEFT.name();
            keys = right;
            match.init(left);
        }

        List<MapTuple> resultList = new ArrayList<>();
        List matching;

        if (flatten) {
            for (final Object keyObj : keys) {
                matching = match.matching(keyObj);
                final List<MapTuple> mapTuples = joinFlattened(keyObj, matching, keyName, matchingValuesName);
                if (!mapTuples.isEmpty()) {
                    resultList.addAll(mapTuples);
                }
            }
        } else {
            for (final Object keyObj : keys) {
                matching = match.matching(keyObj);
                final MapTuple mapTuple = joinAggregated(keyObj, matching, keyName, matchingValuesName);
                if (mapTuple != null) {
                    resultList.add(mapTuple);
                }
            }
        }
        return resultList;
    }

    @Deprecated
    protected List<MapTuple> join(final Iterable keys, final String keyName, final String matchingValuesName, final Match match, final Boolean flatten) {
        throw new NotImplementedException();
    }

    protected abstract List<MapTuple> joinFlattened(Object key, List matches, String keyName, String matchingValuesName);

    protected abstract MapTuple joinAggregated(Object key, List matches, String keyName, String matchingValuesName);
}
