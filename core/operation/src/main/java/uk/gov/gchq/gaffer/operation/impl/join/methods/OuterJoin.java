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

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.operation.impl.join.match.Match;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code OuterJoin} is a Join function which returns values from an iterable (together with an empty list)
 * where they do not match with any value in the list.
 */
public class OuterJoin implements JoinFunction {
    @Override
    public List<MapTuple> join(final Iterable left, final Iterable right, final Match match, final MatchKey matchKey, final Boolean flatten) {
        final String keyName; // For LEFT keyed Joins it's LEFT and vice versa for RIGHT.
        final String matchingValuesName;

        Iterable keys; // The key iterate over
        List matchCandidates; // The iterable to use to check for matches

        keyName = matchKey.name();
        if (matchKey.equals(MatchKey.LEFT)) {
            matchingValuesName = MatchKey.RIGHT.name();
            keys = left;
            matchCandidates = Lists.newArrayList(right);
        } else {
            matchingValuesName = MatchKey.LEFT.name();
            keys = right;
            matchCandidates = Lists.newArrayList(left);
        }

        List<MapTuple> resultList = new ArrayList<>();

        for (final Object keyObj : keys) {
            List matching = match.matching(keyObj, matchCandidates);
            if (matching.isEmpty()) {

                MapTuple<String> tuple = new MapTuple<>();
                tuple.put(keyName, keyObj);

                // flattening will output a null value instead of an empty list
                if (flatten) {
                    tuple.put(matchingValuesName, null);
                } else {
                    tuple.put(matchingValuesName, matching);
                }
                resultList.add(tuple);
            }
        }

        return resultList;
    }
}
