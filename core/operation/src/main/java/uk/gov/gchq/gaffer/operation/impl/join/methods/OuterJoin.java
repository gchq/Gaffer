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

import com.google.common.collect.ImmutableMap;

import uk.gov.gchq.gaffer.operation.impl.join.match.Match;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@code OuterJoin} is a Join function which returns values from an iterable (together with an empty list)
 * where they do not match with any value in the list.
 */
public class OuterJoin implements JoinFunction {
    @Override
    public List<MapTuple> join(final Iterable left, final List right, final Match match, final MatchKey matchKey) {
        final String leftKey;
        final String rightKey;

        // If the left and right inputs have been switched,
        // we need to output the left and right objects in
        // reverse order
        if (matchKey.equals(MatchKey.LEFT)) {
            leftKey = matchKey.name();
            rightKey = MatchKey.RIGHT.name();
        } else {
            leftKey = MatchKey.RIGHT.name();
            rightKey = matchKey.name();
        }
        List<MapTuple> resultList = new ArrayList<>();

        for (final Object leftObj : left) {
            List matching = match.matching(leftObj, right);
            if (matching.isEmpty()) {
                MapTuple<String> tuple = new MapTuple<>();
                tuple.put(leftKey, leftObj);
                tuple.put(rightKey, null);
                resultList.add(tuple);
            }
        }

        return resultList;
    }
}
