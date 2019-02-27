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
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code InnerJoin} is a join function which returns matched items from an iterable and list.
 */
public class InnerJoin implements JoinFunction {
    @Override
    public List<MapTuple> join(final Iterable left, final List right, final Match match, final MatchKey matchKey, final Boolean flatten) {

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

            MapTuple<String> tuple = new MapTuple<>();
            tuple.put(leftKey, leftObj);

            // flattening will output a tuple for each value in the matching list
            if (flatten) {
                for (final Object matched : matching) {
                    tuple.put(rightKey, matched);
                    resultList.add(tuple);
                }
            } else {
                tuple.put(rightKey, matching);
                resultList.add(tuple);
            }


            // TODO make methods output flattened results.
            // TODO add logic to handle to unflatten them.
            // TODO create Matcher which takes two functions to apply to left and right inputs to generate a comparable
        }


        return resultList;
    }
}
