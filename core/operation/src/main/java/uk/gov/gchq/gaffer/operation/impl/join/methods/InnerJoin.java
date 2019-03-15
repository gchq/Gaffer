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
    @Override
    protected List<MapTuple> join(final Iterable keys, final String keyName, final String matchingValuesName, final Match match, final Boolean flatten) {

        List<MapTuple> resultList = new ArrayList<>();

        for (final Object keyObj : keys) {
            List matching = match.matching(keyObj);

            MapTuple<String> tuple = new MapTuple<>();
            tuple.put(keyName, keyObj);

            // flattening will output a tuple for each value in the matching list
            if (flatten) {
                for (final Object matched : matching) {
                    tuple.put(matchingValuesName, matched);
                    resultList.add(tuple);
                }
            } else if (!matching.isEmpty()) {
                tuple.put(matchingValuesName, matching);
                resultList.add(tuple);
            }
        }


        return resultList;
    }
}
